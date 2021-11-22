/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.replication.logical;

import io.crate.common.annotations.VisibleForTesting;
import io.crate.common.unit.TimeValue;
import io.crate.concurrent.CountdownFutureCallback;
import io.crate.execution.support.RetryRunnable;
import io.crate.metadata.RelationName;
import io.crate.replication.logical.metadata.PublicationsMetadata;
import io.crate.replication.logical.metadata.Subscription;
import io.crate.replication.logical.metadata.SubscriptionsMetadata;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.state.ClusterStateAction;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;

import static io.crate.replication.logical.repository.LogicalReplicationRepository.REMOTE_CLUSTER_REPO_REQ_TIMEOUT_IN_MILLI_SEC;
import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING;

public final class MetadataTracker implements Closeable {

    private static final Logger LOGGER = Loggers.getLogger(MetadataTracker.class);

    private final ThreadPool threadPool;
    private final Function<String, Client> remoteClient;
    private final ClusterService clusterService;
    private final TimeValue pollDelay;
    private final IndexScopedSettings indexScopedSettings;

    // Using a copy-on-write approach. The assumption is that subscription changes are rare and reads happen more frequently
    private volatile Set<String> subscriptionsToTrack = new HashSet<>();
    private volatile Scheduler.Cancellable cancellable;
    private volatile boolean isActive = false;

    public MetadataTracker(IndexScopedSettings indexScopedSettings,
                           Settings settings,
                           ThreadPool threadPool,
                           Function<String, Client> remoteClient,
                           ClusterService clusterService) {
        this.threadPool = threadPool;
        this.remoteClient = remoteClient;
        this.clusterService = clusterService;
        this.pollDelay = LogicalReplicationSettings.REPLICATION_READ_POLL_DURATION.get(settings);
        this.indexScopedSettings = indexScopedSettings;
    }

    private void start() {
        assert isActive == false : "MetadataTracker is already started";
        assert clusterService.state().getNodes().getLocalNode().isMasterNode() : "MetadataTracker must only be run on the master node";
        var runnable = new RetryRunnable(
            threadPool.executor(ThreadPool.Names.LOGICAL_REPLICATION),
            threadPool.scheduler(),
            this::run,
            BackoffPolicy.exponentialBackoff(pollDelay, 8)
        );
        runnable.run();
        isActive = true;
    }

    private void stop() {
        if (cancellable != null) {
            cancellable.cancel();
        }
        isActive = false;
    }

    private void schedule() {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("Reschedule tracking metadata");
        }
        cancellable = threadPool.schedule(
            this::run,
            pollDelay,
            ThreadPool.Names.LOGICAL_REPLICATION
        );
    }

    public boolean startTracking(String subscriptionName) {
        synchronized (this) {
            var copy = new HashSet<>(subscriptionsToTrack);
            var updated = copy.add(subscriptionName);
            if (updated && !isActive) {
                start();
            }
            subscriptionsToTrack = copy;
            return updated;
        }
    }

    public boolean stopTracking(String subscriptionName) {
        synchronized (this) {
            var copy = new HashSet<>(subscriptionsToTrack);
            var updated = copy.remove(subscriptionName);
            if (isActive && copy.isEmpty()) {
                stop();
            }
            subscriptionsToTrack = copy;
            return updated;
        }
    }

    private void run() {
        var countDown = new CountdownFutureCallback(subscriptionsToTrack.size());
        for (String subscriptionName : subscriptionsToTrack) {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("Start tracking metadata for subscription {}", subscriptionName);
            }
            getRemoteClusterState(subscriptionName, remoteClusterState -> {
                clusterService.submitStateUpdateTask("track-metadata", new AckedClusterStateUpdateTask<>(
                    new AckMetadataUpdateRequest(),
                    countDownActionListener(subscriptionName, countDown)
                ) {

                    @Override
                    public ClusterState execute(ClusterState localClusterState) throws Exception {
                        return updateIndexMetadata(subscriptionName, localClusterState, remoteClusterState, indexScopedSettings);
                    }

                    @Override
                    protected AcknowledgedResponse newResponse(boolean acknowledged) {
                        return new AcknowledgedResponse(acknowledged);
                    }
                });
            });
        }
        countDown.thenRun(() -> {
            if (isActive) {
                schedule();
            }
        });
    }

    private static ActionListener<AcknowledgedResponse> countDownActionListener(String subscriptionName,
                                                                                CountdownFutureCallback countDown) {
        return ActionListener.wrap(r -> {
            if (r.isAcknowledged()) {
                countDown.onSuccess();
            }
        }, t -> {
            LOGGER.error("Tracking metadata failed for subscription {} {}", subscriptionName, t);
            countDown.onFailure(t);
        });
    }

    private static class AckMetadataUpdateRequest extends AcknowledgedRequest<AckMetadataUpdateRequest> {

    }

    @VisibleForTesting
    static ClusterState updateIndexMetadata(String subscriptionName, ClusterState subscriberClusterState, ClusterState publisherClusterState, IndexScopedSettings indexScopedSettings) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("Process cluster state for subscription {}", subscriptionName);
        }
        PublicationsMetadata publicationsMetadata = publisherClusterState.metadata().custom(PublicationsMetadata.TYPE);
        SubscriptionsMetadata subscriptionsMetadata = subscriberClusterState.metadata().custom(SubscriptionsMetadata.TYPE);
        if (publicationsMetadata == null || subscriptionsMetadata == null) {
            return subscriberClusterState;
        }
        var subscribedTables = new HashSet<RelationName>();
        Subscription subscription = subscriptionsMetadata.subscription().get(subscriptionName);
        if (subscription != null) {
            for (var publicationName : subscription.publications()) {
                var publications = publicationsMetadata.publications();
                if (publications != null) {
                    var publication = publications.get(publicationName);
                    subscribedTables.addAll(publication.tables());
                }
            }
        }
        // Check for all the subscribed tables if the index metadata and settings changed and if so apply
        // the changes from the publisher cluster state to the subscriber cluster state
        var updatedMetadataBuilder = Metadata.builder(subscriberClusterState.metadata());
        var updateClusterState = false;
        for (var followedTable : subscribedTables) {
            var isIndexMetadataUpdated = false;
            var publisherIndexMetadata = publisherClusterState.metadata().index(followedTable.indexNameOrAlias());
            var subscriberIndexMetadata = subscriberClusterState.metadata().index(followedTable.indexNameOrAlias());
            if (publisherIndexMetadata != null && subscriberIndexMetadata != null) {
                var updatedIndexMetadataBuilder = IndexMetadata.builder(subscriberIndexMetadata);
                isIndexMetadataUpdated =
                    updateIndexMetadataSettings(updatedIndexMetadataBuilder, publisherIndexMetadata, subscriberIndexMetadata, indexScopedSettings) ||
                    updateIndexMetadataMapping(updatedIndexMetadataBuilder, publisherIndexMetadata, subscriberIndexMetadata);

                if (isIndexMetadataUpdated) {
                    updatedMetadataBuilder.put(updatedIndexMetadataBuilder.build(), true);
                }
                updateClusterState = updateClusterState || isIndexMetadataUpdated;
            }
        }
        if (updateClusterState) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Updated index metadata for subscription {}", subscriptionName);
            }
            return ClusterState.builder(subscriberClusterState).metadata(updatedMetadataBuilder).build();
        } else {
            return subscriberClusterState;
        }
    }

    private static boolean updateIndexMetadataMapping(IndexMetadata.Builder updatedIndexMetadata,
                                              IndexMetadata publisherIndexMetadata,
                                              IndexMetadata subscriberIndexMetadata) {
        var publisherMapping = publisherIndexMetadata.mapping();
        var subscriberMapping = subscriberIndexMetadata.mapping();
        if (publisherMapping != null && subscriberMapping != null) {
            if (publisherIndexMetadata.getMappingVersion() > subscriberIndexMetadata.getMappingVersion()) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Updated index mapping {} for subscription {}", subscriberIndexMetadata.getIndex().getName(), publisherMapping.toString());
                }
                updatedIndexMetadata.putMapping(publisherMapping).mappingVersion(publisherIndexMetadata.getMappingVersion());
                return true;
            }
        }
        return false;
    }

    private static boolean updateIndexMetadataSettings(IndexMetadata.Builder updatedIndexMetadata,
                                               IndexMetadata publisherMetadata,
                                               IndexMetadata subscriberMetadata,
                                               IndexScopedSettings indexScopedSettings) {
        var isUpdated = false;
        var publisherMetadataSetting = publisherMetadata.getSettings();
        var subscriberMetadataSetting = subscriberMetadata.getSettings();
        var newSubscriberIndexMetadataSettings = Settings.builder();
        // Take all initial settings from the subscriber
        for (var key : subscriberMetadataSetting.keySet()) {
            newSubscriberIndexMetadataSettings.copy(key, subscriberMetadataSetting);
        }
        // Add or update all settings from the publisher. Skip private settings which are not transferable
        // such as uuid or creation date and ignore static settings which are not allowed to change on published tables
        for (var key : publisherMetadataSetting.keySet()) {
            if (INDEX_NUMBER_OF_REPLICAS_SETTING.getKey().equals(key)) {
                // Don't replicate number of replicas setting as this can get dynamically adapted
                continue;
            }
            var setting = indexScopedSettings.get(key);
            if (
                setting != null &&
                !setting.isInternalIndex() &&
                !setting.isPrivateIndex() &&
                indexScopedSettings.isDynamicSetting(key) &&
                !indexScopedSettings.isPrivateSetting(key) &&
                !Objects.equals(subscriberMetadataSetting.get(key), publisherMetadataSetting.get(key))
            ) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Update setting {} for index {}", key, publisherMetadata.getIndex());
                }
                newSubscriberIndexMetadataSettings.copy(key, publisherMetadataSetting);
                isUpdated = true;
            }
        }

        if (isUpdated) {
            updatedIndexMetadata
                .settings(Settings.builder().put(newSubscriberIndexMetadataSettings.build()))
                .settingsVersion(subscriberMetadata.getSettingsVersion() + 1L);
        }
        return isUpdated;
    }

    private void getRemoteClusterState(String subscriptionName, Consumer<ClusterState> consumer) {
        var client = remoteClient.apply(subscriptionName);

        var clusterStateRequest = client.admin().cluster().prepareState()
            .setWaitForTimeOut(new TimeValue(REMOTE_CLUSTER_REPO_REQ_TIMEOUT_IN_MILLI_SEC))
            .request();

        client.admin().cluster().execute(
            ClusterStateAction.INSTANCE, clusterStateRequest, new ActionListener<>() {
                @Override
                public void onResponse(ClusterStateResponse clusterStateResponse) {
                    consumer.accept(clusterStateResponse.getState());
                }

                @Override
                public void onFailure(Exception e) {
                    LOGGER.error(e);
                }
            });
    }

    @Override
    public void close() throws IOException {
        stop();
    }

}
