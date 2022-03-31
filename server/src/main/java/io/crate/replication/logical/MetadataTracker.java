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

import io.crate.action.FutureActionListener;
import io.crate.common.TriConsumer;
import io.crate.common.annotations.VisibleForTesting;
import io.crate.common.unit.TimeValue;
import io.crate.concurrent.CountdownFutureCallback;
import io.crate.exceptions.Exceptions;
import io.crate.execution.support.RetryRunnable;
import io.crate.metadata.IndexParts;
import io.crate.metadata.PartitionName;
import io.crate.metadata.RelationName;
import io.crate.replication.logical.action.PublicationsStateAction;
import io.crate.replication.logical.metadata.ConnectionInfo;
import io.crate.replication.logical.metadata.PublicationsMetadata;
import io.crate.replication.logical.metadata.Subscription;
import io.crate.replication.logical.metadata.SubscriptionsMetadata;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.state.ClusterStateAction;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.MetadataDeleteIndexService;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.NoSuchRemoteClusterException;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;

import static io.crate.replication.logical.LogicalReplicationSettings.NON_REPLICATED_SETTINGS;
import static io.crate.replication.logical.LogicalReplicationSettings.PUBLISHER_INDEX_UUID;
import static io.crate.replication.logical.repository.LogicalReplicationRepository.REMOTE_CLUSTER_REPO_REQ_TIMEOUT_IN_MILLI_SEC;

public final class MetadataTracker implements Closeable {

    private static final Logger LOGGER = Loggers.getLogger(MetadataTracker.class);

    private final Settings settings;
    private final ThreadPool threadPool;
    private final LogicalReplicationService replicationService;
    private final LogicalReplicationSettings replicationSettings;
    private final Function<String, Client> remoteClient;
    private final ClusterService clusterService;
    private final IndexScopedSettings indexScopedSettings;
    private final AllocationService allocationService;
    private final IndexNameExpressionResolver indexNameExpressionResolver = new IndexNameExpressionResolver();

    // Using a copy-on-write approach. The assumption is that subscription changes are rare and reads happen more frequently
    private volatile Set<String> subscriptionsToTrack = Set.of();
    private volatile Scheduler.Cancellable cancellable;
    private volatile boolean isActive = false;

    public MetadataTracker(Settings settings,
                           IndexScopedSettings indexScopedSettings,
                           ThreadPool threadPool,
                           LogicalReplicationService replicationService,
                           LogicalReplicationSettings replicationSettings,
                           Function<String, Client> remoteClient,
                           ClusterService clusterService,
                           AllocationService allocationService) {
        this.settings = settings;
        this.threadPool = threadPool;
        this.replicationService = replicationService;
        this.replicationSettings = replicationSettings;
        this.remoteClient = remoteClient;

        this.clusterService = clusterService;
        this.indexScopedSettings = indexScopedSettings;
        this.allocationService = allocationService;
    }

    private void start() {
        assert isActive == false : "MetadataTracker is already started";
        assert clusterService.state().getNodes().isLocalNodeElectedMaster() : "MetadataTracker must only be run on the master node";
        var runnable = new RetryRunnable(
            threadPool.executor(ThreadPool.Names.LOGICAL_REPLICATION),
            threadPool.scheduler(),
            this::run,
            BackoffPolicy.exponentialBackoff(replicationSettings.pollDelay(), 8)
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
        if (!isActive) {
            return;
        }

        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("Reschedule tracking metadata");
        }
        cancellable = threadPool.schedule(
            this::run,
            replicationSettings.pollDelay(),
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
        // single volatile read
        var currentSubscriptionsToTrack = subscriptionsToTrack;

        var countDown = new CountdownFutureCallback(currentSubscriptionsToTrack.size());
        countDown.thenRun(this::schedule);

        for (String subscriptionName : currentSubscriptionsToTrack) {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("Poll metadata for subscription {}", subscriptionName);
            }
            Consumer<Exception> onError = e -> {
                if (shouldRetry(e) == false) {
                    var msg = "Tracking of metadata failed for subscription '" + subscriptionName + "'" +
                              " with unrecoverable error, stop tracking";
                    LOGGER.error(msg, e);
                    replicationService.updateSubscriptionState(
                        subscriptionName,
                        Subscription.State.FAILED,
                        msg
                    ).whenComplete((ignored, ignoredErr) -> {
                        stopTracking(subscriptionName);
                        countDown.onSuccess();
                    });
                } else {
                    LOGGER.warn("Retrieving remote metadata failed for subscription '" + subscriptionName + "', will retry", e);
                    countDown.onSuccess();
                }
            };
            var subscription = retrieveSubscription(subscriptionName, clusterService.state());
            if (subscription == null) {
                LOGGER.trace("Subscription '" + subscriptionName +
                    "' not found inside current local cluster state");
                continue;
            }

            // Block new relation processing until dropped relations are processed
            CompletableFuture<Boolean> droppedRelationsReplicationDone = new CompletableFuture<>();
            ActionListener<AcknowledgedResponse> listener = ActionListener.wrap(r -> countDown.onSuccess(), onError);
            getRemoteClusterState(subscriptionName).whenComplete(
                (remoteClusterState, err) -> {
                    if (err == null) {
                        PublicationsMetadata publicationsMetadata = remoteClusterState.metadata().custom(PublicationsMetadata.TYPE);
                        if (publicationsMetadata == null) {
                            LOGGER.trace("No publications found on remote cluster.");
                            return;
                        }

                        clusterService.submitStateUpdateTask("track-metadata", new AckedClusterStateUpdateTask<>(
                            new AckMetadataUpdateRequest(),
                            listener
                        ) {

                            @Override
                            public ClusterState execute(ClusterState localClusterState) throws Exception {
                                if (LOGGER.isTraceEnabled()) {
                                    LOGGER.trace("Process cluster state for subscription {}", subscriptionName);
                                }

                                localClusterState = removeDroppedTablesOrPartitions(
                                    subscriptionName,
                                    subscription,
                                    localClusterState,
                                    remoteClusterState
                                );

                                return updateIndexMetadata(
                                    subscriptionName,
                                    subscription,
                                    localClusterState,
                                    remoteClusterState,
                                    indexScopedSettings
                                );
                            }

                            @Override
                            protected AcknowledgedResponse newResponse(boolean acknowledged) {
                                return new AcknowledgedResponse(acknowledged);
                            }

                            @Override
                            public void onAllNodesAcked(@Nullable Exception e) {
                                super.onAllNodesAcked(e);
                                droppedRelationsReplicationDone.complete(e == null);
                            }
                        });

                        var stateFuture = getPublicationsState(subscriptionName, subscription);
                        stateFuture.whenComplete(((response, e) -> {
                            if (e == null) {
                                subscribeToNewRelations(
                                    subscriptionName,
                                    subscription,
                                    clusterService.state(),
                                    response,
                                    replicationService,
                                    droppedRelationsReplicationDone
                                );
                            } else {
                                LOGGER.trace(
                                    "Error while trying to retrieve the publications state for subscription=" + subscriptionName,
                                    err
                                );
                            }
                        }));
                    } else {
                        droppedRelationsReplicationDone.complete(true);
                        onError.accept(Exceptions.toRuntimeException(err));
                    }
                }
            );
        }
    }

    private static boolean shouldRetry(Exception e) {
        return e instanceof ConnectTransportException ||
               e instanceof ElasticsearchTimeoutException ||
               e instanceof NoSuchRemoteClusterException;
    }

    private static class AckMetadataUpdateRequest extends AcknowledgedRequest<AckMetadataUpdateRequest> {

    }

    @Nullable
    static Subscription retrieveSubscription(String subscriptionName, ClusterState currentState) {
        SubscriptionsMetadata subscriptionsMetadata = currentState.metadata().custom(SubscriptionsMetadata.TYPE);
        if (subscriptionsMetadata == null) {
            LOGGER.trace("No subscriptions found inside current local cluster state");
            return null;
        }
        return subscriptionsMetadata.subscription().get(subscriptionName);
    }

    @VisibleForTesting
    static ClusterState updateIndexMetadata(String subscriptionName,
                                            Subscription subscription,
                                            ClusterState subscriberClusterState,
                                            ClusterState publisherClusterState,
                                            IndexScopedSettings indexScopedSettings) {
        // Check for all the subscribed tables if the index metadata and settings changed and if so apply
        // the changes from the publisher cluster state to the subscriber cluster state
        var updatedMetadataBuilder = Metadata.builder(subscriberClusterState.metadata());
        var updateClusterState = false;
        for (var followedTable : subscription.relations().keySet()) {
            var publisherIndexMetadata = publisherClusterState.metadata().index(followedTable.indexNameOrAlias());
            var subscriberIndexMetadata = subscriberClusterState.metadata().index(followedTable.indexNameOrAlias());
            if (publisherIndexMetadata != null && subscriberIndexMetadata != null) {
                var updatedIndexMetadataBuilder = IndexMetadata.builder(subscriberIndexMetadata);
                var updatedMapping = updateIndexMetadataMappings(publisherIndexMetadata, subscriberIndexMetadata);
                if (updatedMapping != null) {
                    updatedIndexMetadataBuilder.putMapping(updatedMapping).mappingVersion(publisherIndexMetadata.getMappingVersion());
                }
                var updatedSettings = updateIndexMetadataSettings(
                    publisherIndexMetadata.getSettings(),
                    subscriberIndexMetadata.getSettings(),
                    indexScopedSettings
                );
                if (updatedSettings != null) {
                    updatedIndexMetadataBuilder.settings(updatedSettings).settingsVersion(subscriberIndexMetadata.getSettingsVersion() + 1L);
                }
                if (updatedMapping != null || updatedSettings != null) {
                    updatedMetadataBuilder.put(updatedIndexMetadataBuilder.build(), true);
                    updateClusterState = true;
                }
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

    /**
     * Extract new relations of all publications of the subscription based on the local and remote publication state
     * and initiate a restore of these.
     * See also {@link LogicalReplicationService#restore}.
     */
    private static void subscribeToNewRelations(String subscriptionName,
                                                Subscription subscription,
                                                ClusterState subscriberClusterState,
                                                PublicationsStateAction.Response stateResponse,
                                                LogicalReplicationService replicationService,
                                                CompletableFuture<Boolean> localStateAckedOnAllNodes) {
        subscribeToNewRelations(
            subscription,
            subscriberClusterState,
            stateResponse,
            relationNames -> replicationService.updateSubscriptionState(
                subscriptionName,
                relationNames,
                Subscription.State.INITIALIZING,
                null
            ),
            (relationNames, toRestoreIndices, toRestoreTemplates) ->
                localStateAckedOnAllNodes.whenComplete((acked, e) -> {
                    if (acked && e == null) {
                        replicationService.restore(
                            subscriptionName,
                            subscription.settings(),
                            relationNames,
                            toRestoreIndices,
                            toRestoreTemplates
                        );
                    }
                })
        );
    }

    @VisibleForTesting
    static CompletableFuture<Boolean> subscribeToNewRelations(
        Subscription subscription,
        ClusterState subscriberClusterState,
        PublicationsStateAction.Response stateResponse,
        Function<Collection<RelationName>, CompletableFuture<Boolean>> updateStateFunction,
        TriConsumer<Collection<RelationName>, List<String>, List<String>> restoreConsumer) {

        var subscribedRelations = subscription.relations();
        var relationNamesForStateUpdate = new HashSet<RelationName>();
        var toRestoreIndices = new ArrayList<String>();
        var toRestoreTemplates = new ArrayList<String>();

        for (var indexName : stateResponse.concreteIndices()) {
            var relationName = RelationName.fromIndexName(indexName);
            if (subscriberClusterState.metadata().hasIndex(indexName) == false) {
                toRestoreIndices.add(indexName);
            }
            if (subscribedRelations.get(relationName) == null) {
                relationNamesForStateUpdate.add(relationName);
            }
        }
        for (var templateName : stateResponse.concreteTemplates()) {
            var indexParts = new IndexParts(templateName);
            if (indexParts.isPartitioned()) {
                var relationName = indexParts.toRelationName();
                if (subscriberClusterState.metadata().templates().get(templateName) == null) {
                    toRestoreTemplates.add(templateName);
                }
                if (subscribedRelations.get(relationName) == null) {
                    relationNamesForStateUpdate.add(relationName);
                }
            }
        }

        if (toRestoreIndices.isEmpty() && toRestoreTemplates.isEmpty()) {
            return CompletableFuture.completedFuture(true);
        }

        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("Restoring newly subscribed relations={}, indices={}, templates={}",
                         relationNamesForStateUpdate,
                         toRestoreIndices,
                         toRestoreTemplates
            );
        }

        var stateFuture = CompletableFuture.completedFuture(true);
        if (relationNamesForStateUpdate.isEmpty() == false) {
            stateFuture = updateStateFunction.apply(relationNamesForStateUpdate);
        }

        // Trigger restore of new subscribed relations
        return stateFuture.whenComplete(
            (ignored, ignoredErr) ->
                restoreConsumer.accept(relationNamesForStateUpdate, toRestoreIndices, toRestoreTemplates)
        );
    }

    private ClusterState removeDroppedTablesOrPartitions(String subscriptionName,
                                                         Subscription subscription,
                                                         ClusterState subscriberClusterState,
                                                         ClusterState publisherClusterState) {
        HashSet<RelationName> relationsToUpdate = new HashSet<>();
        HashSet<Index> indicesToRemove = new HashSet<>();
        ArrayList<String> templatesToRemove = new ArrayList<>();
        for (var relationName : subscription.relations().keySet()) {
            var possibleTemplateName = PartitionName.templateName(relationName.schema(), relationName.name());
            var isPartitioned = subscriberClusterState.metadata().templates().get(possibleTemplateName) != null;
            if (isPartitioned
                && publisherClusterState.metadata().templates().get(possibleTemplateName) == null) {
                templatesToRemove.add(possibleTemplateName);
                relationsToUpdate.add(relationName);
            }

            var concreteIndices = indexNameExpressionResolver.concreteIndices(
                subscriberClusterState,
                IndicesOptions.lenientExpand(),
                relationName.indexNameOrAlias()
            );
            for (var concreteIndex : concreteIndices) {
                var subscriberIndexMetadata = subscriberClusterState.metadata().index(concreteIndex);
                var publisherIndex = new Index(
                    concreteIndex.getName(),
                    PUBLISHER_INDEX_UUID.get(subscriberIndexMetadata.getSettings())
                );
                if (publisherClusterState.metadata().hasIndex(publisherIndex) == false) {
                    indicesToRemove.add(concreteIndex);
                    if (isPartitioned == false) {
                        relationsToUpdate.add(relationName);
                    }
                }
            }
        }

        var updatedClusterState = subscriberClusterState;
        if (templatesToRemove.isEmpty() == false) {
            var newMetadataBuilder = Metadata.builder(subscriberClusterState.metadata());
            for (var templateName : templatesToRemove) {
                newMetadataBuilder.removeTemplate(templateName);
            }
            updatedClusterState = ClusterState.builder(subscriberClusterState).metadata(newMetadataBuilder).build();
        }

        if (indicesToRemove.isEmpty() == false) {
            updatedClusterState = MetadataDeleteIndexService.deleteIndices(
                updatedClusterState,
                settings,
                allocationService,
                indicesToRemove
            );
        }
        if (relationsToUpdate.isEmpty() == false) {
            HashMap<RelationName, Subscription.RelationState> relations = new HashMap<>();
            for (var entry : subscription.relations().entrySet()) {
                if (relationsToUpdate.contains(entry.getKey()) == false) {
                    relations.put(entry.getKey(), entry.getValue());
                }
            }
            replicationService.updateSubscriptionState(subscriptionName, subscription, relations);
        }

        return updatedClusterState;
    }

    @Nullable
    private static MappingMetadata updateIndexMetadataMappings(IndexMetadata publisherIndexMetadata,
                                                               IndexMetadata subscriberIndexMetadata) {
        var publisherMapping = publisherIndexMetadata.mapping();
        var subscriberMapping = subscriberIndexMetadata.mapping();
        if (publisherMapping != null && subscriberMapping != null) {
            if (publisherIndexMetadata.getMappingVersion() > subscriberIndexMetadata.getMappingVersion()) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Updated index mapping {} for subscription {}", subscriberIndexMetadata.getIndex().getName(), publisherMapping.toString());
                }
                return publisherMapping;
            }
        }
        return null;
    }

    @Nullable
    private static Settings updateIndexMetadataSettings(Settings publisherMetadataSettings,
                                                        Settings subscriberMetadataSettings,
                                                        IndexScopedSettings indexScopedSettings) {
        var newSettingsBuilder = Settings.builder().put(subscriberMetadataSettings);
        var updatedSettings = publisherMetadataSettings.filter(
            key -> isReplicatableSetting(key, indexScopedSettings) &&
                   !Objects.equals(subscriberMetadataSettings.get(key), publisherMetadataSettings.get(key)));
        if (updatedSettings.isEmpty()) {
            return null;
        }
        return newSettingsBuilder.put(updatedSettings).build();
    }

    private static boolean isReplicatableSetting(String key, IndexScopedSettings indexScopedSettings) {
        var setting = indexScopedSettings.get(key);
        return setting != null &&
               !setting.isInternalIndex() &&
               !setting.isPrivateIndex() &&
               indexScopedSettings.isDynamicSetting(key) &&
               !indexScopedSettings.isPrivateSetting(key) &&
               !NON_REPLICATED_SETTINGS.contains(setting);
    }

    private CompletableFuture<ClusterState> getRemoteClusterState(String subscriptionName) {
        Client client;
        try {
            client = remoteClient.apply(subscriptionName);
        } catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }

        var clusterStateRequest = client.admin().cluster().prepareState()
            .setWaitForTimeOut(new TimeValue(REMOTE_CLUSTER_REPO_REQ_TIMEOUT_IN_MILLI_SEC))
            .request();

        var future = new FutureActionListener<>(ClusterStateResponse::getState);
        client.admin().cluster().execute(ClusterStateAction.INSTANCE, clusterStateRequest, future);
        return future;
    }

    private CompletableFuture<PublicationsStateAction.Response> getPublicationsState(String subscriptionName,
                                                                                     Subscription subscription) {
        Client client;
        try {
            client = remoteClient.apply(subscriptionName);
        } catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }
        FutureActionListener<PublicationsStateAction.Response, PublicationsStateAction.Response> future =
            FutureActionListener.newInstance();
        client.execute(
            PublicationsStateAction.INSTANCE,
            new PublicationsStateAction.Request(
                subscription.publications(),
                subscription.connectionInfo().settings().get(ConnectionInfo.USERNAME.getKey())
            ),
            future
        );
        return future;
    }

    @Override
    public void close() throws IOException {
        stop();
    }

}
