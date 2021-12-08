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
import io.crate.exceptions.Exceptions;
import io.crate.exceptions.RelationAlreadyExists;
import io.crate.execution.support.RetryRunnable;
import io.crate.metadata.PartitionName;
import io.crate.metadata.RelationName;
import io.crate.replication.logical.action.PublicationsStateAction;
import io.crate.replication.logical.metadata.ConnectionInfo;
import io.crate.replication.logical.metadata.Publication;
import io.crate.replication.logical.metadata.PublicationsMetadata;
import io.crate.replication.logical.metadata.Subscription;
import io.crate.replication.logical.metadata.SubscriptionsMetadata;
import io.crate.replication.logical.repository.LogicalReplicationRepository;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreClusterStateListener;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.snapshots.RestoreInfo;
import org.elasticsearch.snapshots.RestoreService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteClusters;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import static io.crate.replication.logical.repository.LogicalReplicationRepository.REMOTE_REPOSITORY_PREFIX;
import static io.crate.replication.logical.repository.LogicalReplicationRepository.TYPE;
import static org.elasticsearch.action.support.master.MasterNodeRequest.DEFAULT_MASTER_NODE_TIMEOUT;

public class LogicalReplicationService implements ClusterStateListener, Closeable {

    private static final Logger LOGGER = LogManager.getLogger(LogicalReplicationService.class);

    private final ClusterService clusterService;
    private final ThreadPool threadPool;
    private final RemoteClusters remoteClusters;
    private RepositoriesService repositoriesService;
    private RestoreService restoreService;

    private volatile SubscriptionsMetadata currentSubscriptionsMetadata = new SubscriptionsMetadata();
    private volatile PublicationsMetadata currentPublicationsMetadata = new PublicationsMetadata();
    private final MetadataTracker metadataTracker;

    public LogicalReplicationService(IndexScopedSettings indexScopedSettings,
                                     Settings settings,
                                     ClusterService clusterService,
                                     RemoteClusters remoteClusters,
                                     ThreadPool threadPool) {
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.remoteClusters = remoteClusters;
        this.metadataTracker = new MetadataTracker(
            indexScopedSettings,
            settings,
            threadPool,
            subscriptionName -> remoteClusters.getClient(subscriptionName),
            clusterService
        );
        clusterService.addListener(this);
    }

    public void repositoriesService(RepositoriesService repositoriesService) {
        this.repositoriesService = repositoriesService;
    }

    public void restoreService(RestoreService restoreService) {
        this.restoreService = restoreService;
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        var prevMetadata = event.previousState().metadata();
        var newMetadata = event.state().metadata();

        SubscriptionsMetadata prevSubscriptionsMetadata = prevMetadata.custom(SubscriptionsMetadata.TYPE);
        SubscriptionsMetadata newSubscriptionsMetadata = newMetadata.custom(SubscriptionsMetadata.TYPE);

        if ((prevSubscriptionsMetadata == null && newSubscriptionsMetadata != null)
            || (prevSubscriptionsMetadata != null && prevSubscriptionsMetadata.equals(newSubscriptionsMetadata) == false)) {
            currentSubscriptionsMetadata = newSubscriptionsMetadata;
            handleRepositoriesForChangedSubscriptions(prevSubscriptionsMetadata, newSubscriptionsMetadata);
        }

        PublicationsMetadata prevPublicationsMetadata = prevMetadata.custom(PublicationsMetadata.TYPE);
        PublicationsMetadata newPublicationsMetadata = newMetadata.custom(PublicationsMetadata.TYPE);

        if ((prevPublicationsMetadata == null && newPublicationsMetadata != null)
            || (prevPublicationsMetadata != null && prevPublicationsMetadata.equals(newPublicationsMetadata) == false)) {
            currentPublicationsMetadata = newPublicationsMetadata;
        }
    }

    /**
     * Register/unregister logical replication repositories.
     * If on a MASTER node, initial restore will be triggered.
     */
    private void handleRepositoriesForChangedSubscriptions(@Nullable SubscriptionsMetadata prevSubscriptionsMetadata,
                                                           SubscriptionsMetadata newSubscriptionsMetadata) {
        Map<String, Subscription> oldSubscriptions = prevSubscriptionsMetadata == null
            ? Collections.emptyMap() : prevSubscriptionsMetadata.subscription();
        var newSubscriptions = newSubscriptionsMetadata.subscription();

        for (var entry : newSubscriptions.entrySet()) {
            var subscriptionName = entry.getKey();
            boolean subscriptionAdded = oldSubscriptions.get(subscriptionName) == null;
            if (subscriptionAdded) {
                addSubscription(subscriptionName, entry.getValue());
            }
        }
        for (var entry : oldSubscriptions.entrySet()) {
            var subscriptionName = entry.getKey();
            boolean subscriptionRemoved = !newSubscriptions.containsKey(subscriptionName);
            if (subscriptionRemoved) {
                removeSubscription(subscriptionName);
            }
        }
    }

    private void addSubscription(String subscriptionName, Subscription subscription) {
        assert repositoriesService != null
            : "RepositoriesService must be set immediately after LogicalReplicationService construction";
        LOGGER.debug("Adding new logical replication repository for subscription '{}'", subscriptionName);
        repositoriesService.registerInternalRepository(REMOTE_REPOSITORY_PREFIX + subscriptionName, TYPE);

        if (clusterService.localNode().isMasterEligibleNode()) {
            withRetry(
                () -> {
                    // The startReplication will initiate the remote connection upfront
                    LOGGER.debug("Start logical replication for subscription '{}'", subscriptionName);
                    startReplication(subscriptionName, subscription)
                        .whenComplete(
                            (success, e) -> {
                                if (e != null) {
                                    LOGGER.debug("Failure for logical replication for subscription", e);
                                } else if (success) {
                                    LOGGER.debug("Acknowledged logical replication for subscription '{}'", subscriptionName);
                                    metadataTracker.startTracking(subscriptionName);
                                }
                            }
                        );
                }
            ).run();
        } else {
            remoteClusters.connect(subscriptionName, subscription.connectionInfo());
        }
    }

    private void removeSubscription(String subscriptionName) {
        assert repositoriesService != null
            : "RepositoriesService must be set immediately after LogicalReplicationService construction";
        LOGGER.debug("Removing logical replication repository for dropped subscription '{}'",
                     subscriptionName);
        repositoriesService.unregisterInternalRepository(REMOTE_REPOSITORY_PREFIX + subscriptionName);
        metadataTracker.stopTracking(subscriptionName);
        remoteClusters.remove(subscriptionName);
    }

    private Runnable withRetry(Runnable runnable) {
        return new RetryRunnable(
            threadPool.executor(ThreadPool.Names.LOGICAL_REPLICATION),
            threadPool.scheduler(),
            runnable,
            BackoffPolicy.exponentialBackoff()
        );
    }

    public Map<String, Subscription> subscriptions() {
        return currentSubscriptionsMetadata.subscription();
    }

    public Map<String, Publication> publications() {
        return currentPublicationsMetadata.publications();
    }

    @Override
    public void close() throws IOException {
        metadataTracker.close();
    }

    public CompletableFuture<Boolean> startReplication(String subscriptionName, Subscription subscription) {
        return getPublicationState(subscriptionName, subscription)
            .thenCompose(
                stateResponse -> {
                    verifyTablesDoNotExist(subscriptionName, stateResponse);
                    return initiateReplication(subscriptionName, subscription, stateResponse);
                }
            )
            .thenCompose(this::afterReplicationStarted);
    }

    public CompletableFuture<PublicationsStateAction.Response> getPublicationState(String subscriptionName,
                                                                                   Subscription subscription) {
        FutureActionListener<PublicationsStateAction.Response, PublicationsStateAction.Response> future =
            FutureActionListener.newInstance();
        remoteClusters.connect(subscriptionName, subscription.connectionInfo())
            .whenComplete((client, err) -> {
                if (err == null) {
                    client.execute(
                        PublicationsStateAction.INSTANCE,
                        new PublicationsStateAction.Request(
                            subscription.publications(),
                            subscription.connectionInfo().settings().get(ConnectionInfo.USERNAME.getKey())
                        ),
                        future
                    );
                } else {
                    future.onFailure(Exceptions.toRuntimeException(err));
                }
            });
        return future;
    }

    public void verifyTablesDoNotExist(String subscriptionName,
                                       PublicationsStateAction.Response stateResponse) {
        var metadata = clusterService.state().metadata();
        Consumer<RelationName> onExists = (relation) -> {
            var message = String.format(
                Locale.ENGLISH,
                "Subscription '%s' cannot be created as included relation '%s' already exists",
                subscriptionName,
                relation
            );
            throw new RelationAlreadyExists(relation, message);
        };
        for (var index : stateResponse.concreteIndices()) {
            if (metadata.hasIndex(index)) {
                onExists.accept(RelationName.fromIndexName(index));
            }
        }
        for (var template : stateResponse.concreteTemplates()) {
            if (metadata.templates().containsKey(template)) {
                onExists.accept(PartitionName.fromIndexOrTemplate(template).relationName());
            }
        }
    }

    private CompletableFuture<RestoreService.RestoreCompletionResponse> initiateReplication(
        String subscriptionName,
        Subscription subscription,
        PublicationsStateAction.Response stateResponse) {
        var publisherClusterRepoName = LogicalReplicationRepository.REMOTE_REPOSITORY_PREFIX + subscriptionName;
        final RestoreService.RestoreRequest restoreRequest =
            new RestoreService.RestoreRequest(
                publisherClusterRepoName,
                LogicalReplicationRepository.LATEST,
                stateResponse.concreteIndices().toArray(new String[0]),
                stateResponse.concreteTemplates().toArray(new String[0]),
                IndicesOptions.LENIENT_EXPAND_OPEN,
                null,
                null,
                subscription.settings(),
                DEFAULT_MASTER_NODE_TIMEOUT,
                false,
                true,
                Settings.EMPTY,
                Strings.EMPTY_ARRAY,
                "restore_logical_replication_snapshot[" + subscriptionName + "]",
                true,
                false,
                Strings.EMPTY_ARRAY,
                false,
                Strings.EMPTY_ARRAY
            );

        FutureActionListener<RestoreService.RestoreCompletionResponse, RestoreService.RestoreCompletionResponse> future =
            FutureActionListener.newInstance();
        threadPool.executor(ThreadPool.Names.SNAPSHOT).execute(new AbstractRunnable() {
            @Override
            public void onFailure(Exception e) {
                future.onFailure(e);
            }

            @Override
            protected void doRun() {
                restoreService.restoreSnapshot(restoreRequest, future);
            }
        });
        return future;
    }

    private CompletableFuture<Boolean> afterReplicationStarted(RestoreService.RestoreCompletionResponse response) {
        var future = new FutureActionListener<RestoreSnapshotResponse, Boolean>(
            restoreSnapshotResponse -> {
                RestoreInfo restoreInfo = restoreSnapshotResponse.getRestoreInfo();
                if (restoreInfo == null) {
                    LOGGER.error(
                        "Restore failed, restoreInfo = NULL, seems like a master failure happened while restoring");
                    return false;
                } else if (restoreInfo.failedShards() == 0) {
                    LOGGER.debug("Restore success, following will start once shards are active");
                    return true;
                } else {
                    assert restoreInfo.failedShards() > 0 : "Some failed shards are expected";
                    LOGGER.error("Failed to restore {}/{} shards",
                                 restoreInfo.failedShards(),
                                 restoreInfo.totalShards());
                    return false;
                }

            }
        );
        clusterService.addListener(new RestoreClusterStateListener(clusterService, response, future));
        return future;
    }
}
