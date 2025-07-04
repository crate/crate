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

import static io.crate.replication.logical.repository.LogicalReplicationRepository.REMOTE_REPOSITORY_PREFIX;
import static io.crate.replication.logical.repository.LogicalReplicationRepository.TYPE;
import static org.elasticsearch.action.support.master.MasterNodeRequest.DEFAULT_MASTER_NODE_TIMEOUT;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreClusterStateListener;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.restore.TableOrPartition;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.MetadataUpgradeService;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.snapshots.RestoreInfo;
import org.elasticsearch.snapshots.RestoreService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteClusters;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.VisibleForTesting;

import io.crate.concurrent.FutureActionListener;
import io.crate.exceptions.SubscriptionRestoreException;
import io.crate.metadata.RelationName;
import io.crate.replication.logical.action.PublicationsStateAction;
import io.crate.replication.logical.action.UpdateSubscriptionAction;
import io.crate.replication.logical.metadata.ConnectionInfo;
import io.crate.replication.logical.metadata.Publication;
import io.crate.replication.logical.metadata.PublicationsMetadata;
import io.crate.replication.logical.metadata.Subscription;
import io.crate.replication.logical.metadata.SubscriptionsMetadata;
import io.crate.replication.logical.repository.LogicalReplicationRepository;

public class LogicalReplicationService implements ClusterStateListener, Closeable {

    private static final Logger LOGGER = LogManager.getLogger(LogicalReplicationService.class);

    private final ClusterService clusterService;
    private final ThreadPool threadPool;
    private final RemoteClusters remoteClusters;
    private final Client client;
    private final AtomicInteger activeOperations = new AtomicInteger(0);
    private final MetadataUpgradeService metadataUpgradeService;
    private RepositoriesService repositoriesService;
    private RestoreService restoreService;

    private volatile SubscriptionsMetadata currentSubscriptionsMetadata = new SubscriptionsMetadata();
    private volatile PublicationsMetadata currentPublicationsMetadata = new PublicationsMetadata();
    private final MetadataTracker metadataTracker;

    public LogicalReplicationService(Settings settings,
                                     IndexScopedSettings indexScopedSettings,
                                     ClusterService clusterService,
                                     RemoteClusters remoteClusters,
                                     ThreadPool threadPool,
                                     Client client,
                                     AllocationService allocationService,
                                     MetadataUpgradeService metadataUpgradeService,
                                     LogicalReplicationSettings replicationSettings) {
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.remoteClusters = remoteClusters;
        this.client = client;
        this.metadataUpgradeService = metadataUpgradeService;
        this.metadataTracker = new MetadataTracker(
            settings,
            indexScopedSettings,
            threadPool,
            this,
            replicationSettings,
            remoteClusters::getClient,
            clusterService,
            allocationService,
            metadataUpgradeService
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

        PublicationsMetadata publicationsMetaData = newMetadata.custom(PublicationsMetadata.TYPE);
        if (publicationsMetaData != null) {
            currentPublicationsMetadata = publicationsMetaData;
        }

        SubscriptionsMetadata prevSubscriptionsMetadata = SubscriptionsMetadata.get(prevMetadata);
        SubscriptionsMetadata newSubscriptionsMetadata = SubscriptionsMetadata.get(newMetadata);

        boolean subscriptionsChanged = prevSubscriptionsMetadata.equals(newSubscriptionsMetadata) == false;
        if (subscriptionsChanged) {
            currentSubscriptionsMetadata = newSubscriptionsMetadata;
            addAndRemoveRepositories(prevSubscriptionsMetadata, newSubscriptionsMetadata);
        }

        if (subscriptionsChanged || event.nodesDelta().masterNodeChanged()) {
            if (event.localNodeMaster()) {
                metadataTracker.update(newSubscriptionsMetadata.subscription().keySet());
            } else {
                metadataTracker.close();
            }
        }
    }

    /**
     * Register/unregister logical replication repositories.
     * If on a MASTER node, initial restore will be triggered.
     */
    private void addAndRemoveRepositories(SubscriptionsMetadata prevSubscriptionsMetadata,
                                          SubscriptionsMetadata newSubscriptionsMetadata) {
        var oldSubscriptions = prevSubscriptionsMetadata.subscription();
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
        remoteClusters.connect(subscriptionName, subscription.connectionInfo());
    }

    private void removeSubscription(String subscriptionName) {
        assert repositoriesService != null
            : "RepositoriesService must be set immediately after LogicalReplicationService construction";
        LOGGER.debug("Removing logical replication repository for dropped subscription '{}'",
                     subscriptionName);
        repositoriesService.unregisterInternalRepository(REMOTE_REPOSITORY_PREFIX + subscriptionName);
        remoteClusters.remove(subscriptionName);
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

    public CompletableFuture<PublicationsStateAction.Response> getPublicationState(String subscriptionName,
                                                                                   List<String> publications,
                                                                                   ConnectionInfo connectionInfo) {
        FutureActionListener<PublicationsStateAction.Response> finalFuture = new FutureActionListener<>();
        BiConsumer<String, Throwable> onError = (message, err) -> {
            var subscriptionStateFuture = updateSubscriptionState(
                subscriptionName,
                Subscription.State.FAILED,
                message
            );
            subscriptionStateFuture.whenComplete((stateResponse, suppressedErr) -> {
                if (suppressedErr != null) {
                    err.addSuppressed(suppressedErr);
                }
                finalFuture.completeExceptionally(err);
            });
        };

        remoteClusters.connect(subscriptionName, connectionInfo)
            .whenComplete((client, err) -> {
                if (err == null) {
                    client.execute(
                            PublicationsStateAction.INSTANCE,
                            new PublicationsStateAction.Request(
                                publications,
                                connectionInfo.settings().get(ConnectionInfo.USERNAME.getKey())
                            ))
                        .thenApply(r ->
                            new PublicationsStateAction.Response(
                                metadataUpgradeService.upgradeMetadata(r.metadata()),
                                r.unknownPublications()
                            ))
                        .whenComplete((d, stateErr) -> {
                            if (stateErr == null) {
                                finalFuture.complete(d);
                            } else {
                                onError.accept("Failed to request the publications state", stateErr);
                            }
                        });
                } else {
                    onError.accept("Failed to connect to the remote cluster", err);
                }
            });
        return finalFuture;
    }

    /**
     * Restore new subscribed relations using the {@link RestoreService} and update the
     * {@link Subscription.RelationState} of each given {@link RelationName} based on the result.
     *
     * @param subscriptionName      The name of the subscription relations will be restored for
     * @param restoreSettings       The restore settings needed by the {@link RestoreService}
     * @param tablesToRestore       List of {@link TableOrPartition}'s to update their {@link Subscription.RelationState}
     */
    public CompletableFuture<Boolean> restore(String subscriptionName,
                                              Settings restoreSettings,
                                              List<TableOrPartition> tablesToRestore) {
        var publisherClusterRepoName = LogicalReplicationRepository.REMOTE_REPOSITORY_PREFIX + subscriptionName;
        final RestoreService.RestoreRequest restoreRequest =
            new RestoreService.RestoreRequest(
                publisherClusterRepoName,
                LogicalReplicationRepository.LATEST,
                IndicesOptions.LENIENT_EXPAND_OPEN,
                restoreSettings,
                DEFAULT_MASTER_NODE_TIMEOUT,
                true,
                false,
                Strings.EMPTY_ARRAY,
                false,
                Strings.EMPTY_ARRAY
            );

        FutureActionListener<RestoreService.RestoreCompletionResponse> restoreListener = new FutureActionListener<>();
        activeOperations.incrementAndGet();
        Set<RelationName> relationNames = tablesToRestore.stream()
            .map(TableOrPartition::table)
            .collect(Collectors.toUnmodifiableSet());
        var restoreFuture = restoreListener
            .whenComplete((_, _) -> activeOperations.decrementAndGet())
            // Update subscription state, we want to wait until this update is done before proceeding
            .thenCompose(_ -> updateSubscriptionState(
                subscriptionName,
                relationNames,
                Subscription.State.RESTORING,
                null
            ))
            .thenCompose(_ -> afterReplicationStarted(subscriptionName, restoreListener.join(), relationNames));

        try {
            threadPool.executor(ThreadPool.Names.SNAPSHOT).execute(
                () -> {
                    try {
                        restoreService.restoreSnapshot(restoreRequest, tablesToRestore, restoreListener);
                    } catch (Exception e) {
                        restoreListener.onFailure(e);
                    }
                }
            );
        } catch (RejectedExecutionException ex) {
            restoreListener.onFailure(ex);
        }

        return restoreFuture;
    }

    private CompletableFuture<Boolean> afterReplicationStarted(String subscriptionName,
                                                               RestoreService.RestoreCompletionResponse response,
                                                               Collection<RelationName> relationNames) {
        Function<RestoreInfo, CompletableFuture<Boolean>> onRestoreInfo = restoreInfo -> {
            if (restoreInfo == null || restoreInfo.failedShards() == 0) {
                LOGGER.debug("Restore success, following will start once shards are active");
                return updateSubscriptionState(
                    subscriptionName,
                    relationNames,
                    Subscription.State.MONITORING,
                    null
                );
            } else {
                assert restoreInfo.failedShards() > 0 : "Some failed shards are expected";
                LOGGER.error("Failed to restore {}/{} shards", restoreInfo.failedShards(), restoreInfo.totalShards());
                var msg = "Error while initial restoring the subscription relations";
                if (restoreInfo.failedShards() != restoreInfo.totalShards()) {
                    msg = String.format(Locale.ENGLISH,
                                        "Restoring the subscription relations failed partially. Failed to restore %d/%d shards",
                                        restoreInfo.failedShards(),
                                        restoreInfo.totalShards());
                }
                throw new SubscriptionRestoreException(msg);
            }
        };
        if (response.getRestoreInfo() != null) {
            LOGGER.debug("Restore completed immediately, no shards to wait for using a cluster state listener");
            return onRestoreInfo.apply(response.getRestoreInfo());
        } else {
            FutureActionListener<RestoreSnapshotResponse> restoreFuture = new FutureActionListener<>();
            // Restore still in progress, add listener to wait for it
            clusterService.addListener(new RestoreClusterStateListener(clusterService, response, restoreFuture));
            return restoreFuture.thenCompose(resp -> onRestoreInfo.apply(resp.getRestoreInfo()));
        }
    }

    public CompletableFuture<Boolean> updateSubscriptionState(String subscriptionName,
                                                              Collection<RelationName> relationNames,
                                                              Subscription.State newState,
                                                              @Nullable String failureReason) {
        var oldSubscription = subscriptions().get(subscriptionName);
        if (oldSubscription == null) {
            return CompletableFuture.completedFuture(false);
        }
        HashMap<RelationName, Subscription.RelationState> relations = new HashMap<>(oldSubscription.relations());
        for (var relationName : relationNames) {
            relations.put(relationName, new Subscription.RelationState(newState, failureReason));
        }
        return updateSubscriptionState(subscriptionName, oldSubscription, relations);
    }

    public CompletableFuture<Boolean> updateSubscriptionState(String subscriptionName,
                                                              Subscription.State newState,
                                                              @Nullable final String failureReason) {
        var oldSubscription = subscriptions().get(subscriptionName);
        if (oldSubscription == null) {
            return CompletableFuture.completedFuture(false);
        }
        HashMap<RelationName, Subscription.RelationState> relations = new HashMap<>();
        for (var entry : oldSubscription.relations().entrySet()) {
            relations.put(entry.getKey(), new Subscription.RelationState(newState, failureReason));
        }
        return updateSubscriptionState(subscriptionName, oldSubscription, relations);
    }

    private CompletableFuture<Boolean> updateSubscriptionState(String subscriptionName,
                                                               Subscription subscription,
                                                               Map<RelationName, Subscription.RelationState> relations) {
        var newSubscription = new Subscription(
            subscription.owner(),
            subscription.connectionInfo(),
            subscription.publications(),
            subscription.settings(),
            relations
        );
        var request = new UpdateSubscriptionAction.Request(subscriptionName, newSubscription);
        return client.execute(UpdateSubscriptionAction.INSTANCE, request)
            .thenApply(AcknowledgedResponse::isAcknowledged);
    }

    @VisibleForTesting
    public boolean isActive() {
        return metadataTracker.isActive() || activeOperations.get() > 0;
    }
}
