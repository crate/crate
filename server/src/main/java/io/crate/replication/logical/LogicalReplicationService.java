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
import io.crate.exceptions.RelationAlreadyExists;
import io.crate.exceptions.SQLExceptions;
import io.crate.execution.support.RetryRunnable;
import io.crate.metadata.PartitionName;
import io.crate.metadata.RelationName;
import io.crate.replication.logical.action.PublicationsStateAction;
import io.crate.replication.logical.action.UpdateSubscriptionAction;
import io.crate.replication.logical.metadata.ConnectionInfo;
import io.crate.replication.logical.metadata.Publication;
import io.crate.replication.logical.metadata.PublicationsMetadata;
import io.crate.replication.logical.metadata.Subscription;
import io.crate.replication.logical.metadata.SubscriptionsMetadata;
import io.crate.replication.logical.repository.LogicalReplicationRepository;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreClusterStateListener;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.snapshots.RestoreInfo;
import org.elasticsearch.snapshots.RestoreService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteClusters;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

import static io.crate.replication.logical.repository.LogicalReplicationRepository.REMOTE_REPOSITORY_PREFIX;
import static io.crate.replication.logical.repository.LogicalReplicationRepository.TYPE;
import static org.elasticsearch.action.support.master.MasterNodeRequest.DEFAULT_MASTER_NODE_TIMEOUT;

public class LogicalReplicationService implements ClusterStateListener, Closeable {

    private static final Logger LOGGER = LogManager.getLogger(LogicalReplicationService.class);

    private final ClusterService clusterService;
    private final ThreadPool threadPool;
    private final RemoteClusters remoteClusters;
    private final Client client;
    private RepositoriesService repositoriesService;
    private RestoreService restoreService;

    private volatile SubscriptionsMetadata currentSubscriptionsMetadata = new SubscriptionsMetadata();
    private volatile PublicationsMetadata currentPublicationsMetadata = new PublicationsMetadata();
    private final MetadataTracker metadataTracker;

    public LogicalReplicationService(IndexScopedSettings indexScopedSettings,
                                     Settings settings,
                                     ClusterService clusterService,
                                     RemoteClusters remoteClusters,
                                     ThreadPool threadPool,
                                     Client client) {
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.remoteClusters = remoteClusters;
        this.client = client;
        this.metadataTracker = new MetadataTracker(
            indexScopedSettings,
            settings,
            threadPool,
            remoteClusters::getClient,
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

        if (clusterService.state().nodes().isLocalNodeElectedMaster()) {
            withRetry(
                () -> {
                    // The startReplication will initiate the remote connection upfront
                    LOGGER.debug("Start logical replication for subscription '{}'", subscriptionName);
                    startReplication(subscriptionName, subscription)
                        .whenComplete(
                            (success, e) -> {
                                if (e != null) {
                                    e = SQLExceptions.unwrap(e);
                                    LOGGER.debug("Failure for logical replication for subscription", e);
                                    // Set a generic error reason if state is not already "FAILED" (same states won't be overridden)
                                    updateSubscriptionState(
                                        subscriptionName,
                                        Subscription.State.FAILED,
                                        e.getMessage()
                                    );
                                } else if (success) {
                                    LOGGER.debug("Acknowledged logical replication for subscription '{}'",
                                                 subscriptionName);
                                    updateSubscriptionState(
                                        subscriptionName,
                                        Subscription.State.MONITORING,
                                        null
                                    ).thenAccept(
                                        ignored -> metadataTracker.startTracking(subscriptionName)
                                    );
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
        return getPublicationState(subscriptionName, subscription.publications(), subscription.connectionInfo())
            .thenCompose(
                stateResponse -> {
                    verifyTablesDoNotExist(subscriptionName, stateResponse);
                    return initiateReplication(subscriptionName, subscription, stateResponse);
                })
            .thenCompose(response -> afterReplicationStarted(subscriptionName, response));
    }

    public CompletableFuture<PublicationsStateAction.Response> getPublicationState(String subscriptionName,
                                                                                   List<String> publications,
                                                                                   ConnectionInfo connectionInfo) {
        FutureActionListener<PublicationsStateAction.Response, PublicationsStateAction.Response> finalFuture =
            FutureActionListener.newInstance();
        BiConsumer<String, Throwable> onError = (message, err) -> {
            var subscriptionStateFuture = updateSubscriptionState(
                subscriptionName,
                Subscription.State.FAILED,
                message
            );
            subscriptionStateFuture.whenComplete(
                (stateResponse, ignoredErr) -> finalFuture.completeExceptionally(err)
            );
        };

        remoteClusters.connect(subscriptionName, connectionInfo)
            .whenComplete(
                (client, err) -> {
                    if (err == null) {
                        client.execute(
                            PublicationsStateAction.INSTANCE,
                            new PublicationsStateAction.Request(
                                publications,
                                connectionInfo.settings().get(ConnectionInfo.USERNAME.getKey())
                            ),
                            ActionListener.delegateResponse(
                                finalFuture,
                                (d, stateError) -> onError.accept("Failed to request the publications state",
                                                                  stateError)
                            )
                        );
                    } else {
                        onError.accept("Failed to connect to the remote cluster", err);
                    }
                }
            );
        return finalFuture;
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

        var finalFuture = new CompletableFuture<RestoreService.RestoreCompletionResponse>();
        FutureActionListener<RestoreService.RestoreCompletionResponse, RestoreService.RestoreCompletionResponse> restoreFuture =
            FutureActionListener.newInstance();
        restoreFuture.whenComplete(
            (response, err) -> {
                if (err == null) {
                    updateSubscriptionState(
                        subscriptionName,
                        Subscription.State.RESTORING,
                        null
                    ).thenAccept(ignored -> finalFuture.complete(response));
                } else {
                    updateSubscriptionState(
                        subscriptionName,
                        Subscription.State.FAILED,
                        "Failed restoring subscription, error=" + err.getMessage()
                    ).whenComplete((ignored, ignoredErr) -> finalFuture.completeExceptionally(err));
                }
            }
        );

        threadPool.executor(ThreadPool.Names.SNAPSHOT).execute(
            () -> {
                try {
                    restoreService.restoreSnapshot(restoreRequest, restoreFuture);
                } catch (Exception e) {
                    restoreFuture.onFailure(e);
                }
            }
        );

        return finalFuture;
    }

    private CompletableFuture<Boolean> afterReplicationStarted(String subscriptionName,
                                                               RestoreService.RestoreCompletionResponse response) {
        var future = new CompletableFuture<Boolean>();
        var restoreFuture = new FutureActionListener<RestoreSnapshotResponse, RestoreSnapshotResponse>(
            Function.identity());
        restoreFuture.whenComplete(
            (restoreSnapshotResponse, err) -> {
                if (err == null) {
                    RestoreInfo restoreInfo = restoreSnapshotResponse.getRestoreInfo();
                    if (restoreInfo == null) {
                        LOGGER.error(
                            "Restore failed, restoreInfo = NULL, seems like a master failure happened while restoring");
                        updateSubscriptionState(
                            subscriptionName,
                            Subscription.State.FAILED,
                            "Error while initial restoring the subscription relations"
                        ).whenComplete(
                            (ignore, ignoredErr) -> future.complete(false)
                        );
                    } else if (restoreInfo.failedShards() == 0) {
                        LOGGER.debug("Restore success, following will start once shards are active");
                        updateSubscriptionState(
                            subscriptionName,
                            Subscription.State.SYNCHRONIZED,
                            null
                        ).whenComplete(
                            (ignore, ignoredErr) -> future.complete(true)
                        );
                    } else {
                        assert restoreInfo.failedShards() > 0 : "Some failed shards are expected";
                        LOGGER.error("Failed to restore {}/{} shards",
                                     restoreInfo.failedShards(),
                                     restoreInfo.totalShards());
                        var msg = "Error while initial restoring the subscription relations";
                        if (restoreInfo.failedShards() != restoreInfo.totalShards()) {
                            msg = String.format(Locale.ENGLISH,
                                                "Restoring the subscription relations failed partially. Failed to restore %d/%d shards",
                                                restoreInfo.failedShards(),
                                                restoreInfo.totalShards());
                        }
                        updateSubscriptionState(
                            subscriptionName,
                            Subscription.State.FAILED,
                            msg
                        ).whenComplete(
                            (ignore, ignoredErr) -> future.complete(false)
                        );
                    }
                } else {
                    updateSubscriptionState(
                        subscriptionName,
                        Subscription.State.FAILED,
                        "Error while initial restoring the subscription relations"
                    ).whenComplete(
                        (ignore, ignoredErr) -> future.completeExceptionally(err)
                    );
                }
            }
        );
        clusterService.addListener(new RestoreClusterStateListener(clusterService, response, restoreFuture));
        return future;
    }

    private CompletableFuture<Boolean> updateSubscriptionState(
        final String subscriptionName,
        final Subscription.State newState,
        @Nullable final String failureReason) {
        var oldSubscription = subscriptions().get(subscriptionName);
        if (oldSubscription == null) {
            return CompletableFuture.completedFuture(false);
        }
        HashMap<RelationName, Subscription.RelationState> relations = new HashMap<>();
        for (var entry : oldSubscription.relations().entrySet()) {
            relations.put(entry.getKey(), new Subscription.RelationState(newState, failureReason));
        }
        var newSubscription = new Subscription(
            oldSubscription.owner(),
            oldSubscription.connectionInfo(),
            oldSubscription.publications(),
            oldSubscription.settings(),
            relations
        );
        var request = new UpdateSubscriptionAction.Request(subscriptionName, newSubscription);
        var future = new FutureActionListener<>(AcknowledgedResponse::isAcknowledged);
        client.execute(UpdateSubscriptionAction.INSTANCE, request, future);
        return future;
    }
}
