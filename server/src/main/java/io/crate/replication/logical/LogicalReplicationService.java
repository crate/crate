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
import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.function.Consumer;

import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreClusterStateListener;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.snapshots.RestoreInfo;
import org.elasticsearch.snapshots.RestoreService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteClusters;
import org.elasticsearch.transport.TransportService;

import io.crate.common.io.IOUtils;
import io.crate.exceptions.RelationAlreadyExists;
import io.crate.metadata.PartitionName;
import io.crate.metadata.RelationName;
import io.crate.protocols.postgres.PgClientFactory;
import io.crate.replication.logical.action.PublicationsStateAction;
import io.crate.replication.logical.metadata.Publication;
import io.crate.replication.logical.metadata.PublicationsMetadata;
import io.crate.replication.logical.metadata.Subscription;
import io.crate.replication.logical.metadata.SubscriptionsMetadata;
import io.crate.replication.logical.repository.LogicalReplicationRepository;

public class LogicalReplicationService implements ClusterStateListener, Closeable {

    private static final Logger LOGGER = LogManager.getLogger(LogicalReplicationService.class);

    private final ClusterService clusterService;
    private final ThreadPool threadPool;
    private RepositoriesService repositoriesService;
    private RestoreService restoreService;

    private final RemoteClusters remoteClusters;
    private final Map<String, String> subscribedIndices = ConcurrentCollections.newConcurrentMap();
    private volatile SubscriptionsMetadata currentSubscriptionsMetadata = new SubscriptionsMetadata();
    private volatile PublicationsMetadata currentPublicationsMetadata = new PublicationsMetadata();


    public LogicalReplicationService(Settings settings,
                                     ClusterService clusterService,
                                     PgClientFactory pgClientFactory,
                                     TransportService transportService,
                                     ThreadPool threadPool) {
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.remoteClusters = new RemoteClusters(settings, threadPool, pgClientFactory, transportService);
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
            onChangedSubscriptions(prevSubscriptionsMetadata, newSubscriptionsMetadata);
        }

        PublicationsMetadata prevPublicationsMetadata = prevMetadata.custom(PublicationsMetadata.TYPE);
        PublicationsMetadata newPublicationsMetadata = newMetadata.custom(PublicationsMetadata.TYPE);

        if ((prevPublicationsMetadata == null && newPublicationsMetadata != null)
            || (prevPublicationsMetadata != null && prevPublicationsMetadata.equals(newPublicationsMetadata) == false)) {
            currentPublicationsMetadata = newPublicationsMetadata;
        }
    }

    private void onChangedSubscriptions(@Nullable SubscriptionsMetadata prevSubscriptionsMetadata,
                                        SubscriptionsMetadata newSubscriptionsMetadata) {
        Map<String, Subscription> oldSubscriptions = prevSubscriptionsMetadata == null
            ? Collections.emptyMap() : prevSubscriptionsMetadata.subscription();
        var newSubscriptions = newSubscriptionsMetadata.subscription();

        for (var entry : newSubscriptions.entrySet()) {
            var subscriptionName = entry.getKey();
            if (oldSubscriptions.get(subscriptionName) == null) {
                assert repositoriesService != null
                    : "RepositoriesService must be set immediately after LogicalReplicationService construction";

                LOGGER.debug("Adding new logical replication repository for subscription '{}'", subscriptionName);
                repositoriesService.registerInternalRepository(REMOTE_REPOSITORY_PREFIX + subscriptionName, TYPE);

                LOGGER.debug("Start logical replication for subscription '{}'", subscriptionName);
                startReplication(subscriptionName, entry.getValue(), new ActionListener<>() {

                    @Override
                    public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                        LOGGER.debug("Acknowledged logical replication for subscription '{}'", subscriptionName);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        LOGGER.debug("Failure for logical replication for subscription", e);
                    }
                });
            }
        }
        for (var entry : oldSubscriptions.entrySet()) {
            var subscriptionName = entry.getKey();
            if (newSubscriptions.get(subscriptionName) == null) {
                assert repositoriesService != null
                    : "RepositoriesService must be set immediately after LogicalReplicationService construction";
                LOGGER.debug("Removing logical replication repository for dropped subscription '{}'",
                             subscriptionName);
                repositoriesService.unregisterInternalRepository(REMOTE_REPOSITORY_PREFIX + subscriptionName);
            }
        }
    }

    public Map<String, Subscription> subscriptions() {
        return currentSubscriptionsMetadata.subscription();
    }

    public Map<String, Publication> publications() {
        return currentPublicationsMetadata.publications();
    }

    @Override
    public void close() throws IOException {
        IOUtils.close(remoteClusters);
    }

    public boolean isSubscribedIndex(String indexName) {
        return subscribedIndices.containsKey(indexName);
    }

    @Nullable
    public String subscriptionName(String indexName) {
        return subscribedIndices.get(indexName);
    }

    @Nullable
    public Subscription subscription(String indexName) {
        var subscriptionName = subscribedIndices.get(indexName);
        if (subscriptionName != null) {
            return currentSubscriptionsMetadata.subscription().get(subscriptionName);
        }
        return null;
    }

    public void startReplication(String subscriptionName,
                                 Subscription subscription,
                                 ActionListener<AcknowledgedResponse> listener) {
        getPublicationState(
            subscriptionName,
            subscription,
            new ActionListener<>() {

                @Override
                public void onResponse(PublicationsStateAction.Response stateResponse) {
                    verifyTablesDoNotExist(
                        subscriptionName,
                        stateResponse,
                        r -> {
                            for (var index : stateResponse.concreteIndices()) {
                                subscribedIndices.put(index, subscriptionName);
                            }
                            initiateReplication(subscriptionName, subscription, stateResponse, listener);
                        },
                        listener::onFailure
                    );
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }
            }
        );
    }

    public void getPublicationState(String subscriptionName,
                                    Subscription subscription,
                                    ActionListener<PublicationsStateAction.Response> listener) {
        remoteClusters.connectAndGetClient(subscriptionName, subscription.connectionInfo())
            .whenComplete((client, err) -> {
                client.execute(
                    PublicationsStateAction.INSTANCE,
                    new PublicationsStateAction.Request(subscription.publications()),
                    listener
                );
            });
    }

    public void verifyTablesDoNotExist(String subscriptionName,
                                       PublicationsStateAction.Response stateResponse,
                                       Consumer<Void> onSuccess,
                                       Consumer<Exception> onFailure) {
        var metadata = clusterService.state().metadata();
        Consumer<RelationName> onExists = (relation) -> {
            var message = String.format(
                Locale.ENGLISH,
                "Subscription '%s' cannot be created as included relation '%s' already exists",
                subscriptionName,
                relation
            );
            onFailure.accept(new RelationAlreadyExists(relation, message));
        };
        for (var index : stateResponse.concreteIndices()) {
            if (metadata.hasIndex(index)) {
                onExists.accept(RelationName.fromIndexName(index));
                return;
            }
        }
        for (var template : stateResponse.concreteTemplates()) {
            if (metadata.templates().containsKey(template)) {
                onExists.accept(PartitionName.fromIndexOrTemplate(template).relationName());
                return;
            }
        }
        onSuccess.accept(null);
    }


    private void initiateReplication(String subscriptionName,
                                     Subscription subscription,
                                     PublicationsStateAction.Response stateResponse,
                                     ActionListener<AcknowledgedResponse> listener) {
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

        threadPool.executor(ThreadPool.Names.SNAPSHOT).execute(new AbstractRunnable() {

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }

            @Override
            protected void doRun() {
                restoreService.restoreSnapshot(
                    restoreRequest,
                    ActionListener.delegateFailure(
                        listener,
                        (delegatedListener, response) ->
                            afterReplicationStarted(delegatedListener, response)
                    )
                );
            }

            @Override
            public void onAfter() {
                super.onAfter();
                listener.onResponse(new AcknowledgedResponse(true));
            }
        });
    }

    private void afterReplicationStarted(ActionListener<AcknowledgedResponse> listener,
                                         RestoreService.RestoreCompletionResponse response) {
        RestoreClusterStateListener.createAndRegisterListener(
            clusterService,
            response,
            ActionListener.delegateFailure(listener, (delegatedListener, restoreSnapshotResponse) -> {
                RestoreInfo restoreInfo = restoreSnapshotResponse.getRestoreInfo();
                if (restoreInfo == null) {
                    LOGGER.error(
                        "Restore failed, restoreInfo = NULL, seems like a master failure happened while restoring");
                    delegatedListener.onResponse(new AcknowledgedResponse(false));
                } else if (restoreInfo.failedShards() == 0) {
                    LOGGER.debug("Restore success, following will start once shards are active");
                    delegatedListener.onResponse(new AcknowledgedResponse(true));
                } else {
                    assert restoreInfo.failedShards() > 0 : "Some failed shards are expected";
                    LOGGER.error("Failed to restore {}/{} shards",
                                 restoreInfo.failedShards(),
                                 restoreInfo.totalShards());
                    delegatedListener.onResponse(new AcknowledgedResponse(false));
                }
            })
        );
    }

    public Client getRemoteClusterClient(String subscriptionName) {
        return remoteClusters.getClient(subscriptionName);
    }
}
