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

import io.crate.common.io.IOUtils;
import io.crate.common.unit.TimeValue;
import io.crate.exceptions.RelationAlreadyExists;
import io.crate.metadata.PartitionName;
import io.crate.metadata.RelationName;
import io.crate.replication.logical.action.PublicationsStateAction;
import io.crate.replication.logical.metadata.Publication;
import io.crate.replication.logical.metadata.PublicationsMetadata;
import io.crate.replication.logical.metadata.Subscription;
import io.crate.replication.logical.metadata.SubscriptionsMetadata;
import io.crate.replication.logical.repository.LogicalReplicationRepository;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreClusterStateListener;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.snapshots.RestoreInfo;
import org.elasticsearch.snapshots.RestoreService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.NoSuchRemoteClusterException;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.transport.RemoteClusterAwareClient;
import org.elasticsearch.transport.RemoteClusterConnection;
import org.elasticsearch.transport.RemoteConnectionStrategy;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportService;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static io.crate.replication.logical.repository.LogicalReplicationRepository.REMOTE_REPOSITORY_PREFIX;
import static io.crate.replication.logical.repository.LogicalReplicationRepository.TYPE;
import static org.elasticsearch.action.support.master.MasterNodeRequest.DEFAULT_MASTER_NODE_TIMEOUT;

public class LogicalReplicationService extends RemoteClusterAware implements ClusterStateListener, Closeable {

    private static final Logger LOGGER = LogManager.getLogger(LogicalReplicationService.class);

    private final TransportService transportService;
    private final ClusterService clusterService;
    private final ThreadPool threadPool;
    private RepositoriesService repositoriesService;
    private RestoreService restoreService;

    private final Map<String, RemoteClusterConnection> remoteClusters = ConcurrentCollections.newConcurrentMap();
    private final Map<String, String> subscribedIndices = ConcurrentCollections.newConcurrentMap();
    private volatile SubscriptionsMetadata currentSubscriptionsMetadata = new SubscriptionsMetadata();
    private volatile PublicationsMetadata currentPublicationsMetadata = new PublicationsMetadata();

    public LogicalReplicationService(Settings settings,
                                     ClusterService clusterService,
                                     TransportService transportService,
                                     ThreadPool threadPool) {
        super(settings);
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
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
        IOUtils.close(remoteClusters.values());
    }

    @Override
    protected void updateRemoteCluster(String clusterAlias, Settings settings) {
        updateRemoteConnection(clusterAlias, settings);
    }

    @Override
    public Transport.Connection getConnection(DiscoveryNode node, String cluster) {
        return getRemoteClusterConnection(cluster).getConnection(node);
    }

    @Override
    public Transport.Connection getConnection(String cluster) {
        return getRemoteClusterConnection(cluster).getConnection();
    }

    @Override
    public void ensureConnected(String clusterAlias, ActionListener<Void> listener) {
        getRemoteClusterConnection(clusterAlias).ensureConnected(listener);
    }

    RemoteClusterConnection getRemoteClusterConnection(String cluster) {
        RemoteClusterConnection connection = remoteClusters.get(cluster);
        if (connection == null) {
            throw new NoSuchRemoteClusterException(cluster);
        }
        return connection;
    }

    @Override
    public Client getRemoteClusterClient(ThreadPool threadPool,
                                         String clusterAlias) {
        if (remoteClusters.containsKey(clusterAlias) == false) {
            throw new NoSuchRemoteClusterException(clusterAlias);
        }
        return new RemoteClusterAwareClient(settings, threadPool, transportService, clusterAlias, this);
    }

    public void updateRemoteConnection(String name, Settings connectionSettings) {
        CountDownLatch latch = new CountDownLatch(1);
        updateRemoteConnection(name, connectionSettings, ActionListener.wrap(latch::countDown));

        try {
            // Wait 10 seconds for a connections. We must use a latch instead of a future because we
            // are on the cluster state thread and our custom future implementation will throw an
            // assertion.
            if (latch.await(10, TimeUnit.SECONDS) == false) {
                LOGGER.warn("failed to connect to new remote cluster {} within {}",
                            name,
                            TimeValue.timeValueSeconds(10));
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public synchronized void updateRemoteConnection(String name,
                                                    Settings connectionSettings,
                                                    ActionListener<Void> listener) {
        RemoteClusterConnection remote = this.remoteClusters.get(name);
        if (RemoteConnectionStrategy.isConnectionEnabled(connectionSettings) == false) {
            try {
                IOUtils.close(remote);
            } catch (IOException e) {
                LOGGER.warn("failed to close remote cluster connections for cluster: " + name, e);
            }
            remoteClusters.remove(name);
            listener.onResponse(null);
            return;
        }

        if (remote == null) {
            // this is a new cluster we have to add a new representation
            remote = new RemoteClusterConnection(settings, connectionSettings, name, transportService);
            remoteClusters.put(name, remote);
            remote.ensureConnected(listener);
        } else if (remote.shouldRebuildConnection(connectionSettings)) {
            // Changes to connection configuration. Must tear down existing connection
            try {
                IOUtils.close(remote);
            } catch (IOException e) {
                LOGGER.warn("failed to close remote cluster connections for cluster: " + name, e);
            }
            remoteClusters.remove(name);
            remote = new RemoteClusterConnection(settings, connectionSettings, name, transportService);
            remoteClusters.put(name, remote);
            remote.ensureConnected(listener);
        } else {
            // No changes to connection configuration.
            listener.onResponse(null);
        }
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
        updateRemoteConnection(
            subscriptionName,
            subscription.connectionInfo().settings(),
            ActionListener.delegateFailure(
                listener,
                (delegatedListener, response) -> {
                    var remoteClient = getRemoteClusterClient(
                        threadPool,
                        subscriptionName
                    );
                    remoteClient.execute(
                        PublicationsStateAction.INSTANCE,
                        new PublicationsStateAction.Request(subscription.publications()),
                        listener
                    );
                }
            )
        );
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
}
