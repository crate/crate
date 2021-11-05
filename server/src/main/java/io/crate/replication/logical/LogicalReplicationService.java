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
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreClusterStateListener;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.CheckedConsumer;
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
    private volatile SubscriptionsMetadata currentSubscriptionsMetadata = new SubscriptionsMetadata();
    private volatile PublicationsMetadata currentPublicationsMetadata = new PublicationsMetadata();
    private final MetadataTracker metadataTracker;

    public LogicalReplicationService(Settings settings,
                                     ClusterService clusterService,
                                     TransportService transportService,
                                     ThreadPool threadPool) {
        super(settings);
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.metadataTracker = new MetadataTracker(
            settings,
            threadPool,
            subscriptionName -> getRemoteClusterClient(threadPool, subscriptionName),
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
            boolean subscriptionRemoved = newSubscriptions.get(subscriptionName) == null;
            if (subscriptionRemoved) {
                removeSubscription(subscriptionName);
            }
            // Close connection for removed subscription (must not run inside a transport thread)
            withRetry(() -> updateRemoteConnection(subscriptionName, Settings.EMPTY));
        }
    }

    private void addSubscription(String subscriptionName, Subscription subscription) {
        assert repositoriesService != null
            : "RepositoriesService must be set immediately after LogicalReplicationService construction";
        LOGGER.debug("Adding new logical replication repository for subscription '{}'", subscriptionName);
        repositoriesService.registerInternalRepository(REMOTE_REPOSITORY_PREFIX + subscriptionName, TYPE);

        if (clusterService.localNode().isMasterNode()) {
            withRetry(
                () -> {
                    // The startReplication will initiate the remote connection upfront
                    LOGGER.debug("Start logical replication for subscription '{}'", subscriptionName);
                    startReplication(subscriptionName, subscription, new ActionListener<>() {
                        @Override
                        public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                            LOGGER.debug("Acknowledged logical replication for subscription '{}'", subscriptionName);
                            metadataTracker.startTracking(subscriptionName);
                        }

                        @Override
                        public void onFailure(Exception e) {
                            LOGGER.debug("Failure for logical replication for subscription", e);
                        }
                    });
                }
            ).run();
        } else {
            // Initiate the remote connection for the subscription, needed on all nodes to restore the
            // repository and for following remote shard changes.
            // Must run inside a transport thread
            withRetry(() -> updateRemoteConnection(subscriptionName)).run();
        }
    }

    private void removeSubscription(String subscriptionName) {
        assert repositoriesService != null
            : "RepositoriesService must be set immediately after LogicalReplicationService construction";
        LOGGER.debug("Removing logical replication repository for dropped subscription '{}'",
                     subscriptionName);
        repositoriesService.unregisterInternalRepository(REMOTE_REPOSITORY_PREFIX + subscriptionName);
        metadataTracker.stopTracking(subscriptionName);
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

    private RemoteClusterConnection getRemoteClusterConnection(String cluster) {
        RemoteClusterConnection connection = remoteClusters.get(cluster);
        if (connection == null) {
            throw new NoSuchRemoteClusterException(cluster);
        }
        return connection;
    }

    @Override
    public Client getRemoteClusterClient(ThreadPool threadPool,
                                         String subcriptionName) {
        if (remoteClusters.containsKey(subcriptionName) == false) {
            throw new NoSuchRemoteClusterException(subcriptionName);
        }
        return new RemoteClusterAwareClient(settings, threadPool, transportService, subcriptionName, this);
    }

    private void updateRemoteConnection(String subscriptionName) {
        var subscription = subscriptions().get(subscriptionName);
        if (subscription != null) {
            updateRemoteConnection(subscriptionName, subscription.connectionInfo().settings());
        }
    }

    private void updateRemoteConnection(String name, Settings connectionSettings) {
        CountDownLatch latch = new CountDownLatch(1);
        updateRemoteConnection(name, connectionSettings, r -> latch.countDown(), e -> latch.countDown());

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
                                                    Settings connectionSettingsOfSubscription,
                                                    CheckedConsumer<Void, Exception> onSuccess,
                                                    Consumer<Exception> onFailure) {
        RemoteClusterConnection remote = this.remoteClusters.get(name);
        // The ConnectionManager uses node settings in addition to connection settings
        var connectionSettings = Settings.builder()
            .put(settings)
            .put(connectionSettingsOfSubscription)
            .build();
        if (RemoteConnectionStrategy.isConnectionEnabled(connectionSettings) == false) {
            try {
                IOUtils.close(remote);
            } catch (IOException e) {
                LOGGER.warn("failed to close remote cluster connections for cluster: " + name, e);
            }
            remoteClusters.remove(name);
            try {
                onSuccess.accept(null);
            } catch (Exception e) {
                onFailure.accept(e);
            }
            return;
        }

        if (remote == null) {
            // this is a new cluster we have to add a new representation
            remote = new RemoteClusterConnection(settings, connectionSettings, name, transportService);
            remoteClusters.put(name, remote);
            remote.ensureConnected(ActionListener.wrap(onSuccess, onFailure));
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
            remote.ensureConnected(ActionListener.wrap(onSuccess, onFailure));
        } else {
            // No changes to connection configuration.
            try {
                onSuccess.accept(null);
            } catch (Exception e) {
                onFailure.accept(e);
            }
        }
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
                        r -> initiateReplication(subscriptionName, subscription, stateResponse, listener),
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
            response -> {
                var remoteClient = getRemoteClusterClient(
                    threadPool,
                    subscriptionName
                );
                remoteClient.execute(
                    PublicationsStateAction.INSTANCE,
                    new PublicationsStateAction.Request(
                        subscription.publications(),
                        subscription.connectionInfo().settings().get(ConnectionInfo.USERNAME.getKey())
                    ),
                    listener
                );
            },
            listener::onFailure
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
