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

package io.crate.replication.logical.repository;

import io.crate.replication.logical.LogicalReplicationService;
import io.crate.replication.logical.action.GetStoreMetadataAction;
import io.crate.replication.logical.action.PublicationsStateAction;
import io.crate.replication.logical.action.ReleasePublisherResourcesAction;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.IndexCommit;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.ShardGenerations;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotShardFailure;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ConnectTransportException;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Derived from org.opensearch.replication.repository.RemoteClusterRepository
 */
public class LogicalReplicationRepository extends AbstractLifecycleComponent implements Repository {

    private static final Logger LOGGER = LogManager.getLogger(LogicalReplicationRepository.class);

    public static final Setting<ByteSizeValue> RECOVERY_CHUNK_SIZE =
        Setting.byteSizeSetting("replication.logical.indices.recovery.chunk_size",
                                new ByteSizeValue(1, ByteSizeUnit.MB),
                                new ByteSizeValue(1, ByteSizeUnit.KB),
                                new ByteSizeValue(1, ByteSizeUnit.GB),
                                Setting.Property.Dynamic,
                                Setting.Property.NodeScope
        );



    public static final String TYPE = "logical_replication";
    public static final String LATEST = "_latest_";
    public static final String REMOTE_REPOSITORY_PREFIX = "_logical_replication_";
    private static final SnapshotId SNAPSHOT_ID = new SnapshotId(LATEST, LATEST);
    public static final long REMOTE_CLUSTER_REPO_REQ_TIMEOUT_IN_MILLI_SEC = 60000L;

    private final LogicalReplicationService logicalReplicationService;
    private final RepositoryMetadata metadata;
    private final String subscriptionName;
    private final ThreadPool threadPool;
    private final Settings settings;
    private final ClusterService clusterService;

    public LogicalReplicationRepository(Settings settings,
                                        ClusterService clusterService,
                                        LogicalReplicationService logicalReplicationService,
                                        RepositoryMetadata metadata,
                                        ThreadPool threadPool) {
        this.settings = settings;
        this.clusterService = clusterService;
        this.logicalReplicationService = logicalReplicationService;
        this.metadata = metadata;
        assert metadata.name().startsWith(REMOTE_REPOSITORY_PREFIX)
            : "SubscriptionRepository metadata.name() must start with: " + REMOTE_REPOSITORY_PREFIX;
        //noinspection ConstantConditions
        this.subscriptionName = Strings.split(metadata.name(), REMOTE_REPOSITORY_PREFIX)[1];
        this.threadPool = threadPool;
    }

    @Override
    protected void doStart() {

    }

    @Override
    protected void doStop() {

    }

    @Override
    protected void doClose() throws IOException {

    }

    @Override
    public RepositoryMetadata getMetadata() {
        return metadata;
    }

    @Override
    public SnapshotInfo getSnapshotInfo(SnapshotId snapshotId) {
        assert SNAPSHOT_ID.equals(snapshotId) : "SubscriptionRepository only supports " + SNAPSHOT_ID + " as the SnapshotId";
        var stateResponse = getPublicationsState();
        return new SnapshotInfo(snapshotId, stateResponse.concreteIndices(), SnapshotState.SUCCESS, Version.CURRENT);
    }

    @Override
    public Metadata getSnapshotGlobalMetadata(SnapshotId snapshotId) {
        var stateResponse = getPublicationsState();
        var remoteClusterState = getRemoteClusterState(stateResponse.concreteIndices().toArray(new String[0]));
        return remoteClusterState.metadata();
    }

    @Override
    public IndexMetadata getSnapshotIndexMetadata(SnapshotId snapshotId,
                                                  IndexId index) throws IOException {
        assert SNAPSHOT_ID.equals(snapshotId) : "SubscriptionRepository only supports " + SNAPSHOT_ID + " as the SnapshotId";
        var remoteClusterState = getRemoteClusterState(index.getName());
        var indexMetadata = remoteClusterState.metadata().index(index.getName());

        // Add replication specific settings, this setting will trigger a custom engine, see {@link SQLPlugin#getEngineFactory}
        var builder = Settings.builder().put(indexMetadata.getSettings());
        builder.put(LogicalReplicationService.REPLICATION_SUBSCRIBED_INDEX.getKey(), index.getName());

        var indexMdBuilder = IndexMetadata.builder(indexMetadata).settings(builder);
        indexMetadata.getAliases().valuesIt().forEachRemaining(a -> indexMdBuilder.putAlias(a.get()));
        return indexMdBuilder.build();
    }

    @Override
    public void getRepositoryData(ActionListener<RepositoryData> listener) {
        var stateResponse = getPublicationsState();
        var remoteClusterState = getRemoteClusterState(stateResponse.concreteIndices().toArray(new String[0]));
        var remoteMetadata = remoteClusterState.metadata();
        var shardGenerations = ShardGenerations.builder();

        var it = remoteMetadata.getIndices().valuesIt();
        while (it.hasNext()) {
            var indexMetadata = it.next();
            var indexId = new IndexId(indexMetadata.getIndex().getName(), indexMetadata.getIndexUUID());
            for (int i = 0; i < indexMetadata.getNumberOfShards(); i++) {
                shardGenerations.put(indexId, i, "dummy");
            }
        }
        var repositoryData = RepositoryData.EMPTY
            .addSnapshot(SNAPSHOT_ID, SnapshotState.SUCCESS, Version.CURRENT, shardGenerations.build());
        listener.onResponse(repositoryData);
    }

    @Override
    public void finalizeSnapshot(SnapshotId snapshotId,
                                 ShardGenerations shardGenerations,
                                 long startTime,
                                 String failure,
                                 int totalShards,
                                 List<SnapshotShardFailure> shardFailures,
                                 long repositoryStateId,
                                 boolean includeGlobalState,
                                 Metadata clusterMetadata,
                                 boolean writeShardGens,
                                 ActionListener<SnapshotInfo> listener) {
        throw new UnsupportedOperationException("Operation not permitted");
    }

    @Override
    public void deleteSnapshot(SnapshotId snapshotId,
                               long repositoryStateId,
                               boolean writeShardGens, ActionListener<Void> listener) {
        throw new UnsupportedOperationException("Operation not permitted");
    }

    @Override
    public String startVerification() {
        throw new UnsupportedOperationException("Operation not permitted");
    }

    @Override
    public void endVerification(String verificationToken) {
        throw new UnsupportedOperationException("Operation not permitted");
    }

    @Override
    public void verify(String verificationToken, DiscoveryNode localNode) {
        throw new UnsupportedOperationException("Operation not permitted");
    }

    @Override
    public boolean isReadOnly() {
        return true;
    }

    @Override
    public void snapshotShard(Store store,
                              MapperService mapperService,
                              SnapshotId snapshotId,
                              IndexId indexId,
                              IndexCommit snapshotIndexCommit,
                              IndexShardSnapshotStatus snapshotStatus,
                              boolean writeShardGens, ActionListener<String> listener) {
        throw new UnsupportedOperationException("Operation not permitted");
    }

    @Override
    public IndexShardSnapshotStatus getShardSnapshotStatus(SnapshotId snapshotId,
                                                           IndexId indexId,
                                                           ShardId shardId) {
        assert SNAPSHOT_ID.equals(snapshotId) : "SubscriptionRepository only supports " + SNAPSHOT_ID + " as the SnapshotId";
        final String remoteIndex = indexId.getName();
        final IndicesStatsResponse response = getRemoteClusterClient().admin().indices().prepareStats(remoteIndex)
            .clear().setStore(true)
            .execute().actionGet(REMOTE_CLUSTER_REPO_REQ_TIMEOUT_IN_MILLI_SEC);
        for (ShardStats shardStats : response.getIndex(remoteIndex).getShards()) {
            final ShardRouting shardRouting = shardStats.getShardRouting();
            if (shardRouting.shardId().id() == shardId.getId()
                && shardRouting.primary()
                && shardRouting.active()) {
                // we only care about the shard size here for shard allocation, populate the rest with dummy values
                final long totalSize = shardStats.getStats().getStore().getSizeInBytes();
                return IndexShardSnapshotStatus.newDone(0L, 0L, 1, 1, totalSize, totalSize, "");
            }
        }
        throw new ElasticsearchException("Could not get shard stats for primary of index " + remoteIndex + " on publisher cluster");
    }

    @Override
    public void updateState(ClusterState state) {
        throw new UnsupportedOperationException("Operation not permitted");
    }


    @Override
    public void restoreShard(Store store,
                             SnapshotId snapshotId,
                             IndexId indexId,
                             ShardId snapshotShardId,
                             RecoveryState recoveryState,
                             ActionListener<Void> listener) {
        // TODO: implement RETRY logic
        store.incRef();
        restoreShardUsingMultiChunkTransfer(store, indexId, snapshotShardId, recoveryState, listener);
        // We will do decRef and releaseResources ultimately, not while during our retries/restarts of
        // restoreShard .

    }

    private void restoreShardUsingMultiChunkTransfer(Store store,
                                                     IndexId indexId,
                                                     ShardId snapshotShardId,
                                                     RecoveryState recoveryState,
                                                     ActionListener<Void> listener) {
        var subscriberShardId = store.shardId();
        // 1. Get all the files info from the publisher cluster for this shardId
        var publisherClusterState = getRemoteClusterState(true, true, indexId.getName());
        var publisherShardRouting = publisherClusterState.routingTable()
            .shardRoutingTable(
                snapshotShardId.getIndexName(),
                snapshotShardId.getId()
            )
            .primaryShard();
        var publisherShardNode = publisherClusterState.nodes().get(publisherShardRouting.currentNodeId());
        // Get the index UUID of the publisher cluster for the metadata request
        var publisherShardId = new ShardId(
            snapshotShardId.getIndexName(),
            publisherClusterState.metadata().index(indexId.getName()).getIndexUUID(),
            snapshotShardId.getId()
        );
        var restoreUUID = UUIDs.randomBase64UUID();
        var getStoreMetadataRequest = new GetStoreMetadataAction.Request(
            restoreUUID,
            publisherShardNode,
            publisherShardId,
            clusterService.getClusterName().value(),
            subscriberShardId
        );

        var remoteClient = getRemoteClusterClient();

        // Gets the remote store metadata
        var metadataResponse = remoteClient.execute(
            GetStoreMetadataAction.INSTANCE,
            getStoreMetadataRequest
        ).actionGet(REMOTE_CLUSTER_REPO_REQ_TIMEOUT_IN_MILLI_SEC);
        var metadataSnapshot = metadataResponse.metadataSnapshot();

        // 2. Request for individual files from publisher cluster for this shardId
        // make sure the store is not released until we are done.
        var fileMetadata = new ArrayList<>(metadataSnapshot.asMap().values());
        var multiChunkTransfer = new RemoteClusterMultiChunkTransfer(
            LOGGER,
            clusterService.getClusterName().value(),
            store,
            RecoverySettings.INDICES_RECOVERY_MAX_CONCURRENT_FILE_CHUNKS_SETTING.get(settings),
            restoreUUID,
            //metadata,
            publisherShardNode,
            publisherShardId,
            fileMetadata,
            remoteClient,
            threadPool,
            recoveryState,
            RECOVERY_CHUNK_SIZE.get(settings),
            new ActionListener<>() {
                @Override
                public void onResponse(Void unused) {
                    LOGGER.info("Restore successful for {}", store.shardId());
                    store.decRef();
                    releasePublisherResources(restoreUUID, publisherShardNode, publisherShardId, subscriberShardId);
                    listener.onResponse(null);
                }

                @Override
                public void onFailure(Exception e) {
                    LOGGER.error("Restore of " + store.shardId() + " failed due to ", e);
                    if (e instanceof ConnectTransportException) {
                        // TODO: retry
                        LOGGER.info("TODO: Retry restore shard for ${store.shardId()}");
                    } else {
                        LOGGER.error("Not retrying restore shard for {}", store.shardId());
                        store.decRef();
                        releasePublisherResources(restoreUUID, publisherShardNode, publisherShardId, subscriberShardId);
                        listener.onFailure(e);
                    }

                }
            }
        );
        if (fileMetadata.isEmpty()) {
            LOGGER.info("Initializing with empty store for shard: {}", snapshotShardId.getId());
            try {
                store.createEmpty(store.indexSettings().getIndexVersionCreated().luceneVersion);
                listener.onResponse(null);
            } catch (IOException e) {
                listener.onFailure(new UncheckedIOException(e));
            } finally {
                store.decRef();
                releasePublisherResources(restoreUUID, publisherShardNode, publisherShardId, subscriberShardId);
            }
        } else {
            multiChunkTransfer.start();
        }
    }

    private Client getRemoteClusterClient() {
        return logicalReplicationService.getRemoteClusterClient(threadPool, subscriptionName);
    }

    private ClusterState getRemoteClusterState(String... remoteIndices) {
        return getRemoteClusterState(false, false, remoteIndices);
    }

    private ClusterState getRemoteClusterState(boolean includeNodes, boolean includeRouting, String... remoteIndices) {
        var clusterStateRequest = getRemoteClusterClient().admin().cluster().prepareState()
            .clear()
            .setIndices(remoteIndices)
            .setMetadata(true)
            .setNodes(includeNodes)
            .setRoutingTable(includeRouting)
            .setIndicesOptions(IndicesOptions.strictSingleIndexNoExpandForbidClosed())
            .request();

        var remoteState = getRemoteClusterClient().admin().cluster().state(clusterStateRequest)
            .actionGet(REMOTE_CLUSTER_REPO_REQ_TIMEOUT_IN_MILLI_SEC).getState();
        LOGGER.trace("Successfully fetched the cluster state from remote repository {}", remoteState);
        return remoteState;
    }

    private PublicationsStateAction.Response getPublicationsState() {
        return getRemoteClusterClient().execute(
            PublicationsStateAction.INSTANCE,
            new PublicationsStateAction.Request(logicalReplicationService.subscriptions().get(subscriptionName).publications())
        ).actionGet(REMOTE_CLUSTER_REPO_REQ_TIMEOUT_IN_MILLI_SEC);
    }

    private void releasePublisherResources(String restoreUUID,
                                           DiscoveryNode publisherShardNode,
                                           ShardId publisherShardId,
                                           ShardId subcriberShardId) {
        var releaseResourcesReq = new ReleasePublisherResourcesAction.Request(
            restoreUUID,
            publisherShardNode,
            publisherShardId,
            clusterService.getClusterName().value(),
            subcriberShardId
        );
        try {
            var response = getRemoteClusterClient().execute(
                ReleasePublisherResourcesAction.INSTANCE,
                releaseResourcesReq
            ).actionGet(REMOTE_CLUSTER_REPO_REQ_TIMEOUT_IN_MILLI_SEC);
            if (response.isAcknowledged()) {
                LOGGER.info("Successfully released resources at the publisher cluster for {} at {}",
                            publisherShardId,
                            publisherShardNode
                );
            }
        } catch (Exception e) {
            LOGGER.error("Releasing publisher resource failed due to ", e);
        }

    }
}
