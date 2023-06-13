/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.snapshots;

import static java.util.Collections.emptyMap;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.jetbrains.annotations.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.index.IndexCommit;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.SnapshotsInProgress.ShardSnapshotStatus;
import org.elasticsearch.cluster.SnapshotsInProgress.ShardState;
import org.elasticsearch.cluster.SnapshotsInProgress.State;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.IndexShardSnapshotFailedException;
import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus;
import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus.Stage;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.ShardGenerations;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequestDeduplicator;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;

import io.crate.common.io.IOUtils;

/**
 * This service runs on data nodes and controls currently running shard snapshots on these nodes. It is responsible for
 * starting and stopping shard level snapshots.
 * See package level documentation of {@link org.elasticsearch.snapshots} for details.
 */
public class SnapshotShardsService extends AbstractLifecycleComponent implements ClusterStateListener, IndexEventListener {
    private static final Logger LOGGER = LogManager.getLogger(SnapshotShardsService.class);

    private final ClusterService clusterService;

    private final IndicesService indicesService;

    private final RepositoriesService repositoriesService;

    private final TransportService transportService;

    private final ThreadPool threadPool;

    private final Map<Snapshot, Map<ShardId, IndexShardSnapshotStatus>> shardSnapshots = new HashMap<>();

    // A map of snapshots to the shardIds that we already reported to the master as failed
    private final TransportRequestDeduplicator<UpdateIndexShardSnapshotStatusRequest> remoteFailedRequestDeduplicator =
        new TransportRequestDeduplicator<>();

    public SnapshotShardsService(Settings settings, ClusterService clusterService, RepositoriesService repositoriesService,
                                 TransportService transportService, IndicesService indicesService) {
        this.indicesService = indicesService;
        this.repositoriesService = repositoriesService;
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.threadPool = transportService.getThreadPool();
        if (DiscoveryNode.isDataNode(settings)) {
            // this is only useful on the nodes that can hold data
            clusterService.addListener(this);
        }
    }

    @Override
    protected void doStart() {
    }

    @Override
    protected void doStop() {
    }

    @Override
    protected void doClose() {
        clusterService.removeListener(this);
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        try {
            SnapshotsInProgress previousSnapshots = event.previousState().custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY);
            SnapshotsInProgress currentSnapshots = event.state().custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY);
            if (previousSnapshots.equals(currentSnapshots) == false) {
                synchronized (shardSnapshots) {
                    cancelRemoved(currentSnapshots);
                    startNewSnapshots(currentSnapshots);
                }
            }

            String previousMasterNodeId = event.previousState().nodes().getMasterNodeId();
            String currentMasterNodeId = event.state().nodes().getMasterNodeId();
            if (currentMasterNodeId != null && currentMasterNodeId.equals(previousMasterNodeId) == false) {
                syncShardStatsOnNewMaster(event);
            }

        } catch (Exception e) {
            assert false : new AssertionError(e);
            LOGGER.warn("Failed to update snapshot state ", e);
        }
    }

    @Override
    public void beforeIndexShardClosed(ShardId shardId, @Nullable IndexShard indexShard, Settings indexSettings) {
        // abort any snapshots occurring on the soon-to-be closed shard
        synchronized (shardSnapshots) {
            for (Map.Entry<Snapshot, Map<ShardId, IndexShardSnapshotStatus>> snapshotShards : shardSnapshots.entrySet()) {
                Map<ShardId, IndexShardSnapshotStatus> shards = snapshotShards.getValue();
                if (shards.containsKey(shardId)) {
                    LOGGER.debug("[{}] shard closing, abort snapshotting for snapshot [{}]",
                                 shardId, snapshotShards.getKey().getSnapshotId());
                    shards.get(shardId).abortIfNotCompleted("shard is closing, aborting");
                }
            }
        }
    }

    /**
     * Returns status of shards that are snapshotted on the node and belong to the given snapshot
     * <p>
     * This method is executed on data node
     * </p>
     *
     * @param snapshot  snapshot
     * @return map of shard id to snapshot status
     */
    public Map<ShardId, IndexShardSnapshotStatus> currentSnapshotShards(Snapshot snapshot) {
        synchronized (shardSnapshots) {
            final Map<ShardId, IndexShardSnapshotStatus> current = shardSnapshots.get(snapshot);
            return current == null ? null : new HashMap<>(current);
        }
    }

    private void cancelRemoved(SnapshotsInProgress snapshotsInProgress) {
        // First, remove snapshots that are no longer there
        Iterator<Map.Entry<Snapshot, Map<ShardId, IndexShardSnapshotStatus>>> it = shardSnapshots.entrySet().iterator();
        while (it.hasNext()) {
            final Map.Entry<Snapshot, Map<ShardId, IndexShardSnapshotStatus>> entry = it.next();
            final Snapshot snapshot = entry.getKey();
            if (snapshotsInProgress.snapshot(snapshot) == null) {
                // abort any running snapshots of shards for the removed entry;
                // this could happen if for some reason the cluster state update for aborting
                // running shards is missed, then the snapshot is removed is a subsequent cluster
                // state update, which is being processed here
                it.remove();
                for (IndexShardSnapshotStatus snapshotStatus : entry.getValue().values()) {
                    snapshotStatus.abortIfNotCompleted("snapshot has been removed in cluster state, aborting");
                }
            }
        }
    }

    private void startNewSnapshots(SnapshotsInProgress snapshotsInProgress) {
        // For now we will be mostly dealing with a single snapshot at a time but might have multiple simultaneously running
        // snapshots in the future
        // Now go through all snapshots and update existing or create missing
        final String localNodeId = clusterService.localNode().getId();
        for (SnapshotsInProgress.Entry entry : snapshotsInProgress.entries()) {
            final State entryState = entry.state();
            if (entryState == State.STARTED) {
                Map<ShardId, IndexShardSnapshotStatus> startedShards = null;
                final Snapshot snapshot = entry.snapshot();
                Map<ShardId, IndexShardSnapshotStatus> snapshotShards = shardSnapshots.getOrDefault(snapshot, emptyMap());
                for (ObjectObjectCursor<ShardId, ShardSnapshotStatus> shard : entry.shards()) {
                    // Add all new shards to start processing on
                    final ShardId shardId = shard.key;
                    final ShardSnapshotStatus shardSnapshotStatus = shard.value;
                    if (shardSnapshotStatus.state() == ShardState.INIT
                        && localNodeId.equals(shardSnapshotStatus.nodeId())
                        && snapshotShards.containsKey(shardId) == false) {
                        LOGGER.trace("[{}] - Adding shard to the queue", shardId);
                        if (startedShards == null) {
                            startedShards = new HashMap<>();
                        }
                        startedShards.put(shardId, IndexShardSnapshotStatus.newInitializing(shardSnapshotStatus.generation()));
                    }
                }
                if (startedShards != null && startedShards.isEmpty() == false) {
                    shardSnapshots.computeIfAbsent(snapshot, s -> new HashMap<>()).putAll(startedShards);
                    startNewShards(entry, startedShards);
                }
            } else if (entryState == State.ABORTED) {
                // Abort all running shards for this snapshot
                final Snapshot snapshot = entry.snapshot();
                Map<ShardId, IndexShardSnapshotStatus> snapshotShards = shardSnapshots.getOrDefault(snapshot, emptyMap());
                for (ObjectObjectCursor<ShardId, ShardSnapshotStatus> shard : entry.shards()) {
                    final IndexShardSnapshotStatus snapshotStatus = snapshotShards.get(shard.key);
                    if (snapshotStatus == null) {
                        // due to CS batching we might have missed the INIT state and straight went into ABORTED
                        // notify master that abort has completed by moving to FAILED
                        if (shard.value.state() == ShardState.ABORTED && localNodeId.equals(shard.value.nodeId())) {
                            notifyFailedSnapshotShard(snapshot, shard.key, shard.value.reason());
                        }
                    } else {
                        snapshotStatus.abortIfNotCompleted("snapshot has been aborted");
                    }
                }
            }
        }
    }

    private void startNewShards(SnapshotsInProgress.Entry entry, Map<ShardId, IndexShardSnapshotStatus> startedShards) {
        threadPool.executor(ThreadPool.Names.SNAPSHOT).execute(() -> {
            final Snapshot snapshot = entry.snapshot();
            final Map<String, IndexId> indicesMap =
                entry.indices().stream().collect(Collectors.toMap(IndexId::getName, Function.identity()));
            for (final Map.Entry<ShardId, IndexShardSnapshotStatus> shardEntry : startedShards.entrySet()) {
                final ShardId shardId = shardEntry.getKey();
                final IndexShardSnapshotStatus snapshotStatus = shardEntry.getValue();
                final IndexId indexId = indicesMap.get(shardId.getIndexName());
                assert indexId != null;
                assert SnapshotsService.useShardGenerations(entry.version()) ||
                        ShardGenerations.fixShardGeneration(snapshotStatus.generation()) == null :
                        "Found non-null, non-numeric shard generation [" + snapshotStatus.generation() +
                                "] for snapshot with old-format compatibility";
                snapshot(shardId, snapshot, indexId, snapshotStatus, entry.version(),
                    new ActionListener<String>() {
                        @Override
                        public void onResponse(String newGeneration) {
                            assert newGeneration != null;
                            assert newGeneration.equals(snapshotStatus.generation());
                            if (LOGGER.isDebugEnabled()) {
                                final IndexShardSnapshotStatus.Copy lastSnapshotStatus = snapshotStatus.asCopy();
                                LOGGER.debug("snapshot [{}] completed to [{}] with [{}] at generation [{}]",
                                    snapshot, snapshot.getRepository(), lastSnapshotStatus, snapshotStatus.generation());
                            }
                            notifySuccessfulSnapshotShard(snapshot, shardId, newGeneration);
                        }

                        @Override
                        public void onFailure(Exception e) {
                            final String failure;
                            if (e instanceof AbortedSnapshotException) {
                                failure = "aborted";
                                LOGGER.debug(() -> new ParameterizedMessage("[{}][{}] aborted shard snapshot", shardId, snapshot), e);
                            } else {
                                failure = summarizeFailure(e);
                                LOGGER.warn(() -> new ParameterizedMessage("[{}][{}] failed to snapshot shard", shardId, snapshot), e);
                            }
                            snapshotStatus.moveToFailed(threadPool.absoluteTimeInMillis(), failure);
                            notifyFailedSnapshotShard(snapshot, shardId, failure);
                        }
                    });
            }
        });
    }

    //package private for testing
    static String summarizeFailure(Throwable t) {
        if (t.getCause() == null) {
            return t.getClass().getSimpleName() + "[" + t.getMessage() + "]";
        } else {
            StringBuilder sb = new StringBuilder();
            while (t != null) {
                sb.append(t.getClass().getSimpleName());
                if (t.getMessage() != null) {
                    sb.append("[");
                    sb.append(t.getMessage());
                    sb.append("]");
                }
                t = t.getCause();
                if (t != null) {
                    sb.append("; nested: ");
                }
            }
            return sb.toString();
        }
    }

    /**
     * Creates shard snapshot
     *
     * @param snapshot       snapshot
     * @param snapshotStatus snapshot status
     */
    private void snapshot(final ShardId shardId, final Snapshot snapshot, final IndexId indexId,
                          final IndexShardSnapshotStatus snapshotStatus, Version version, ActionListener<String> listener) {
        try {
            final IndexShard indexShard = indicesService.indexServiceSafe(shardId.getIndex()).getShardOrNull(shardId.id());
            if (indexShard.routingEntry().primary() == false) {
                throw new IndexShardSnapshotFailedException(shardId, "snapshot should be performed only on primary");
            }
            if (indexShard.routingEntry().relocating()) {
                // do not snapshot when in the process of relocation of primaries so we won't get conflicts
                throw new IndexShardSnapshotFailedException(shardId, "cannot snapshot while relocating");
            }

            final IndexShardState indexShardState = indexShard.state();
            if (indexShardState == IndexShardState.CREATED || indexShardState == IndexShardState.RECOVERING) {
                // shard has just been created, or still recovering
                throw new IndexShardSnapshotFailedException(shardId, "shard didn't fully recover yet");
            }

            final Repository repository = repositoriesService.repository(snapshot.getRepository());
            Engine.IndexCommitRef snapshotRef = null;
            try {
                // we flush first to make sure we get the latest writes snapshotted
                snapshotRef = indexShard.acquireLastIndexCommit(true);
                final IndexCommit snapshotIndexCommit = snapshotRef.getIndexCommit();
                repository.snapshotShard(indexShard.store(), indexShard.mapperService(), snapshot.getSnapshotId(), indexId,
                    snapshotRef.getIndexCommit(), getShardStateId(indexShard, snapshotIndexCommit), snapshotStatus, version,
                    ActionListener.runBefore(listener, snapshotRef::close));
            } catch (Exception e) {
                IOUtils.close(snapshotRef);
                throw e;
            }
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * Generates an identifier from the current state of a shard that can be used to detect whether a shard's contents
     * have changed between two snapshots.
     * A shard is assumed to have unchanged contents if its global- and local checkpoint are equal, its maximum
     * sequence number has not changed and its history- and force-merge-uuid have not changed.
     * The method returns {@code null} if global and local checkpoint are different for a shard since no safe unique
     * shard state id can be used in this case because of the possibility of a primary failover leading to different
     * shard content for the same sequence number on a subsequent snapshot.
     *
     * @param indexShard          Shard
     * @param snapshotIndexCommit IndexCommit for shard
     * @return shard state id or {@code null} if none can be used
     */
    @Nullable
    private static String getShardStateId(IndexShard indexShard, IndexCommit snapshotIndexCommit) throws IOException {
        final Map<String, String> userCommitData = snapshotIndexCommit.getUserData();
        final SequenceNumbers.CommitInfo seqNumInfo = SequenceNumbers.loadSeqNoInfoFromLuceneCommit(userCommitData.entrySet());
        final long maxSeqNo = seqNumInfo.maxSeqNo;
        if (maxSeqNo != seqNumInfo.localCheckpoint || maxSeqNo != indexShard.getLastSyncedGlobalCheckpoint()) {
            return null;
        }
        return userCommitData.get(Engine.HISTORY_UUID_KEY) + "-" +
            userCommitData.getOrDefault(Engine.FORCE_MERGE_UUID_KEY, "na") + "-" + maxSeqNo;
    }

    /**
     * Checks if any shards were processed that the new master doesn't know about
     */
    private void syncShardStatsOnNewMaster(ClusterChangedEvent event) {
        SnapshotsInProgress snapshotsInProgress = event.state().custom(SnapshotsInProgress.TYPE);
        if (snapshotsInProgress == null) {
            return;
        }

        // Clear request deduplicator since we need to send all requests that were potentially not handled by the previous
        // master again
        remoteFailedRequestDeduplicator.clear();
        for (SnapshotsInProgress.Entry snapshot : snapshotsInProgress.entries()) {
            if (snapshot.state() == State.STARTED || snapshot.state() == State.ABORTED) {
                Map<ShardId, IndexShardSnapshotStatus> localShards = currentSnapshotShards(snapshot.snapshot());
                if (localShards != null) {
                    ImmutableOpenMap<ShardId, ShardSnapshotStatus> masterShards = snapshot.shards();
                    for (Map.Entry<ShardId, IndexShardSnapshotStatus> localShard : localShards.entrySet()) {
                        ShardId shardId = localShard.getKey();
                        ShardSnapshotStatus masterShard = masterShards.get(shardId);
                        if (masterShard != null && masterShard.state().completed() == false) {
                            final IndexShardSnapshotStatus.Copy indexShardSnapshotStatus = localShard.getValue().asCopy();
                            final Stage stage = indexShardSnapshotStatus.getStage();
                            // Master knows about the shard and thinks it has not completed
                            if (stage == Stage.DONE) {
                                // but we think the shard is done - we need to make new master know that the shard is done
                                LOGGER.debug("[{}] new master thinks the shard [{}] is not completed but the shard is done locally, " +
                                    "updating status on the master", snapshot.snapshot(), shardId);
                                notifySuccessfulSnapshotShard(snapshot.snapshot(), shardId, localShard.getValue().generation());

                            } else if (stage == Stage.FAILURE) {
                                // but we think the shard failed - we need to make new master know that the shard failed
                                LOGGER.debug("[{}] new master thinks the shard [{}] is not completed but the shard failed locally, " +
                                    "updating status on master", snapshot.snapshot(), shardId);
                                notifyFailedSnapshotShard(snapshot.snapshot(), shardId, indexShardSnapshotStatus.getFailure());
                            }
                        }
                    }
                }
            }
        }
    }

    /** Notify the master node that the given shard has been successfully snapshotted **/
    private void notifySuccessfulSnapshotShard(final Snapshot snapshot, final ShardId shardId, String generation) {
        assert generation != null;
        sendSnapshotShardUpdate(snapshot, shardId,
                                new ShardSnapshotStatus(clusterService.localNode().getId(), ShardState.SUCCESS, generation));
    }

    /** Notify the master node that the given shard failed to be snapshotted **/
    private void notifyFailedSnapshotShard(final Snapshot snapshot, final ShardId shardId, final String failure) {
        sendSnapshotShardUpdate(snapshot, shardId,
                                new ShardSnapshotStatus(clusterService.localNode().getId(), ShardState.FAILED, failure, null));
    }

    /** Updates the shard snapshot status by sending a {@link UpdateIndexShardSnapshotStatusRequest} to the master node */
    private void sendSnapshotShardUpdate(final Snapshot snapshot, final ShardId shardId, final ShardSnapshotStatus status) {
        remoteFailedRequestDeduplicator.executeOnce(
            new UpdateIndexShardSnapshotStatusRequest(snapshot, shardId, status),
            new ActionListener<>() {
                @Override
                public void onResponse(Void aVoid) {
                    LOGGER.trace("[{}] [{}] updated snapshot state", snapshot, status);
                }

                @Override
                public void onFailure(Exception e) {
                    LOGGER.warn(
                        () -> new ParameterizedMessage("[{}] [{}] failed to update snapshot state", snapshot, status), e);
                }
            },
            (req, reqListener) -> transportService.sendRequest(transportService.getLocalNode(),
                    SnapshotsService.UPDATE_SNAPSHOT_STATUS_ACTION_NAME, req,
                new TransportResponseHandler<UpdateIndexShardSnapshotStatusResponse>() {
                    @Override
                    public UpdateIndexShardSnapshotStatusResponse read(StreamInput in) throws IOException {
                        return new UpdateIndexShardSnapshotStatusResponse();
                    }

                    @Override
                    public void handleResponse(UpdateIndexShardSnapshotStatusResponse response) {
                        reqListener.onResponse(null);
                    }

                    @Override
                    public void handleException(TransportException exp) {
                        reqListener.onFailure(exp);
                    }

                    @Override
                    public String executor() {
                        return ThreadPool.Names.SAME;
                    }
                })
        );
    }
}
