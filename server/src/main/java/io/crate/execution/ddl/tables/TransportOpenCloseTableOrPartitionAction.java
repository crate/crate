/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.execution.ddl.tables;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import javax.annotation.Nullable;

import com.carrotsearch.hppc.cursors.IntObjectCursor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.NotifyOnceListener;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.cluster.AckedClusterStateTaskListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskConfig;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.MetadataIndexUpgradeService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.collect.ImmutableOpenIntMap;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.snapshots.RestoreService;
import org.elasticsearch.snapshots.SnapshotsService;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import io.crate.common.unit.TimeValue;
import io.crate.execution.ddl.tables.close.IndexResult;
import io.crate.execution.ddl.tables.close.ShardResult;
import io.crate.metadata.RelationName;
import io.crate.metadata.cluster.CloseTableClusterStateTaskExecutor;
import io.crate.metadata.cluster.DDLClusterStateService;
import io.crate.metadata.cluster.OpenTableClusterStateTaskExecutor;

@Singleton
public class TransportOpenCloseTableOrPartitionAction extends TransportMasterNodeAction<OpenCloseTableOrPartitionRequest, AcknowledgedResponse> {

    private static final Logger LOGGER = LogManager.getLogger(TransportOpenCloseTableOrPartitionAction.class);
    private static final IndicesOptions STRICT_INDICES_OPTIONS = IndicesOptions.fromOptions(false, false, false, false);
    private static final String ACTION_NAME = "internal:crate:sql/table_or_partition/open_close";

    private final OpenTableClusterStateTaskExecutor openExecutor;
    private final CloseTableClusterStateTaskExecutor closeExecutor;

    @Inject
    public TransportOpenCloseTableOrPartitionAction(TransportService transportService,
                                                    ClusterService clusterService,
                                                    ThreadPool threadPool,
                                                    IndexNameExpressionResolver indexNameExpressionResolver,
                                                    AllocationService allocationService,
                                                    DDLClusterStateService ddlClusterStateService,
                                                    MetadataIndexUpgradeService metadataIndexUpgradeService,
                                                    IndicesService indexServices) {
        super(ACTION_NAME,
            transportService,
            clusterService,
            threadPool,
            OpenCloseTableOrPartitionRequest::new,
            indexNameExpressionResolver
        );
        openExecutor = new OpenTableClusterStateTaskExecutor(
            indexNameExpressionResolver,
            allocationService,
            ddlClusterStateService,
            metadataIndexUpgradeService,
            indexServices
        );
        closeExecutor = new CloseTableClusterStateTaskExecutor(
            indexNameExpressionResolver,
            allocationService,
            ddlClusterStateService
        );
    }

    @Override
    protected ClusterBlockException checkBlock(OpenCloseTableOrPartitionRequest request, ClusterState state) {
        return state.blocks().indicesBlockedException(
            ClusterBlockLevel.METADATA_WRITE,
            indexNameExpressionResolver.concreteIndexNames(state, STRICT_INDICES_OPTIONS, request.tableIdent().indexNameOrAlias())
        );
    }

    @Override
    protected String executor() {
        // no need to use a thread pool, we go async right away
        return ThreadPool.Names.SAME;
    }

    @Override
    protected AcknowledgedResponse read(StreamInput in) throws IOException {
        return new AcknowledgedResponse(in);
    }

    @Override
    protected void masterOperation(OpenCloseTableOrPartitionRequest request,
                                   ClusterState state,
                                   ActionListener<AcknowledgedResponse> listener) throws Exception {
        if (request.isOpenTable()) {
            openTable(request, state, listener);
        } else {
            closeTable(request, state, listener);
        }
    }

    private void closeTable(OpenCloseTableOrPartitionRequest request,
                            ClusterState state,
                            ActionListener<AcknowledgedResponse> listener) throws Exception {
        if (state.nodes().getMinNodeVersion().onOrAfter(Version.V_4_3_0)) {
            do3PhaseClose(request, state, listener);
        } else {
            clusterService.submitStateUpdateTask(
                "close-table-or-partition",
                request,
                ClusterStateTaskConfig.build(Priority.HIGH, request.masterNodeTimeout()),
                closeExecutor,
                new AckedClusterStateTaskListener() {
                    @Override
                    public void onFailure(String source, Exception e) {
                        listener.onFailure(e);
                    }

                    @Override
                    public boolean mustAck(DiscoveryNode discoveryNode) {
                        return true;
                    }

                    @Override
                    public void onAllNodesAcked(@Nullable Exception e) {
                        listener.onResponse(new AcknowledgedResponse(true));
                    }

                    @Override
                    public void onAckTimeout() {
                        listener.onResponse(new AcknowledgedResponse(false));
                    }

                    @Override
                    public TimeValue ackTimeout() {
                        return request.ackTimeout();
                    }
                }
            );
        }
    }

    private void do3PhaseClose(OpenCloseTableOrPartitionRequest request,
                               ClusterState state,
                               ActionListener<AcknowledgedResponse> listener) {
        assert !request.isOpenTable() : "Must close table";
        assert state.nodes().getMinNodeVersion().onOrAfter(Version.V_4_3_0) : "All nodes must support close verification action";

        clusterService.submitStateUpdateTask(
            "add-block-table-to-close",
            new ClusterStateUpdateTask(Priority.HIGH) {

                private final Set<Index> blockedIndices = new HashSet<>();

                @Override
                public ClusterState execute(ClusterState currentState) throws Exception {
                    // TODO: At which point should we change the template?
                    RelationName table = request.tableIdent();
                    String partition = request.partitionIndexName();
                    Index[] indices = indexNameExpressionResolver.concreteIndices(
                        currentState,
                        IndicesOptions.lenientExpandOpen(),
                        partition == null ? table.indexNameOrAlias() : partition
                    );
                    if (indices.length == 0) {
                        return currentState;
                    }
                    return addCloseBlocks(currentState, indices, blockedIndices::add);
                }

                @Override
                public void onFailure(String source, Exception e) {
                    listener.onFailure(e);
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    if (oldState == newState) {
                        assert blockedIndices.isEmpty() : "List of blocked indices is not empty but cluster state wasn't changed";
                        listener.onResponse(new AcknowledgedResponse(true));
                        return;
                    }

                    assert blockedIndices.isEmpty() == false : "List of blocked indices is empty but cluster state was changed";
                    threadPool.executor(ThreadPool.Names.MANAGEMENT)
                        .execute(new WaitForClosedBlocksApplied(
                            blockedIndices,
                            ActionListener.wrap(
                                closedBlocksResults -> clusterService.submitStateUpdateTask(
                                    "close-indices",
                                    new ClusterStateUpdateTask(Priority.URGENT) {

                                        @Override
                                        public ClusterState execute(final ClusterState currentState) throws Exception {
                                            final ClusterState updatedState = closeRoutingTable(currentState, closedBlocksResults);
                                            return allocationService.reroute(updatedState, "indices closed");
                                        }

                                        @Override
                                        public void onFailure(final String source, final Exception e) {
                                            listener.onFailure(e);
                                        }

                                        @Override
                                        public void clusterStateProcessed(final String source,
                                                                          final ClusterState oldState, final ClusterState newState) {
                                            boolean acknowledged = closedBlocksResults.values().stream()
                                                .allMatch(AcknowledgedResponse::isAcknowledged);
                                            listener.onResponse(new AcknowledgedResponse(acknowledged));
                                        }
                                    }
                                ),
                                listener::onFailure
                            )
                        ));
                }
            }
        );
    }

    private static ClusterState addCloseBlocks(ClusterState currentState, Index[] indices, Consumer<Index> blockedIndex) {
        Metadata.Builder metadata = Metadata.builder(currentState.metadata());
        ClusterBlocks.Builder blocks = ClusterBlocks.builder().blocks(currentState.blocks());

        Set<IndexMetadata> indicesToClose = Set.of();
        for (Index index : indices) {
            final IndexMetadata indexMetadata = metadata.getSafe(index);
            if (indexMetadata.getState() != IndexMetadata.State.CLOSE) {
                indicesToClose.add(indexMetadata);
            } else {
                LOGGER.debug("index {} is already closed, ignoring", index);
            }
        }
        if (indicesToClose.isEmpty()) {
            return currentState;
        }

        RestoreService.checkIndexClosing(currentState, indicesToClose);
        SnapshotsService.checkIndexClosing(currentState, indicesToClose);

        for (var indexToClose : indicesToClose) {
            final Index index = indexToClose.getIndex();
            blockedIndex.accept(index);
            if (currentState.blocks().hasIndexBlock(index.getName(), IndexMetadata.INDEX_CLOSED_BLOCK) == false) {
                blocks.addIndexBlock(index.getName(), IndexMetadata.INDEX_CLOSED_BLOCK);
            }
        }

        return ClusterState.builder(currentState)
            .blocks(blocks)
            .metadata(metadata)
            .routingTable(currentState.routingTable())
            .build();
    }

    /**
     * Step 2 - Wait for indices to be ready for closing
     * <p>
     * This step iterates over the indices previously blocked and sends a {@link TransportVerifyShardBeforeCloseAction} to each shard. If
     * this action succeed then the shard is considered to be ready for closing. When all shards of a given index are ready for closing,
     * the index is considered ready to be closed.
     */
    class WaitForClosedBlocksApplied extends ActionRunnable<Map<Index, IndexResult>> {

        private final Map<Index, ClusterBlock> blockedIndices;

        private WaitForClosedBlocksApplied(final Map<Index, ClusterBlock> blockedIndices,
                                           final ActionListener<Map<Index, IndexResult>> listener) {
            super(listener);
            if (blockedIndices == null || blockedIndices.isEmpty()) {
                throw new IllegalArgumentException("Cannot wait for closed blocks to be applied, list of blocked indices is empty or null");
            }
            this.blockedIndices = blockedIndices;
        }

        @Override
        protected void doRun() throws Exception {
            final Map<Index, IndexResult> results = ConcurrentCollections.newConcurrentMap();
            final CountDown countDown = new CountDown(blockedIndices.size());
            final ClusterState state = clusterService.state();
            blockedIndices.forEach((index, block) -> {
                waitForShardsReadyForClosing(index, block, state, response -> {
                    results.put(index, response);
                    if (countDown.countDown()) {
                        listener.onResponse(Collections.unmodifiableMap(results));
                    }
                });
            });
        }

        private void waitForShardsReadyForClosing(final Index index,
                                                  final ClusterBlock closingBlock,
                                                  final ClusterState state,
                                                  final Consumer<IndexResult> onResponse) {
            final IndexMetadata indexMetadata = state.metadata().index(index);
            if (indexMetadata == null) {
                logger.debug("index {} has been blocked before closing and is now deleted, ignoring", index);
                onResponse.accept(new IndexResult(index));
                return;
            }
            final IndexRoutingTable indexRoutingTable = state.routingTable().index(index);
            if (indexRoutingTable == null || indexMetadata.getState() == IndexMetadata.State.CLOSE) {
                assert state.blocks().hasIndexBlock(index.getName(), IndexMetadata.INDEX_CLOSED_BLOCK);
                LOGGER.debug("index {} has been blocked before closing and is already closed, ignoring", index);
                onResponse.accept(new IndexResult(index));
                return;
            }

            final ImmutableOpenIntMap<IndexShardRoutingTable> shards = indexRoutingTable.getShards();
            final AtomicArray<ShardResult> results = new AtomicArray<>(shards.size());
            final CountDown countDown = new CountDown(shards.size());

            for (IntObjectCursor<IndexShardRoutingTable> shard : shards) {
                final IndexShardRoutingTable shardRoutingTable = shard.value;
                final int shardId = shardRoutingTable.shardId().id();
                sendVerifyShardBeforeCloseRequest(shardRoutingTable, closingBlock, new NotifyOnceListener<ReplicationResponse>() {
                    @Override
                    public void innerOnResponse(final ReplicationResponse replicationResponse) {
                        ShardResult.Failure[] failures = Arrays.stream(replicationResponse.getShardInfo().getFailures())
                            .map(f -> new ShardResult.Failure(f.index(), f.shardId(), f.getCause(), f.nodeId()))
                            .toArray(ShardResult.Failure[]::new);
                        results.setOnce(shardId, new ShardResult(shardId, failures));
                        processIfFinished();
                    }

                    @Override
                    public void innerOnFailure(final Exception e) {
                        ShardResult.Failure failure = new ShardResult.Failure(index.getName(), shardId, e);
                        results.setOnce(shardId, new ShardResult(shardId, new ShardResult.Failure[]{failure}));
                        processIfFinished();
                    }

                    private void processIfFinished() {
                        if (countDown.countDown()) {
                            onResponse.accept(new IndexResult(index, results.toArray(new ShardResult[results.length()])));
                        }
                    }
                });
            }
        }

        private void sendVerifyShardBeforeCloseRequest(final IndexShardRoutingTable shardRoutingTable,
                                                       final ClusterBlock closingBlock,
                                                       final ActionListener<ReplicationResponse> listener) {
            final ShardId shardId = shardRoutingTable.shardId();
            if (shardRoutingTable.primaryShard().unassigned()) {
                logger.debug("primary shard {} is unassigned, ignoring", shardId);
                final ReplicationResponse response = new ReplicationResponse();
                response.setShardInfo(new ReplicationResponse.ShardInfo(shardRoutingTable.size(), shardRoutingTable.size()));
                listener.onResponse(response);
                return;
            }
            final TransportVerifyShardBeforeCloseAction.ShardRequest shardRequest =
                new TransportVerifyShardBeforeCloseAction.ShardRequest(shardId, closingBlock, true, parentTaskId);
            // TODO:
            // if (request.ackTimeout() != null) {
            //     shardRequest.timeout(request.ackTimeout());
            // }
            client.executeLocally(TransportVerifyShardBeforeCloseAction.TYPE, shardRequest, new ActionListener<>() {
                @Override
                public void onResponse(ReplicationResponse replicationResponse) {
                    final TransportVerifyShardBeforeCloseAction.ShardRequest shardRequest =
                        new TransportVerifyShardBeforeCloseAction.ShardRequest(shardId, closingBlock, false, parentTaskId);
                    if (request.ackTimeout() != null) {
                        shardRequest.timeout(request.ackTimeout());
                    }
                    client.executeLocally(TransportVerifyShardBeforeCloseAction.TYPE, shardRequest, listener);
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }
            });
        }
    }

    private void openTable(OpenCloseTableOrPartitionRequest request,
                           ClusterState state,
                           ActionListener<AcknowledgedResponse> listener) {
        clusterService.submitStateUpdateTask(
            "open-table-or-partition",
            request,
            ClusterStateTaskConfig.build(Priority.HIGH, request.masterNodeTimeout()),
            openExecutor,
            new AckedClusterStateTaskListener() {
                @Override
                public void onFailure(String source, Exception e) {
                    listener.onFailure(e);
                }

                @Override
                public boolean mustAck(DiscoveryNode discoveryNode) {
                    return true;
                }

                @Override
                public void onAllNodesAcked(@Nullable Exception e) {
                    listener.onResponse(new AcknowledgedResponse(true));
                }

                @Override
                public void onAckTimeout() {
                    listener.onResponse(new AcknowledgedResponse(false));
                }

                @Override
                public TimeValue ackTimeout() {
                    return request.ackTimeout();
                }
            }
        );
    }
}
