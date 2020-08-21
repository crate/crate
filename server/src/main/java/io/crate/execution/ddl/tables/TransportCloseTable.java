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
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import com.carrotsearch.hppc.cursors.IntObjectCursor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.NotifyOnceListener;
import org.elasticsearch.action.admin.indices.close.TransportVerifyShardBeforeCloseAction;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.collect.ImmutableOpenIntMap;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.snapshots.RestoreService;
import org.elasticsearch.snapshots.SnapshotsService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import io.crate.metadata.PartitionName;
import io.crate.metadata.RelationName;
import io.crate.metadata.cluster.DDLClusterStateHelpers;
import io.crate.metadata.cluster.DDLClusterStateService;

public final class TransportCloseTable extends TransportMasterNodeAction<CloseTableRequest, AcknowledgedResponse> {

    private static final Logger LOGGER = LogManager.getLogger(TransportCloseTable.class);
    private static final String ACTION_NAME = "internal:crate:sql/table_or_partition/close";
    private static final IndicesOptions STRICT_INDICES_OPTIONS = IndicesOptions.fromOptions(false, false, false, false);
    private final TransportVerifyShardBeforeCloseAction verifyShardBeforeClose;
    private final AllocationService allocationService;
    private final DDLClusterStateService ddlClusterStateService;

    @Inject
    public TransportCloseTable(String actionName,
                               TransportService transportService,
                               ClusterService clusterService,
                               ThreadPool threadPool,
                               IndexNameExpressionResolver indexNameExpressionResolver,
                               AllocationService allocationService,
                               DDLClusterStateService ddlClusterStateService,
                               TransportVerifyShardBeforeCloseAction verifyShardBeforeClose) {
        super(
            ACTION_NAME,
            transportService,
            clusterService,
            threadPool,
            CloseTableRequest::new,
            indexNameExpressionResolver
        );
        this.allocationService = allocationService;
        this.ddlClusterStateService = ddlClusterStateService;
        this.verifyShardBeforeClose = verifyShardBeforeClose;
    }

    @Override
    protected String executor() {
        // goes async right away
        return ThreadPool.Names.SAME;
    }

    @Override
    protected AcknowledgedResponse read(StreamInput in) throws IOException {
        return new AcknowledgedResponse(in);
    }

    @Override
    protected void masterOperation(CloseTableRequest request,
                                   ClusterState state,
                                   ActionListener<AcknowledgedResponse> listener) throws Exception {
        assert state.nodes().getMinNodeVersion().onOrAfter(Version.V_4_3_0)
            : "All nodes must be on 4.3 to use the new dedicated close action";

        clusterService.submitStateUpdateTask("add-block-close-table", new AddCloseBlocksTask(listener, request));
    }

    /**
     * Step 3 - Move index states from OPEN to CLOSE in cluster state for indices that are ready for closing.
     * @param target
     */
    static ClusterState closeRoutingTable(ClusterState currentState,
                                          AlterTableTarget target,
                                          DDLClusterStateService ddlClusterStateService,
                                          Map<Index, AcknowledgedResponse> results) {
        IndexTemplateMetadata templateMetadata = target.templateMetadata();
        ClusterState updatedState;
        if (templateMetadata == null) {
            updatedState = currentState;
        } else {
            Metadata.Builder metadata = Metadata.builder(currentState.metadata());
            metadata.put(closePartitionTemplate(templateMetadata));
            updatedState = ClusterState.builder(currentState).metadata(metadata).build();
        }

        String partition = target.partition();
        if (partition != null) {
            PartitionName partitionName = PartitionName.fromIndexOrTemplate(partition);
            updatedState = ddlClusterStateService.onCloseTablePartition(updatedState, partitionName);
        } else {
            updatedState = ddlClusterStateService.onCloseTable(updatedState, target.table());
        }

        final Metadata.Builder metadata = Metadata.builder(updatedState.metadata());
        final ClusterBlocks.Builder blocks = ClusterBlocks.builder().blocks(updatedState.blocks());
        final RoutingTable.Builder routingTable = RoutingTable.builder(updatedState.routingTable());
        final Set<String> closedIndices = new HashSet<>();
        for (Map.Entry<Index, AcknowledgedResponse> result : results.entrySet()) {
            final Index index = result.getKey();
            try {
                final IndexMetadata indexMetadata = metadata.getSafe(index);
                if (indexMetadata.getState() != IndexMetadata.State.CLOSE) {
                    if (result.getValue().isAcknowledged()) {
                        LOGGER.debug("closing index {} succeed, removing index routing table", index);
                        metadata.put(IndexMetadata.builder(indexMetadata).state(IndexMetadata.State.CLOSE));
                        routingTable.remove(index.getName());
                        closedIndices.add(index.getName());
                    } else {
                        LOGGER.debug("closing index {} failed, removing index block because: {}", index, result.getValue());
                        blocks.removeIndexBlock(index.getName(), IndexMetadata.INDEX_CLOSED_BLOCK);
                    }
                } else {
                    LOGGER.debug("index {} has been closed since it was blocked before closing, ignoring", index);
                }
            } catch (IndexNotFoundException e) {
                LOGGER.debug("index {} has been deleted since it was blocked before closing, ignoring", index);
            }
        }
        LOGGER.info("completed closing of indices {}", closedIndices);
        return ClusterState.builder(currentState)
            .blocks(blocks)
            .metadata(metadata)
            .routingTable(routingTable.build())
            .build();
    }

    private static IndexTemplateMetadata closePartitionTemplate(IndexTemplateMetadata templateMetadata) {
        Map<String, Object> metaMap = Collections.singletonMap("_meta", Collections.singletonMap("closed", true));
        return DDLClusterStateHelpers.updateTemplate(
            templateMetadata,
            metaMap,
            Collections.emptyMap(),
            Settings.EMPTY,
            (n, s) -> {},
            s -> true
        );
    }

    @Override
    protected ClusterBlockException checkBlock(CloseTableRequest request, ClusterState state) {
        String partition = request.partition();
        String indexName = partition == null ? request.table().indexNameOrAlias() : partition;
        return state.blocks().indicesBlockedException(
            ClusterBlockLevel.METADATA_WRITE,
            indexNameExpressionResolver.concreteIndexNames(state, STRICT_INDICES_OPTIONS, indexName)
        );
    }

    private static ClusterState addCloseBlocks(ClusterState currentState, Index[] indices, Consumer<Index> blockedIndex) {
        Metadata.Builder metadata = Metadata.builder(currentState.metadata());
        ClusterBlocks.Builder blocks = ClusterBlocks.builder().blocks(currentState.blocks());

        Set<IndexMetadata> indicesToClose = new HashSet<>();
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

    private final class AddCloseBlocksTask extends ClusterStateUpdateTask {

        private final ActionListener<AcknowledgedResponse> listener;
        private final CloseTableRequest request;
        private final Set<Index> blockedIndices = new HashSet<>();

        private AddCloseBlocksTask(ActionListener<AcknowledgedResponse> listener,
                                   CloseTableRequest request) {
            this.listener = listener;
            this.request = request;
        }

        @Override
        public ClusterState execute(ClusterState currentState) throws Exception {
            RelationName table = request.table();
            String partition = request.partition();
            Index[] indices = indexNameExpressionResolver.concreteIndices(currentState,
                    IndicesOptions.lenientExpandOpen(), partition == null ? table.indexNameOrAlias() : partition);
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
                assert blockedIndices.isEmpty()
                        : "List of blocked indices is not empty but cluster state wasn't changed";
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
                            new CloseRoutingTableTask(
                                Priority.URGENT,
                                closedBlocksResults,
                                request,
                                listener
                            )
                        ),
                        listener::onFailure)
                    )
                );
        }
    }


    private final class CloseRoutingTableTask extends ClusterStateUpdateTask {

        private final Map<Index, AcknowledgedResponse> closedBlocksResults;
        private final ActionListener<AcknowledgedResponse> listener;
        private final CloseTableRequest request;

        private CloseRoutingTableTask(Priority priority,
                                      Map<Index, AcknowledgedResponse> closedBlocksResults,
                                      CloseTableRequest request,
                                      ActionListener<AcknowledgedResponse> listener) {
            super(priority);
            this.closedBlocksResults = closedBlocksResults;
            this.listener = listener;
            this.request = request;
        }

        @Override
        public ClusterState execute(final ClusterState currentState) throws Exception {
            AlterTableTarget target = AlterTableTarget.resolve(
                indexNameExpressionResolver,
                currentState,
                request.table(),
                request.partition()
            );
            final ClusterState updatedState = closeRoutingTable(currentState, target, ddlClusterStateService, closedBlocksResults);
            return allocationService.reroute(updatedState, "indices closed");
        }

        @Override
        public void onFailure(final String source, final Exception e) {
            listener.onFailure(e);
        }

        @Override
        public void clusterStateProcessed(final String source, final ClusterState oldState, final ClusterState newState) {
            boolean acknowledged = closedBlocksResults.values().stream().allMatch(AcknowledgedResponse::isAcknowledged);
            listener.onResponse(new AcknowledgedResponse(acknowledged));
        }
    }


    /**
     * Step 2 - Wait for indices to be ready for closing
     * <p>
     * This step iterates over the indices previously blocked and sends a {@link TransportVerifyShardBeforeCloseAction} to each shard. If
     * this action succeed then the shard is considered to be ready for closing. When all shards of a given index are ready for closing,
     * the index is considered ready to be closed.
     */
    class WaitForClosedBlocksApplied extends ActionRunnable<Map<Index, AcknowledgedResponse>> {

        private final Set<Index> blockedIndices;

        private WaitForClosedBlocksApplied(Set<Index> blockedIndices,
                                           ActionListener<Map<Index, AcknowledgedResponse>> listener) {
            super(listener);
            if (blockedIndices == null || blockedIndices.isEmpty()) {
                throw new IllegalArgumentException("Cannot wait for closed blocks to be applied, list of blocked indices is empty or null");
            }
            this.blockedIndices = blockedIndices;
        }

        @Override
        protected void doRun() throws Exception {
            final Map<Index, AcknowledgedResponse> results = ConcurrentCollections.newConcurrentMap();
            final CountDown countDown = new CountDown(blockedIndices.size());
            final ClusterState state = clusterService.state();
            for (Index blockedIndex : blockedIndices) {
                waitForShardsReadyForClosing(blockedIndex, state, response -> {
                    results.put(blockedIndex, response);
                    if (countDown.countDown()) {
                        listener.onResponse(Collections.unmodifiableMap(results));
                    }
                });
            }
        }

        private void waitForShardsReadyForClosing(final Index index,
                                                  final ClusterState state,
                                                  final Consumer<AcknowledgedResponse> onResponse) {
            final IndexMetadata indexMetadata = state.metadata().index(index);
            if (indexMetadata == null) {
                logger.debug("index {} has been blocked before closing and is now deleted, ignoring", index);
                onResponse.accept(new AcknowledgedResponse(true));
                return;
            }
            final IndexRoutingTable indexRoutingTable = state.routingTable().index(index);
            if (indexRoutingTable == null || indexMetadata.getState() == IndexMetadata.State.CLOSE) {
                logger.debug("index {} has been blocked before closing and is already closed, ignoring", index);
                onResponse.accept(new AcknowledgedResponse(true));
                return;
            }

            final ImmutableOpenIntMap<IndexShardRoutingTable> shards = indexRoutingTable.getShards();
            final AtomicArray<AcknowledgedResponse> results = new AtomicArray<>(shards.size());
            final CountDown countDown = new CountDown(shards.size());

            for (IntObjectCursor<IndexShardRoutingTable> shard : shards) {
                final IndexShardRoutingTable shardRoutingTable = shard.value;
                final ShardId shardId = shardRoutingTable.shardId();
                sendVerifyShardBeforeCloseRequest(shardRoutingTable, new NotifyOnceListener<ReplicationResponse>() {
                    @Override
                    public void innerOnResponse(final ReplicationResponse replicationResponse) {
                        ReplicationResponse.ShardInfo shardInfo = replicationResponse.getShardInfo();
                        results.setOnce(shardId.id(), new AcknowledgedResponse(shardInfo.getFailed() == 0));
                        processIfFinished();
                    }

                    @Override
                    public void innerOnFailure(final Exception e) {
                        results.setOnce(shardId.id(), new AcknowledgedResponse(false));
                        processIfFinished();
                    }

                    private void processIfFinished() {
                        if (countDown.countDown()) {
                            final boolean acknowledged = results.asList().stream().allMatch(AcknowledgedResponse::isAcknowledged);
                            onResponse.accept(new AcknowledgedResponse(acknowledged));
                        }
                    }
                });
            }
        }

        private void sendVerifyShardBeforeCloseRequest(final IndexShardRoutingTable shardRoutingTable,
                                                       final ActionListener<ReplicationResponse> listener) {
            final ShardId shardId = shardRoutingTable.shardId();
            if (shardRoutingTable.primaryShard().unassigned()) {
                logger.debug("primary shard {} is unassigned, ignoring", shardId);
                final ReplicationResponse response = new ReplicationResponse();
                response.setShardInfo(new ReplicationResponse.ShardInfo(shardRoutingTable.size(), shardRoutingTable.size()));
                listener.onResponse(response);
                return;
            }
            TransportVerifyShardBeforeCloseAction.ShardRequest shardRequest =
                new TransportVerifyShardBeforeCloseAction.ShardRequest(shardId);

            // TODO propagate a task id from the parent CloseIndexRequest to the ShardCloseRequests
            verifyShardBeforeClose.execute(shardRequest, listener);
        }
    }
}
