/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.execution.ddl.tables;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.NotifyOnceListener;
import org.elasticsearch.action.admin.indices.close.TransportVerifyShardBeforeCloseAction;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.ActiveShardsObserver;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.block.ClusterBlock;
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
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.collect.ImmutableOpenIntMap;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.snapshots.RestoreService;
import org.elasticsearch.snapshots.SnapshotInProgressException;
import org.elasticsearch.snapshots.SnapshotsService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import com.carrotsearch.hppc.cursors.IntObjectCursor;

import io.crate.metadata.PartitionName;
import io.crate.metadata.RelationName;
import io.crate.metadata.cluster.DDLClusterStateHelpers;
import io.crate.metadata.cluster.DDLClusterStateService;

public final class TransportCloseTable extends TransportMasterNodeAction<CloseTableRequest, AcknowledgedResponse> {

    private static final Logger LOGGER = LogManager.getLogger(TransportCloseTable.class);
    private static final String ACTION_NAME = "internal:crate:sql/table_or_partition/close";
    private static final IndicesOptions STRICT_INDICES_OPTIONS = IndicesOptions.fromOptions(false, false, false, false);
    public static final int INDEX_CLOSED_BLOCK_ID = 4;

    private final TransportVerifyShardBeforeCloseAction verifyShardBeforeClose;
    private final AllocationService allocationService;
    private final DDLClusterStateService ddlClusterStateService;
    private final ActiveShardsObserver activeShardsObserver;

    @Inject
    public TransportCloseTable(TransportService transportService,
                               ClusterService clusterService,
                               ThreadPool threadPool,
                               AllocationService allocationService,
                               DDLClusterStateService ddlClusterStateService,
                               TransportVerifyShardBeforeCloseAction verifyShardBeforeClose) {
        super(
            ACTION_NAME,
            transportService,
            clusterService,
            threadPool,
            CloseTableRequest::new
        );
        this.allocationService = allocationService;
        this.ddlClusterStateService = ddlClusterStateService;
        this.verifyShardBeforeClose = verifyShardBeforeClose;
        this.activeShardsObserver = new ActiveShardsObserver(clusterService);
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
                                          Map<Index, ClusterBlock> blockedIndices,
                                          Map<Index, AcknowledgedResponse> results) {
        // Remove the index routing table of closed indices if the cluster is in a mixed version
        // that does not support the replication of closed indices
        final boolean removeRoutingTable = currentState.nodes().getMinNodeVersion().before(Version.V_4_3_0);

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
            final boolean acknowledged = result.getValue().isAcknowledged();
            try {
                if (acknowledged == false) {
                    LOGGER.debug("verification of shards before closing {} failed", index);
                    continue;
                }
                final IndexMetadata indexMetadata = metadata.getSafe(index);
                if (indexMetadata.getState() == IndexMetadata.State.CLOSE) {
                    LOGGER.debug("verification of shards before closing {} succeeded but index is already closed", index);
                    assert currentState.blocks().hasIndexBlock(index.getName(), IndexMetadata.INDEX_CLOSED_BLOCK);
                    continue;
                }
                final ClusterBlock closingBlock = blockedIndices.get(index);
                if (currentState.blocks().hasIndexBlock(index.getName(), closingBlock) == false) {
                    LOGGER.debug("verification of shards before closing {} succeeded but block has been removed in the meantime", index);
                    continue;
                }

                Set<Index> restoringIndices = RestoreService.restoringIndices(updatedState, Set.of(index));
                if (restoringIndices.isEmpty() == false) {
                    result.setValue(new AcknowledgedResponse(false));
                    LOGGER.debug("verification of shards before closing {} succeeded but index is being restored in the meantime", index);
                    continue;
                }
                Set<Index> snapshottingIndices = SnapshotsService.snapshottingIndices(updatedState, Set.of(index));
                if (snapshottingIndices.isEmpty() == false) {
                    result.setValue(new AcknowledgedResponse(false));
                    LOGGER.debug("verification of shards before closing {} succeeded but index is being snapshot in the meantime", index);
                    continue;
                }

                blocks.removeIndexBlockWithId(index.getName(), INDEX_CLOSED_BLOCK_ID);
                blocks.addIndexBlock(index.getName(), IndexMetadata.INDEX_CLOSED_BLOCK);
                final IndexMetadata.Builder updatedMetadata = IndexMetadata.builder(indexMetadata).state(IndexMetadata.State.CLOSE);
                if (removeRoutingTable) {
                    metadata.put(updatedMetadata);
                    routingTable.remove(index.getName());
                } else {
                    metadata.put(updatedMetadata
                        .settingsVersion(indexMetadata.getSettingsVersion() + 1)
                        .settings(Settings.builder()
                            .put(indexMetadata.getSettings())
                            .put(IndexMetadata.VERIFIED_BEFORE_CLOSE_SETTING.getKey(), true)));
                    routingTable.addAsFromOpenToClose(metadata.getSafe(index));
                }

                LOGGER.debug("closing index {} succeeded", index);
                closedIndices.add(index.getName());
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
            IndexScopedSettings.DEFAULT_SCOPED_SETTINGS // Not used if new settings are empty
        );
    }

    @Override
    protected ClusterBlockException checkBlock(CloseTableRequest request, ClusterState state) {
        String partition = request.partition();
        if (partition == null && isEmptyPartitionedTable(request.table(), state)) {
            return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
        }
        String indexName = partition == null ? request.table().indexNameOrAlias() : partition;
        return state.blocks().indicesBlockedException(
            ClusterBlockLevel.METADATA_WRITE,
            IndexNameExpressionResolver.concreteIndexNames(state.metadata(), STRICT_INDICES_OPTIONS, indexName)
        );
    }

    public static boolean isEmptyPartitionedTable(RelationName relationName,
                                                  ClusterState clusterState) {
        var concreteIndices = IndexNameExpressionResolver.concreteIndexNames(
            clusterState.metadata(),
            IndicesOptions.lenientExpandOpen(),
            relationName.indexNameOrAlias()
        );
        if (concreteIndices.length > 0) {
            return false;
        }

        var templateName = PartitionName.templateName(relationName.schema(), relationName.name());
        var templateMetadata = clusterState.metadata().templates().get(templateName);
        return templateMetadata != null;
    }


    /**
     * Step 1 - Start closing indices by adding a write block
     *
     * This step builds the list of indices to close (the ones explicitly requested that are not in CLOSE state) and adds a unique cluster
     * block (or reuses an existing one) to every index to close in the cluster state. After the cluster state is published, the shards
     * should start to reject writing operations and we can proceed with step 2.
     */
    private static ClusterState addCloseBlocks(ClusterState currentState,
                                               Index[] indices,
                                               Map<Index, ClusterBlock> blockedIndices) {
        Metadata.Builder metadata = Metadata.builder(currentState.metadata());
        ClusterBlocks.Builder blocks = ClusterBlocks.builder().blocks(currentState.blocks());

        Set<Index> indicesToClose = new HashSet<>();
        for (Index index : indices) {
            final IndexMetadata indexMetadata = metadata.getSafe(index);
            if (indexMetadata.getState() != IndexMetadata.State.CLOSE) {
                indicesToClose.add(index);
            } else {
                LOGGER.debug("index {} is already closed, ignoring", index);
                assert currentState.blocks().hasIndexBlock(index.getName(), IndexMetadata.INDEX_CLOSED_BLOCK);

            }
        }
        if (indicesToClose.isEmpty()) {
            return currentState;
        }

        Set<Index> restoringIndices = RestoreService.restoringIndices(currentState, indicesToClose);
        if (restoringIndices.isEmpty() == false) {
            throw new IllegalArgumentException("Cannot close indices that are being restored: " + restoringIndices);
        }
        Set<Index> snapshottingIndices = SnapshotsService.snapshottingIndices(currentState, indicesToClose);
        if (snapshottingIndices.isEmpty() == false) {
            throw new SnapshotInProgressException("Cannot close indices that are being snapshotted: " + snapshottingIndices +
                ". Try again after snapshot finishes or cancel the currently running snapshot.");
        }

        for (var index : indicesToClose) {
            ClusterBlock indexBlock = null;
            final Set<ClusterBlock> clusterBlocks = currentState.blocks().indices().get(index.getName());
            if (clusterBlocks != null) {
                for (ClusterBlock clusterBlock : clusterBlocks) {
                    if (clusterBlock.id() == INDEX_CLOSED_BLOCK_ID) {
                        // Reuse the existing index closed block
                        indexBlock = clusterBlock;
                        break;
                    }
                }
            }
            if (indexBlock == null) {
                indexBlock = new ClusterBlock(
                    INDEX_CLOSED_BLOCK_ID,
                    UUIDs.randomBase64UUID(),
                    "Table or partition preparing to close. Reopen the table to allow " +
                    "writes again or retry closing the table to fully close it.",
                    false,
                    false,
                    false,
                    RestStatus.FORBIDDEN,
                    EnumSet.of(ClusterBlockLevel.WRITE)
                );
            }
            assert Strings.hasLength(indexBlock.uuid()) : "Closing block should have a UUID";

            blocks.addIndexBlock(index.getName(), indexBlock);
            blockedIndices.put(index, indexBlock);
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
        private final Map<Index, ClusterBlock> blockedIndices = new HashMap<>();

        private AddCloseBlocksTask(ActionListener<AcknowledgedResponse> listener,
                                   CloseTableRequest request) {
            this.listener = listener;
            this.request = request;
        }

        @Override
        public ClusterState execute(ClusterState currentState) throws Exception {
            RelationName table = request.table();
            String partition = request.partition();
            Index[] indices = IndexNameExpressionResolver.concreteIndices(currentState.metadata(),
                    IndicesOptions.lenientExpandOpen(), partition == null ? table.indexNameOrAlias() : partition);
            if (indices.length == 0) {
                return currentState;
            }
            return addCloseBlocks(currentState, indices, blockedIndices);
        }

        @Override
        public void onFailure(String source, Exception e) {
            listener.onFailure(e);
        }

        @Override
        public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
            if (blockedIndices.isEmpty()
                && isEmptyPartitionedTable(request.table(), newState)) {
                clusterService.submitStateUpdateTask(
                    "close-indices",
                    new CloseRoutingTableTask(
                        Priority.URGENT,
                        blockedIndices,
                        Map.of(),
                        request,
                        listener
                    )
                );
                return;
            }
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
                        results -> clusterService.submitStateUpdateTask(
                            "close-indices",
                            new CloseRoutingTableTask(
                                Priority.URGENT,
                                blockedIndices,
                                results,
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

        private final Map<Index, AcknowledgedResponse> results;
        private final Map<Index, ClusterBlock> blockedIndices;
        private final ActionListener<AcknowledgedResponse> listener;
        private final CloseTableRequest request;

        boolean acknowledged = true;

        private CloseRoutingTableTask(Priority priority,
                                      Map<Index, ClusterBlock> blockedIndices,
                                      Map<Index, AcknowledgedResponse> results,
                                      CloseTableRequest request,
                                      ActionListener<AcknowledgedResponse> listener) {
            super(priority);
            this.blockedIndices = blockedIndices;
            this.results = results;
            this.listener = listener;
            this.request = request;
        }

        @Override
        public ClusterState execute(final ClusterState currentState) throws Exception {
            AlterTableTarget target = AlterTableTarget.resolve(
                currentState,
                request.table(),
                request.partition()
            );
            final ClusterState updatedState = closeRoutingTable(
                currentState,
                target,
                ddlClusterStateService,
                blockedIndices,
                results
            );
            for (Map.Entry<Index, AcknowledgedResponse> result : results.entrySet()) {
                if (result.getValue().isAcknowledged() == false) {
                    acknowledged = false;
                    break;
                }
                IndexMetadata updatedMetadata = updatedState.metadata().index(result.getKey());
                if (updatedMetadata != null && updatedMetadata.getState() != IndexMetadata.State.CLOSE) {
                    acknowledged = false;
                    break;
                }
            }
            return allocationService.reroute(updatedState, "indices closed");
        }

        @Override
        public void onFailure(final String source, final Exception e) {
            listener.onFailure(e);
        }

        @Override
        public void clusterStateProcessed(final String source, final ClusterState oldState, final ClusterState newState) {
            final String[] indices = results.entrySet().stream()
                .filter(result -> result.getValue().isAcknowledged())
                .map(result -> result.getKey().getName())
                .filter(index -> newState.routingTable().hasIndex(index))
                .toArray(String[]::new);
            if (indices.length > 0) {
                activeShardsObserver.waitForActiveShards(indices, ActiveShardCount.ONE,
                    request.ackTimeout(), shardsAcknowledged -> {
                        if (shardsAcknowledged == false && LOGGER.isDebugEnabled()) {
                            LOGGER.debug("[{}] indices closed, but the operation timed out while waiting " +
                                "for enough shards to be started.", Arrays.toString(indices));
                        }
                        // acknowledged maybe be false but some indices may have been correctly closed, so
                        // we maintain a kind of coherency by overriding the shardsAcknowledged value
                        // (see ShardsAcknowledgedResponse constructor)
                        listener.onResponse(new AcknowledgedResponse(acknowledged));
                    }, listener::onFailure);
            } else {
                listener.onResponse(new AcknowledgedResponse(acknowledged));
            }
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

        private final Map<Index, ClusterBlock> blockedIndices;

        private WaitForClosedBlocksApplied(Map<Index, ClusterBlock> blockedIndices,
                                           ActionListener<Map<Index, AcknowledgedResponse>> listener) {
            super(listener);
            if (blockedIndices == null || blockedIndices.isEmpty()) {
                throw new IllegalArgumentException("Cannot wait for closed blocks to be applied, list of blocked indices is empty or null");
            }
            this.blockedIndices = blockedIndices;
        }

        @Override
        protected void doRun() throws Exception {
            final Map<Index, AcknowledgedResponse> results = new ConcurrentHashMap<>();
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
                                                  final Consumer<AcknowledgedResponse> onResponse) {
            final IndexMetadata indexMetadata = state.metadata().index(index);
            if (indexMetadata == null) {
                logger.debug("index {} has been blocked before closing and is now deleted, ignoring", index);
                onResponse.accept(new AcknowledgedResponse(true));
                return;
            }
            final IndexRoutingTable indexRoutingTable = state.routingTable().index(index);
            if (indexRoutingTable == null || indexMetadata.getState() == IndexMetadata.State.CLOSE) {
                assert state.blocks().hasIndexBlock(index.getName(), IndexMetadata.INDEX_CLOSED_BLOCK);
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
                sendVerifyShardBeforeCloseRequest(shardRoutingTable, closingBlock, new NotifyOnceListener<>() {
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
                                                       final ClusterBlock closingBlock,
                                                       final ActionListener<ReplicationResponse> listener) {
            final ShardId shardId = shardRoutingTable.shardId();
            if (shardRoutingTable.primaryShard().unassigned()) {
                logger.debug("primary shard {} is unassigned, ignoring", shardId);
                final ReplicationResponse response = new ReplicationResponse();
                response.setShardInfo(new ReplicationResponse.ShardInfo(
                    shardRoutingTable.size(),
                    shardRoutingTable.size()));
                listener.onResponse(response);
                return;
            }

            var shardRequest = new TransportVerifyShardBeforeCloseAction.ShardRequest(
                shardId,
                true,
                closingBlock
            );
            verifyShardBeforeClose.execute(shardRequest)
                .thenCompose(response -> verifyShardBeforeClose.execute(
                    new TransportVerifyShardBeforeCloseAction.ShardRequest(shardId, false, closingBlock)))
                .whenComplete(listener);
        }
    }
}
