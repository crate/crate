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

import io.crate.action.FutureActionListener;
import io.crate.action.sql.ResultReceiver;
import io.crate.action.sql.SQLOperations;
import io.crate.action.sql.Session;
import io.crate.analyze.AnalyzedAlterTableRename;
import io.crate.analyze.BoundAddColumn;
import io.crate.analyze.BoundAlterTable;
import io.crate.common.annotations.VisibleForTesting;
import io.crate.data.Row;
import io.crate.execution.ddl.index.SwapAndDropIndexRequest;
import io.crate.execution.ddl.index.TransportSwapAndDropIndexNameAction;
import io.crate.execution.support.ChainableAction;
import io.crate.execution.support.ChainableActions;
import io.crate.metadata.PartitionName;
import io.crate.metadata.RelationName;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.TableInfo;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.TransportDeleteIndexAction;
import org.elasticsearch.action.admin.indices.shrink.ResizeRequest;
import org.elasticsearch.action.admin.indices.shrink.ResizeResponse;
import org.elasticsearch.action.admin.indices.shrink.ResizeType;
import org.elasticsearch.action.admin.indices.shrink.TransportResizeAction;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.Settings;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_BLOCKS_WRITE_SETTING;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;

@Singleton
public class AlterTableOperation {

    public static final String RESIZE_PREFIX = ".resized.";

    private final ClusterService clusterService;
    private final TransportAlterTableAction transportAlterTableAction;
    private final TransportRenameTableAction transportRenameTableAction;
    private final TransportOpenCloseTableOrPartitionAction transportOpenCloseTableOrPartitionAction;
    private final TransportResizeAction transportResizeAction;
    private final TransportDeleteIndexAction transportDeleteIndexAction;
    private final TransportSwapAndDropIndexNameAction transportSwapAndDropIndexNameAction;
    private final TransportCloseTable transportCloseTable;
    private final SQLOperations sqlOperations;
    private Session session;


    @Inject
    public AlterTableOperation(ClusterService clusterService,
                               TransportRenameTableAction transportRenameTableAction,
                               TransportOpenCloseTableOrPartitionAction transportOpenCloseTableOrPartitionAction,
                               TransportCloseTable transportCloseTable,
                               TransportResizeAction transportResizeAction,
                               TransportDeleteIndexAction transportDeleteIndexAction,
                               TransportSwapAndDropIndexNameAction transportSwapAndDropIndexNameAction,
                               TransportAlterTableAction transportAlterTableAction,
                               SQLOperations sqlOperations) {

        this.clusterService = clusterService;
        this.transportRenameTableAction = transportRenameTableAction;
        this.transportResizeAction = transportResizeAction;
        this.transportDeleteIndexAction = transportDeleteIndexAction;
        this.transportSwapAndDropIndexNameAction = transportSwapAndDropIndexNameAction;
        this.transportOpenCloseTableOrPartitionAction = transportOpenCloseTableOrPartitionAction;
        this.transportCloseTable = transportCloseTable;
        this.transportAlterTableAction = transportAlterTableAction;
        this.sqlOperations = sqlOperations;
    }

    public CompletableFuture<Long> executeAlterTableAddColumn(final BoundAddColumn analysis) {
        FutureActionListener<AcknowledgedResponse, Long> result = new FutureActionListener<>(r -> -1L);
        if (analysis.newPrimaryKeys() || analysis.hasNewGeneratedColumns()) {
            RelationName ident = analysis.table().ident();
            String stmt =
                String.format(Locale.ENGLISH, "SELECT COUNT(*) FROM \"%s\".\"%s\"", ident.schema(), ident.name());

            try {
                session().quickExec(stmt, new ResultSetReceiver(analysis, result), Row.EMPTY);
            } catch (Throwable t) {
                result.completeExceptionally(t);
            }
        } else {
            return addColumnToTable(analysis, result);
        }
        return result;
    }

    private Session session() {
        if (session == null) {
            this.session = sqlOperations.newSystemSession();
        }
        return session;
    }

    public CompletableFuture<Long> executeAlterTableOpenClose(DocTableInfo tableInfo,
                                                              boolean openTable,
                                                              @Nullable PartitionName partitionName) {
        String partitionIndexName = null;
        if (partitionName != null) {
            partitionIndexName = partitionName.asIndexName();
        }
        FutureActionListener<AcknowledgedResponse, Long> listener = new FutureActionListener<>(r -> -1L);
        if (openTable || clusterService.state().getNodes().getMinNodeVersion().before(Version.V_4_3_0)) {
            OpenCloseTableOrPartitionRequest request = new OpenCloseTableOrPartitionRequest(
                tableInfo.ident(), partitionIndexName, openTable);
            transportOpenCloseTableOrPartitionAction.execute(request, listener);
        } else {
            transportCloseTable.execute(
                new CloseTableRequest(tableInfo.ident(), partitionIndexName),
                listener
            );
        }
        return listener;
    }

    public CompletableFuture<Long> executeAlterTable(BoundAlterTable analysis) {
        final Settings settings = analysis.tableParameter().settings();
        final boolean includesNumberOfShardsSetting = settings.hasValue(SETTING_NUMBER_OF_SHARDS);
        final boolean isResizeOperationRequired = includesNumberOfShardsSetting &&
                                                  (!analysis.isPartitioned() || analysis.partitionName().isPresent());

        if (isResizeOperationRequired) {
            if (settings.size() > 1) {
                throw new IllegalArgumentException("Setting [number_of_shards] cannot be combined with other settings");
            }
            return executeAlterTableChangeNumberOfShards(analysis);
        }
        return executeAlterTableSetOrReset(analysis);
    }

    private CompletableFuture<Long> executeAlterTableSetOrReset(BoundAlterTable analysis) {
        try {
            AlterTableRequest request = new AlterTableRequest(
                analysis.table().ident(),
                analysis.partitionName().map(PartitionName::asIndexName).orElse(null),
                analysis.isPartitioned(),
                analysis.excludePartitions(),
                analysis.tableParameter().settings(),
                analysis.tableParameter().mappings()
            );
            FutureActionListener<AcknowledgedResponse, Long> listener = new FutureActionListener<>(r -> -1L);
            transportAlterTableAction.execute(request, listener);
            return listener;
        } catch (IOException e) {
            return FutureActionListener.failedFuture(e);
        }
    }

    private CompletableFuture<Long> executeAlterTableChangeNumberOfShards(BoundAlterTable analysis) {
        final TableInfo table = analysis.table();
        final boolean isPartitioned = analysis.isPartitioned();
        String sourceIndexName;
        String sourceIndexAlias;
        if (isPartitioned) {
            Optional<PartitionName> partitionName = analysis.partitionName();
            assert partitionName.isPresent() : "Resizing operations for partitioned tables " +
                                               "are only supported at partition level";
            sourceIndexName = partitionName.get().asIndexName();
            sourceIndexAlias = table.ident().indexNameOrAlias();
        } else {
            sourceIndexName = table.ident().indexNameOrAlias();
            sourceIndexAlias = null;
        }

        final ClusterState currentState = clusterService.state();
        final IndexMetadata sourceIndexMetadata = currentState.metadata().index(sourceIndexName);
        final int targetNumberOfShards = getNumberOfShards(analysis.tableParameter().settings());
        validateForResizeRequest(sourceIndexMetadata, targetNumberOfShards);

        final List<ChainableAction<Long>> actions = new ArrayList<>();
        final String resizedIndex = RESIZE_PREFIX + sourceIndexName;
        deleteLeftOverFromPreviousOperations(currentState, actions, resizedIndex);

        actions.add(new ChainableAction<>(
            () -> resizeIndex(
                currentState.metadata().index(sourceIndexName),
                sourceIndexAlias,
                resizedIndex,
                targetNumberOfShards
            ),
            () -> CompletableFuture.completedFuture(-1L)
        ));
        actions.add(new ChainableAction<>(
            () -> swapAndDropIndex(resizedIndex, sourceIndexName)
                .exceptionally(error -> {
                    throw new IllegalStateException(
                        "The resize operation to change the number of shards completed partially but run into a failure. " +
                        "Please retry the operation or clean up the internal indices with ALTER CLUSTER GC DANGLING ARTIFACTS. "
                        + error.getMessage(), error
                    );
                }),
            () -> CompletableFuture.completedFuture(-1L)
        ));
        return ChainableActions.run(actions);
    }

    private void deleteLeftOverFromPreviousOperations(ClusterState currentState,
                                                      List<ChainableAction<Long>> actions,
                                                      String resizeIndex) {

        if (currentState.metadata().hasIndex(resizeIndex)) {
            actions.add(new ChainableAction<>(
                () -> deleteIndex(resizeIndex),
                () -> CompletableFuture.completedFuture(-1L)
            ));
        }
    }

    private static void validateForResizeRequest(IndexMetadata sourceIndex, int targetNumberOfShards) {
        validateNumberOfShardsForResize(sourceIndex, targetNumberOfShards);
        validateReadOnlyIndexForResize(sourceIndex);
    }

    @VisibleForTesting
    static int getNumberOfShards(final Settings settings) {
        return Objects.requireNonNull(
            settings.getAsInt(SETTING_NUMBER_OF_SHARDS, null),
            "Setting 'number_of_shards' is missing"
        );
    }

    @VisibleForTesting
    static void validateNumberOfShardsForResize(IndexMetadata indexMetadata, int targetNumberOfShards) {
        final int currentNumberOfShards = indexMetadata.getNumberOfShards();
        if (currentNumberOfShards == targetNumberOfShards) {
            throw new IllegalArgumentException(
                String.format(Locale.ENGLISH,
                    "Table/partition is already allocated <%d> shards",
                    currentNumberOfShards));
        }
        if (targetNumberOfShards < currentNumberOfShards && currentNumberOfShards % targetNumberOfShards != 0) {
            throw new IllegalArgumentException(
                String.format(Locale.ENGLISH,
                    "Requested number of shards: <%d> needs to be a factor of the current one: <%d>",
                    targetNumberOfShards,
                    currentNumberOfShards));
        }
    }

    @VisibleForTesting
    static void validateReadOnlyIndexForResize(IndexMetadata indexMetadata) {
        final Boolean readOnly = indexMetadata
            .getSettings()
            .getAsBoolean(INDEX_BLOCKS_WRITE_SETTING.getKey(), Boolean.FALSE);
        if (!readOnly) {
            throw new IllegalStateException("Table/Partition needs to be at a read-only state." +
                                               " Use 'ALTER table ... set (\"blocks.write\"=true)' and retry");
        }
    }

    private CompletableFuture<Long> resizeIndex(IndexMetadata sourceIndex,
                                                @Nullable String sourceIndexAlias,
                                                String targetIndexName,
                                                int targetNumberOfShards) {
        Settings targetIndexSettings = Settings.builder()
            .put(SETTING_NUMBER_OF_SHARDS, targetNumberOfShards)
            .build();

        int currentNumShards = sourceIndex.getNumberOfShards();
        ResizeRequest request = new ResizeRequest(targetIndexName, sourceIndex.getIndex().getName());
        request.getTargetIndexRequest().settings(targetIndexSettings);
        if (sourceIndexAlias != null) {
            request.getTargetIndexRequest().alias(new Alias(sourceIndexAlias));
        }
        request.setResizeType(targetNumberOfShards > currentNumShards ? ResizeType.SPLIT : ResizeType.SHRINK);
        request.setCopySettings(Boolean.TRUE);
        request.setWaitForActiveShards(ActiveShardCount.ONE);
        FutureActionListener<ResizeResponse, Long> listener =
            new FutureActionListener<>(resp -> resp.isAcknowledged() ? 1L : 0L);

        transportResizeAction.execute(request, listener);
        return listener;
    }

    private CompletableFuture<Long> deleteIndex(String... indexNames) {
        DeleteIndexRequest request = new DeleteIndexRequest(indexNames);

        FutureActionListener<AcknowledgedResponse, Long> listener = new FutureActionListener<>(r -> 0L);
        transportDeleteIndexAction.execute(request, listener);
        return listener;
    }

    private CompletableFuture<Long> swapAndDropIndex(String source, String target) {
        SwapAndDropIndexRequest request = new SwapAndDropIndexRequest(source, target);
        FutureActionListener<AcknowledgedResponse, Long> listener = new FutureActionListener<>(response -> {
            if (!response.isAcknowledged()) {
                throw new RuntimeException("Publishing new cluster state during Shrink operation (rename phase) " +
                                           "has timed out");
            }
            return 0L;
        });
        transportSwapAndDropIndexNameAction.execute(request, listener);
        return listener;
    }

    public CompletableFuture<Long> executeAlterTableRenameTable(AnalyzedAlterTableRename statement) {
        DocTableInfo sourceTableInfo = statement.sourceTableInfo();
        RelationName sourceRelationName = sourceTableInfo.ident();
        RelationName targetRelationName = statement.targetTableIdent();

        return renameTable(sourceRelationName, targetRelationName, sourceTableInfo.isPartitioned());
    }

    private CompletableFuture<Long> renameTable(RelationName sourceRelationName,
                                                RelationName targetRelationName,
                                                boolean isPartitioned) {
        RenameTableRequest request = new RenameTableRequest(sourceRelationName, targetRelationName, isPartitioned);
        FutureActionListener<AcknowledgedResponse, Long> listener = new FutureActionListener<>(r -> -1L);
        transportRenameTableAction.execute(request, listener);
        return listener;
    }

    private CompletableFuture<Long> addColumnToTable(BoundAddColumn analysis, final FutureActionListener<AcknowledgedResponse, Long> result) {
        try {
            AlterTableRequest request = new AlterTableRequest(
                analysis.table().ident(),
                null,
                analysis.table().isPartitioned(),
                false,
                analysis.settings(),
                analysis.mapping()
            );
            transportAlterTableAction.execute(request, result);
            return result;
        } catch (IOException e) {
            return FutureActionListener.failedFuture(e);
        }
    }

    private class ResultSetReceiver implements ResultReceiver {

        private final BoundAddColumn analysis;
        private final FutureActionListener<AcknowledgedResponse, Long> result;

        private long count;

        ResultSetReceiver(BoundAddColumn analysis, FutureActionListener<AcknowledgedResponse, Long> result) {
            this.analysis = analysis;
            this.result = result;
        }

        @Override
        public void setNextRow(Row row) {
            count = (long) row.get(0);
        }

        @Override
        public void batchFinished() {
        }

        @Override
        public void allFinished(boolean interrupted) {
            if (count == 0L) {
                addColumnToTable(analysis, result);
            } else {
                String columnFailure = analysis.newPrimaryKeys() ? "primary key" : "generated";
                fail(new UnsupportedOperationException(String.format(Locale.ENGLISH,
                    "Cannot add a %s column to a table that isn't empty", columnFailure)));
            }
        }

        @Override
        public void fail(@Nonnull Throwable t) {
            result.completeExceptionally(t);
        }

        @Override
        public CompletableFuture<?> completionFuture() {
            return result;
        }

    }
}
