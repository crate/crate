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

import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_BLOCKS_WRITE_SETTING;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.TransportDeleteIndexAction;
import org.elasticsearch.action.admin.indices.shrink.ResizeRequest;
import org.elasticsearch.action.admin.indices.shrink.TransportResizeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.VisibleForTesting;

import io.crate.action.FutureActionListener;
import io.crate.analyze.AnalyzedAlterTableRenameTable;
import io.crate.analyze.BoundAlterTable;
import io.crate.data.Row;
import io.crate.execution.ddl.index.SwapAndDropIndexRequest;
import io.crate.execution.ddl.index.TransportSwapAndDropIndexNameAction;
import io.crate.execution.support.ChainableAction;
import io.crate.execution.support.ChainableActions;
import io.crate.metadata.GeneratedReference;
import io.crate.metadata.PartitionName;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;
import io.crate.metadata.table.TableInfo;
import io.crate.replication.logical.LogicalReplicationService;
import io.crate.replication.logical.metadata.Publication;
import io.crate.session.CollectingResultReceiver;
import io.crate.session.Sessions;
import io.crate.sql.tree.ColumnPolicy;

@Singleton
public class AlterTableClient {

    public static final String RESIZE_PREFIX = ".resized.";

    private static final Logger LOGGER = LogManager.getLogger(AlterTableClient.class);

    private final ClusterService clusterService;
    private final TransportAlterTableAction transportAlterTableAction;
    private final TransportDropConstraintAction transportDropConstraintAction;
    private final TransportAddColumnAction transportAddColumnAction;
    private final TransportDropColumnAction transportDropColumnAction;
    private final TransportRenameTableAction transportRenameTableAction;
    private final TransportOpenTableAction transportOpenCloseTableOrPartitionAction;
    private final TransportResizeAction transportResizeAction;
    private final TransportDeleteIndexAction transportDeleteIndexAction;
    private final TransportSwapAndDropIndexNameAction transportSwapAndDropIndexNameAction;
    private final TransportCloseTable transportCloseTable;
    private final Sessions sessions;
    private final IndexScopedSettings indexScopedSettings;
    private final LogicalReplicationService logicalReplicationService;

    @Inject
    public AlterTableClient(ClusterService clusterService,
                            TransportRenameTableAction transportRenameTableAction,
                            TransportOpenTableAction transportOpenCloseTableOrPartitionAction,
                            TransportCloseTable transportCloseTable,
                            TransportResizeAction transportResizeAction,
                            TransportDeleteIndexAction transportDeleteIndexAction,
                            TransportSwapAndDropIndexNameAction transportSwapAndDropIndexNameAction,
                            TransportAlterTableAction transportAlterTableAction,
                            TransportDropConstraintAction transportDropConstraintAction,
                            TransportAddColumnAction transportAddColumnAction,
                            TransportDropColumnAction transportDropColumnAction,
                            Sessions sessions,
                            IndexScopedSettings indexScopedSettings,
                            LogicalReplicationService logicalReplicationService) {

        this.clusterService = clusterService;
        this.transportRenameTableAction = transportRenameTableAction;
        this.transportResizeAction = transportResizeAction;
        this.transportDeleteIndexAction = transportDeleteIndexAction;
        this.transportSwapAndDropIndexNameAction = transportSwapAndDropIndexNameAction;
        this.transportOpenCloseTableOrPartitionAction = transportOpenCloseTableOrPartitionAction;
        this.transportCloseTable = transportCloseTable;
        this.transportAlterTableAction = transportAlterTableAction;
        this.transportAddColumnAction = transportAddColumnAction;
        this.transportDropColumnAction = transportDropColumnAction;
        this.transportDropConstraintAction = transportDropConstraintAction;
        this.sessions = sessions;
        this.indexScopedSettings = indexScopedSettings;
        this.logicalReplicationService = logicalReplicationService;
    }

    public CompletableFuture<Long> addColumn(AddColumnRequest addColumnRequest) {
        var newReferences = addColumnRequest.references();
        String exceptionSubject = null;
        String warning = null;
        if (addColumnRequest.pKeyIndices().isEmpty() == false) {
            exceptionSubject = "primary key";
        } else if (newReferences.stream().anyMatch(ref -> ref instanceof GeneratedReference)) {
            exceptionSubject = "generated";
        } else {
            for (Reference newRef : newReferences) {
                if (newReferences.stream().anyMatch(r -> r.column().isChildOf(newRef.column()))
                    && (newRef.valueType().columnPolicy() == ColumnPolicy.IGNORED)) {
                    warning = "Adding a sub column to an OBJECT(IGNORED) parent may shade existing data of this column as the table isn't empty";
                    break;
                }
            }
        }
        if (exceptionSubject != null || warning != null) {
            String finalSubject = exceptionSubject;
            String finalWarning = warning;
            return getRowCount(addColumnRequest.relationName()).thenCompose(rowCount -> {
                if (rowCount > 0) {
                    if (finalSubject != null) {
                        throw new UnsupportedOperationException("Cannot add a " + finalSubject + " column to a table that isn't empty");
                    }
                    LOGGER.warn(finalWarning);
                }
                return transportAddColumnAction.execute(addColumnRequest).thenApply(_ -> -1L);
            });
        }
        return transportAddColumnAction.execute(addColumnRequest).thenApply(_ -> -1L);
    }

    public CompletableFuture<Long> dropColumn(DropColumnRequest dropColumnRequest) {
        return transportDropColumnAction.execute(dropColumnRequest).thenApply(_ -> -1L);
    }

    private CompletableFuture<Long> getRowCount(RelationName ident) {
        String stmt =
            String.format(Locale.ENGLISH, "SELECT COUNT(*) FROM \"%s\".\"%s\"", ident.schema(), ident.name());

        var rowCountReceiver = new CollectingResultReceiver<>(Collectors.summingLong(row -> (long) row.get(0)));
        try {
            try (var session = sessions.newSystemSession()) {
                session.quickExec(stmt, rowCountReceiver, Row.EMPTY);
            }
        } catch (Throwable t) {
            return CompletableFuture.failedFuture(t);
        }
        return rowCountReceiver.completionFuture();
    }

    public CompletableFuture<Long> closeOrOpen(RelationName relationName,
                                               boolean openTable,
                                               @Nullable PartitionName partitionName) {
        if (openTable) {
            OpenTableRequest request = new OpenTableRequest(relationName, partitionName == null ? List.of() : partitionName.values());
            return transportOpenCloseTableOrPartitionAction.execute(request, _ -> -1L);
        } else {
            return transportCloseTable.execute(
                new CloseTableRequest(relationName, partitionName == null ? List.of() : partitionName.values()),
                _ -> -1L
            );
        }
    }

    public CompletableFuture<Long> setSettingsOrResize(BoundAlterTable analysis) {
        validateSettingsForPublishedTables(analysis.table().ident(),
                                           analysis.settings(),
                                           logicalReplicationService.publications(),
                                           indexScopedSettings);

        final Settings settings = analysis.settings();
        final boolean includesNumberOfShardsSetting = settings.hasValue(SETTING_NUMBER_OF_SHARDS);
        final boolean isResizeOperationRequired = includesNumberOfShardsSetting &&
                                                  (!analysis.isPartitioned() || analysis.partitionName() != null);

        if (isResizeOperationRequired) {
            if (settings.size() > 1) {
                throw new IllegalArgumentException("Setting [number_of_shards] cannot be combined with other settings");
            }
            return resize(analysis);
        }
        return setSettings(analysis);
    }

    private CompletableFuture<Long> setSettings(BoundAlterTable analysis) {
        try {
            PartitionName partitionName = analysis.partitionName();
            AlterTableRequest request = new AlterTableRequest(
                analysis.table().ident(),
                partitionName == null ? List.of() : partitionName.values(),
                analysis.isPartitioned(),
                analysis.excludePartitions(),
                analysis.settings()
            );
            return transportAlterTableAction.execute(request, _ -> -1L);
        } catch (IOException e) {
            return FutureActionListener.failedFuture(e);
        }
    }

    private CompletableFuture<Long> resize(BoundAlterTable analysis) {
        final TableInfo table = analysis.table();
        PartitionName partitionName = analysis.partitionName();
        String sourceIndexName = partitionName == null
            ? table.ident().indexNameOrAlias()
            : partitionName.asIndexName();

        final ClusterState currentState = clusterService.state();
        final IndexMetadata sourceIndexMetadata = currentState.metadata().index(sourceIndexName);
        final int targetNumberOfShards = getNumberOfShards(analysis.settings());
        validateNumberOfShardsForResize(sourceIndexMetadata, targetNumberOfShards);
        validateReadOnlyIndexForResize(sourceIndexMetadata);

        final List<ChainableAction<Long>> actions = new ArrayList<>();
        final String resizedIndex = RESIZE_PREFIX + sourceIndexName;
        deleteLeftOverFromPreviousOperations(currentState, actions, resizedIndex);

        final ResizeRequest request = new ResizeRequest(
            table.ident(),
            partitionName == null ? List.of() : partitionName.values(),
            targetNumberOfShards
        );

        actions.add(new ChainableAction<>(
            () -> transportResizeAction.execute(request, r -> r.isAcknowledged() ? 1L : 0L),
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

    @VisibleForTesting
    static void validateSettingsForPublishedTables(RelationName relationName,
                                                   Settings settings,
                                                   Map<String, Publication> publications,
                                                   IndexScopedSettings indexScopedSettings) {
        // Static settings changes must be prevented for tables included
        // in logical replication publication. Static settings cannot not be applied ad-hoc on
        // an open table and may also cause the shard tracking as part of the logical replication
        // to fail e.g. when the number of shards is changed on a table which is tracked.
        for (var entry : publications.entrySet()) {
            var publicationName = entry.getKey();
            var publication = entry.getValue();
            if (publication.isForAllTables() || publication.tables().contains(relationName)) {
                for (var key : settings.keySet()) {
                    if (!indexScopedSettings.isDynamicSetting(key)) {
                        throw new IllegalArgumentException(
                            String.format(
                                Locale.ENGLISH,
                                "Setting [%s] cannot be applied to table '%s' because it is included in a logical replication publication '%s'",
                                key,
                                relationName.toString(),
                                publicationName
                            )
                        );
                    }
                }
            }
        }
    }

    private CompletableFuture<Long> deleteIndex(String... indexNames) {
        DeleteIndexRequest request = new DeleteIndexRequest(indexNames);
        return transportDeleteIndexAction.execute(request, _ -> 0L);
    }

    private CompletableFuture<Long> swapAndDropIndex(String source, String target) {
        SwapAndDropIndexRequest request = new SwapAndDropIndexRequest(source, target);
        return transportSwapAndDropIndexNameAction.execute(request, response -> {
            if (!response.isAcknowledged()) {
                throw new RuntimeException("Publishing new cluster state during Shrink operation (rename phase) " +
                                           "has timed out");
            }
            return 0L;
        });
    }

    public CompletableFuture<Long> rename(AnalyzedAlterTableRenameTable renameTable) {
        var request = new RenameTableRequest(renameTable.sourceName(), renameTable.targetName(), renameTable.isPartitioned());
        return transportRenameTableAction.execute(request, _ -> -1L);
    }

    public CompletableFuture<Long> dropConstraint(DropConstraintRequest request) {
        return transportDropConstraintAction.execute(request).thenApply(_ -> -1L);
    }

}
