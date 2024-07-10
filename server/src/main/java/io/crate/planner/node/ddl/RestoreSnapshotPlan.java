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

package io.crate.planner.node.ddl;

import static io.crate.analyze.SnapshotSettings.IGNORE_UNAVAILABLE;
import static io.crate.analyze.SnapshotSettings.SCHEMA_RENAME_PATTERN;
import static io.crate.analyze.SnapshotSettings.SCHEMA_RENAME_REPLACEMENT;
import static io.crate.analyze.SnapshotSettings.TABLE_RENAME_PATTERN;
import static io.crate.analyze.SnapshotSettings.TABLE_RENAME_REPLACEMENT;
import static io.crate.analyze.SnapshotSettings.WAIT_FOR_COMPLETION;

import java.util.HashSet;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotAction;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.settings.Settings;
import org.jetbrains.annotations.VisibleForTesting;

import io.crate.analyze.AnalyzedRestoreSnapshot;
import io.crate.analyze.BoundRestoreSnapshot;
import io.crate.analyze.SnapshotSettings;
import io.crate.analyze.SymbolEvaluator;
import io.crate.common.collections.Lists;
import io.crate.data.Row;
import io.crate.data.Row1;
import io.crate.data.RowConsumer;
import io.crate.exceptions.RelationUnknown;
import io.crate.exceptions.SchemaUnknownException;
import io.crate.execution.support.OneRowActionListener;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.NodeContext;
import io.crate.metadata.PartitionName;
import io.crate.metadata.RelationName;
import io.crate.metadata.Schemas;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.Operation;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.Plan;
import io.crate.planner.PlannerContext;
import io.crate.planner.operators.SubQueryResults;
import io.crate.sql.tree.Assignment;
import io.crate.sql.tree.GenericProperties;
import io.crate.sql.tree.Table;

public class RestoreSnapshotPlan implements Plan {

    private static final String ALL_TEMPLATES = "_all";

    private final AnalyzedRestoreSnapshot restoreSnapshot;

    public RestoreSnapshotPlan(AnalyzedRestoreSnapshot restoreSnapshot) {
        this.restoreSnapshot = restoreSnapshot;
    }

    @Override
    public StatementType type() {
        return StatementType.DDL;
    }

    @Override
    public void executeOrFail(DependencyCarrier dependencies,
                              PlannerContext plannerContext,
                              RowConsumer consumer,
                              Row parameters,
                              SubQueryResults subQueryResults) {
        BoundRestoreSnapshot stmt = bind(
            restoreSnapshot,
            plannerContext.transactionContext(),
            dependencies.nodeContext(),
            parameters,
            subQueryResults,
            dependencies.schemas()
        );
        var settings = stmt.settings();
        boolean ignoreUnavailable = IGNORE_UNAVAILABLE.get(settings);

        // ignore_unavailable as set by statement
        IndicesOptions indicesOptions = IndicesOptions.fromOptions(
            ignoreUnavailable,
            true,
            true,
            false,
            IndicesOptions.LENIENT_EXPAND_OPEN
        );

        boolean includeTables = stmt.includeTables();

        List<RestoreSnapshotRequest.TableOrPartition> tablesToRestore = stmt.restoreTables().stream()
            .map(restoreTableInfo -> {
                String partitionIdent = null;
                if (restoreTableInfo.partitionName() != null) {
                    partitionIdent = restoreTableInfo.partitionName().ident();
                }
                return new RestoreSnapshotRequest.TableOrPartition(restoreTableInfo.tableIdent(), partitionIdent);
            })
            .collect(Collectors.toList());

        String tableRenamePattern = TABLE_RENAME_PATTERN.get(settings);
        String tableRenameReplacement = TABLE_RENAME_REPLACEMENT.get(settings);
        String schemaRenamePattern = SCHEMA_RENAME_PATTERN.get(settings);
        String schemaRenameReplacement = SCHEMA_RENAME_REPLACEMENT.get(settings);

        RestoreSnapshotRequest request = new RestoreSnapshotRequest(
            restoreSnapshot.repository(),
            restoreSnapshot.snapshot())
            .tablesToRestore(tablesToRestore)
            .tableRenamePattern(tableRenamePattern)
            .tableRenameReplacement(tableRenameReplacement)
            .schemaRenamePattern(schemaRenamePattern)
            .schemaRenameReplacement(schemaRenameReplacement)
            .indicesOptions(indicesOptions)
            .settings(settings)
            .waitForCompletion(WAIT_FOR_COMPLETION.get(settings))
            .includeIndices(includeTables)
            .includeAliases(includeTables)
            .includeCustomMetadata(stmt.includeCustomMetadata())
            .customMetadataTypes(stmt.customMetadataTypes())
            .includeGlobalSettings(stmt.includeGlobalSettings())
            .globalSettings(stmt.globalSettings());
        dependencies.client().execute(RestoreSnapshotAction.INSTANCE, request)
            .whenComplete(new OneRowActionListener<>(consumer, r -> new Row1(r == null ? -1L : 1L)));
    }

    @VisibleForTesting
    public static BoundRestoreSnapshot bind(AnalyzedRestoreSnapshot restoreSnapshot,
                                            CoordinatorTxnCtx txnCtx,
                                            NodeContext nodeCtx,
                                            Row parameters,
                                            SubQueryResults subQueryResults,
                                            Schemas schemas) {
        Function<? super Symbol, Object> eval = x -> SymbolEvaluator.evaluate(
            txnCtx,
            nodeCtx,
            x,
            parameters,
            subQueryResults
        );

        GenericProperties<Object> properties = restoreSnapshot.properties()
            .ensureContainsOnly(SnapshotSettings.SETTINGS.keySet())
            .map(eval);
        Settings settings = Settings.builder().put(properties).build();

        HashSet<BoundRestoreSnapshot.RestoreTableInfo> restoreTables = new HashSet<>(restoreSnapshot.tables().size());
        for (Table<Symbol> table : restoreSnapshot.tables()) {
            var relationName = RelationName.of(
                table.getName(),
                txnCtx.sessionSettings().searchPath().currentSchema());

            List<Assignment<Object>> partitionProperties = Lists.map(table.partitionProperties(), x -> x.map(eval));
            try {
                DocTableInfo docTableInfo = schemas.getTableInfo(relationName);
                Operation.blockedRaiseException(docTableInfo, Operation.RESTORE_SNAPSHOT);
                // Table existence check is done later after resolving indices and applying all table name/schema renaming options.
                PartitionName partitionName = partitionProperties.isEmpty()
                    ? null
                    : PartitionName.ofAssignmentsUnsafe(docTableInfo, partitionProperties);
                restoreTables.add(new BoundRestoreSnapshot.RestoreTableInfo(relationName, partitionName));
            } catch (RelationUnknown | SchemaUnknownException e) {
                if (table.partitionProperties().isEmpty()) {
                    restoreTables.add(new BoundRestoreSnapshot.RestoreTableInfo(relationName, null));
                } else {
                    var partitionName = PartitionName.ofAssignments(relationName, partitionProperties);
                    restoreTables.add(
                        new BoundRestoreSnapshot.RestoreTableInfo(relationName, partitionName));
                }
            }
        }

        return new BoundRestoreSnapshot(
            restoreSnapshot.repository(),
            restoreSnapshot.snapshot(),
            restoreTables,
            restoreSnapshot.includeTables(),
            restoreSnapshot.includeCustomMetadata(),
            restoreSnapshot.customMetadataTypes(),
            restoreSnapshot.includeGlobalSettings(),
            restoreSnapshot.globalSettings(),
            settings
        );
    }
}
