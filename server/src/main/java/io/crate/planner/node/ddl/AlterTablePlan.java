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

import static io.crate.metadata.table.Operation.isReplicated;

import java.util.Locale;
import java.util.Set;
import java.util.function.Function;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.settings.Settings;
import org.jetbrains.annotations.Nullable;

import io.crate.analyze.AnalyzedAlterTable;
import io.crate.analyze.BoundAlterTable;
import io.crate.analyze.SymbolEvaluator;
import io.crate.analyze.TableParameters;
import io.crate.analyze.TableProperties;
import io.crate.data.Row;
import io.crate.data.Row1;
import io.crate.data.RowConsumer;
import io.crate.execution.support.OneRowActionListener;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.NodeContext;
import io.crate.metadata.PartitionName;
import io.crate.metadata.blob.BlobSchemaInfo;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.Operation;
import io.crate.metadata.table.TableInfo;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.Plan;
import io.crate.planner.PlannerContext;
import io.crate.planner.operators.SubQueryResults;
import io.crate.sql.tree.AlterTable;
import io.crate.sql.tree.Table;

public class AlterTablePlan implements Plan {

    final AnalyzedAlterTable alterTable;

    public AlterTablePlan(AnalyzedAlterTable alterTable) {
        this.alterTable = alterTable;
    }

    @Override
    public StatementType type() {
        return StatementType.DDL;
    }

    @Override
    public void executeOrFail(DependencyCarrier dependencies,
                              PlannerContext plannerContext,
                              RowConsumer consumer,
                              Row params,
                              SubQueryResults subQueryResults) throws Exception {
        BoundAlterTable stmt = bind(
            alterTable,
            plannerContext.transactionContext(),
            dependencies.nodeContext(),
            params,
            subQueryResults,
            plannerContext.clusterState().metadata()
        );


        dependencies.alterTableClient().setSettingsOrResize(stmt)
            .whenComplete(new OneRowActionListener<>(consumer, rCount -> new Row1(rCount == null ? -1 : rCount)));
    }

    public static BoundAlterTable bind(AnalyzedAlterTable analyzedAlterTable,
                                       CoordinatorTxnCtx txnCtx,
                                       NodeContext nodeCtx,
                                       Row params,
                                       SubQueryResults subQueryResults,
                                       Metadata metadata) {
        Function<? super Symbol, Object> eval = x -> SymbolEvaluator.evaluate(
            txnCtx,
            nodeCtx,
            x,
            params,
            subQueryResults
        );
        TableInfo tableInfo = analyzedAlterTable.tableInfo();
        AlterTable<Object> alterTable = analyzedAlterTable.alterTable().map(eval);
        Table<Object> table = alterTable.table();

        boolean isPartitioned = false;
        PartitionName partitionName = null;
        TableParameters tableParameters;
        if (tableInfo instanceof DocTableInfo docTableInfo) {
            partitionName = table.partitionProperties().isEmpty()
                ? null
                : PartitionName.ofAssignments(docTableInfo, table.partitionProperties(), metadata);
            isPartitioned = docTableInfo.isPartitioned();
            tableParameters = getTableParameterInfo(table, tableInfo, partitionName);
        } else {
            assert tableInfo.ident().schema().equals(BlobSchemaInfo.NAME) : "If tableInfo is not a DocTableInfo, the schema must be `blob`";
            tableParameters = TableParameters.ALTER_BLOB_TABLE_PARAMETERS;
        }
        Settings.Builder settingsBuilder = getTableParameter(alterTable, tableParameters);
        Settings settings = settingsBuilder.build();
        if (partitionName != null) {
            for (var tableOnlySetting : TableParameters.TABLE_ONLY_SETTINGS) {
                if (tableOnlySetting.exists(settings)) {
                    throw new IllegalArgumentException(String.format(
                        Locale.ENGLISH,
                        "Changing \"%s\" on partition level is not supported",
                        tableOnlySetting.getKey()
                    ));
                }
            }
        }
        maybeRaiseBlockedException(tableInfo, settings);
        return new BoundAlterTable(
            tableInfo,
            partitionName,
            settings,
            table.excludePartitions(),
            isPartitioned
        );
    }

    private static TableParameters getTableParameterInfo(Table<?> table, TableInfo tableInfo, @Nullable PartitionName partitionName) {
        if (isReplicated(tableInfo.parameters())) {
            return TableParameters.REPLICATED_TABLE_ALTER_PARAMETER_INFO;
        }
        if (partitionName == null) {
            return TableParameters.TABLE_ALTER_PARAMETER_INFO;
        }
        assert !table.excludePartitions() : "Alter table ONLY not supported when using a partition";
        return TableParameters.PARTITION_PARAMETER_INFO;
    }

    private static Settings.Builder getTableParameter(AlterTable<Object> node, TableParameters tableParameters) {
        Settings.Builder settingsBuilder = Settings.builder();
        if (!node.genericProperties().isEmpty()) {
            TableProperties.analyze(settingsBuilder, tableParameters, node.genericProperties());
        } else if (!node.resetProperties().isEmpty()) {
            TableProperties.analyzeResetProperties(settingsBuilder, tableParameters, node.resetProperties());
        }
        return settingsBuilder;
    }

    // Only check for permission if statement is not changing the metadata blocks, so don't block `re-enabling` these.
    static void maybeRaiseBlockedException(TableInfo tableInfo, Settings tableSettings) {
        Set<String> blockSettings = Set.of(
            IndexMetadata.SETTING_BLOCKS_METADATA,
            IndexMetadata.SETTING_BLOCKS_READ,
            IndexMetadata.SETTING_BLOCKS_WRITE,
            IndexMetadata.SETTING_READ_ONLY
        );
        if (blockSettings.containsAll(tableSettings.keySet())) {
            Operation.blockedRaiseException(tableInfo, Operation.ALTER_BLOCKS);
        } else {
            Operation.blockedRaiseException(tableInfo, Operation.ALTER);
        }
    }
}
