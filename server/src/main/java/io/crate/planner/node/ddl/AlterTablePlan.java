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

import java.util.function.Function;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.jetbrains.annotations.Nullable;

import io.crate.analyze.AnalyzedAlterTable;
import io.crate.analyze.BoundAlterTable;
import io.crate.analyze.PartitionPropertiesAnalyzer;
import io.crate.analyze.SymbolEvaluator;
import io.crate.analyze.TableParameter;
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
            subQueryResults);


        dependencies.alterTableOperation().executeAlterTable(stmt)
            .whenComplete(new OneRowActionListener<>(consumer, rCount -> new Row1(rCount == null ? -1 : rCount)));
    }

    public static BoundAlterTable bind(AnalyzedAlterTable analyzedAlterTable,
                                       CoordinatorTxnCtx txnCtx,
                                       NodeContext nodeCtx,
                                       Row params,
                                       SubQueryResults subQueryResults) {
        Function<? super Symbol, Object> eval = x -> SymbolEvaluator.evaluate(
            txnCtx,
            nodeCtx,
            x,
            params,
            subQueryResults
        );
        DocTableInfo docTableInfo = analyzedAlterTable.tableInfo();
        AlterTable<Object> alterTable = analyzedAlterTable.alterTable().map(eval);
        Table<Object> table = alterTable.table();

        PartitionName partitionName = PartitionPropertiesAnalyzer.createPartitionName(table.partitionProperties(), docTableInfo);
        TableParameters tableParameters = getTableParameterInfo(table, docTableInfo, partitionName);
        TableParameter tableParameter = getTableParameter(alterTable, tableParameters);
        maybeRaiseBlockedException(docTableInfo, tableParameter.settings());
        return new BoundAlterTable(
            docTableInfo,
            partitionName,
            tableParameter,
            table.excludePartitions(),
            docTableInfo.isPartitioned());
    }

    private static TableParameters getTableParameterInfo(Table table, TableInfo tableInfo, @Nullable PartitionName partitionName) {
        if (isReplicated(tableInfo.parameters())) {
            return TableParameters.REPLICATED_TABLE_ALTER_PARAMETER_INFO;
        }
        if (partitionName == null) {
            return TableParameters.TABLE_ALTER_PARAMETER_INFO;
        }
        assert !table.excludePartitions() : "Alter table ONLY not supported when using a partition";
        return TableParameters.PARTITION_PARAMETER_INFO;
    }

    public static TableParameter getTableParameter(AlterTable<Object> node, TableParameters tableParameters) {
        TableParameter tableParameter = new TableParameter();
        if (!node.genericProperties().isEmpty()) {
            TableProperties.analyze(tableParameter, tableParameters, node.genericProperties(), false);
        } else if (!node.resetProperties().isEmpty()) {
            TableProperties.analyzeResetProperties(tableParameter, tableParameters, node.resetProperties());
        }
        return tableParameter;
    }

    // Only check for permission if statement is not changing the metadata blocks, so don't block `re-enabling` these.
    static void maybeRaiseBlockedException(TableInfo tableInfo, Settings tableSettings) {
        if (tableSettings.size() != 1 ||
            (tableSettings.get(IndexMetadata.SETTING_BLOCKS_METADATA) == null &&
             tableSettings.get(IndexMetadata.SETTING_READ_ONLY) == null)) {

            Operation.blockedRaiseException(tableInfo, Operation.ALTER);
        }
    }

}
