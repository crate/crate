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

import java.util.function.Function;

import org.elasticsearch.cluster.metadata.Metadata;

import io.crate.analyze.AnalyzedAlterTable;
import io.crate.analyze.BoundAlterTable;
import io.crate.analyze.SymbolEvaluator;
import io.crate.data.Row;
import io.crate.data.Row1;
import io.crate.data.RowConsumer;
import io.crate.execution.support.OneRowActionListener;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.NodeContext;
import io.crate.metadata.PartitionName;
import io.crate.metadata.doc.DocTableInfo;
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


        dependencies.alterTableOperation().executeAlterTable(stmt)
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
        AlterTable<Object> alterTable = analyzedAlterTable.alterTable().map(eval);
        TableInfo tableInfo = analyzedAlterTable.tableInfo();
        boolean isPartitioned = false;
        PartitionName partitionName = null;
        if (tableInfo instanceof DocTableInfo docTableInfo) {
            Table<Object> table = alterTable.table();
            partitionName = table.partitionProperties().isEmpty()
                ? null
                : PartitionName.ofAssignments(docTableInfo, table.partitionProperties(), metadata);
            isPartitioned = docTableInfo.isPartitioned();
        }
        return new BoundAlterTable(analyzedAlterTable, alterTable, isPartitioned, partitionName);
    }
}
