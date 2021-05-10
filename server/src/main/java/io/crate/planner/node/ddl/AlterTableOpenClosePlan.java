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

import io.crate.analyze.AnalyzedAlterTableOpenClose;
import io.crate.analyze.PartitionPropertiesAnalyzer;
import io.crate.analyze.SymbolEvaluator;
import io.crate.data.Row;
import io.crate.data.Row1;
import io.crate.data.RowConsumer;
import io.crate.execution.support.OneRowActionListener;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.PartitionName;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.Plan;
import io.crate.planner.PlannerContext;
import io.crate.planner.operators.SubQueryResults;
import io.crate.sql.tree.Table;

import java.util.function.Function;

public class AlterTableOpenClosePlan implements Plan {

    private final AnalyzedAlterTableOpenClose analyzedAlterTable;

    public AlterTableOpenClosePlan(AnalyzedAlterTableOpenClose analyzedAlterTable) {
        this.analyzedAlterTable = analyzedAlterTable;
    }

    @Override
    public StatementType type() {
        return StatementType.DDL;
    }

    @Override
    public void executeOrFail(DependencyCarrier dependencies,
                              PlannerContext plannerContext,
                              RowConsumer consumer,
                              Row params, SubQueryResults subQueryResults) {
        Function<? super Symbol, Object> eval = x -> SymbolEvaluator.evaluate(
            plannerContext.transactionContext(),
            plannerContext.nodeContext(),
            x,
            params,
            subQueryResults
        );

        DocTableInfo tableInfo = analyzedAlterTable.tableInfo();
        Table<Object> table = analyzedAlterTable.table().map(eval);

        PartitionName partitionName = null;
        if (tableInfo.isPartitioned()) {
            partitionName = PartitionPropertiesAnalyzer.createPartitionName(table.partitionProperties(), tableInfo);
        }

        dependencies.alterTableOperation()
            .executeAlterTableOpenClose(tableInfo, analyzedAlterTable.isOpenTable(), partitionName)
            .whenComplete(new OneRowActionListener<>(consumer, rCount -> new Row1(rCount == null ? -1 : rCount)));
    }
}
