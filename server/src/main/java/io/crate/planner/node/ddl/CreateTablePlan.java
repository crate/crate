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

import io.crate.analyze.AnalyzedCreateTable;
import io.crate.analyze.BoundCreateTable;
import io.crate.analyze.NumberOfShards;
import io.crate.data.Row;
import io.crate.data.Row1;
import io.crate.data.RowConsumer;
import io.crate.execution.ddl.tables.TableCreator;
import io.crate.execution.support.OneRowActionListener;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.Plan;
import io.crate.planner.PlannerContext;
import io.crate.planner.operators.SubQueryResults;

public class CreateTablePlan implements Plan {

    private final AnalyzedCreateTable createTable;
    private final NumberOfShards numberOfShards;
    private final TableCreator tableCreator;

    public CreateTablePlan(AnalyzedCreateTable createTable, NumberOfShards numberOfShards, TableCreator tableCreator) {
        this.createTable = createTable;
        this.numberOfShards = numberOfShards;
        this.tableCreator = tableCreator;
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
                              SubQueryResults subQueryResults) {
        BoundCreateTable boundCreateTable = createTable.bind(
            numberOfShards,
            dependencies.fulltextAnalyzerResolver(),
            plannerContext.nodeContext(),
            plannerContext.transactionContext(),
            params,
            subQueryResults
        );
        tableCreator.create(boundCreateTable, plannerContext.clusterState().nodes().getMinNodeVersion())
            .whenComplete(new OneRowActionListener<>(consumer, rCount -> new Row1(rCount == null ? -1 : rCount)));
    }
}
