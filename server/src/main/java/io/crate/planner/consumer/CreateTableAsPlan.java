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

package io.crate.planner.consumer;

import java.util.function.Supplier;

import io.crate.analyze.AnalyzedCreateTable;
import io.crate.analyze.AnalyzedCreateTableAs;
import io.crate.analyze.BoundCreateTable;
import io.crate.analyze.NumberOfShards;
import io.crate.data.Row;
import io.crate.data.RowConsumer;
import io.crate.execution.ddl.tables.TableCreator;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.Plan;
import io.crate.planner.PlannerContext;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.operators.LogicalPlanner;
import io.crate.planner.operators.SubQueryResults;

public final class CreateTableAsPlan implements Plan {

    private final AnalyzedCreateTable analyzedCreateTable;
    private final Supplier<LogicalPlan> postponedInsertPlan;
    private final TableCreator tableCreator;
    private final NumberOfShards numberOfShards;

    public static CreateTableAsPlan of(AnalyzedCreateTableAs analyzedCreateTableAs,
                                       NumberOfShards numberOfShards,
                                       TableCreator tableCreator,
                                       PlannerContext context,
                                       LogicalPlanner logicalPlanner) {
        Supplier<LogicalPlan> postponedInsertPlan =
            () -> logicalPlanner.plan(analyzedCreateTableAs.analyzePostponedInsertStatement(), context);
        return new CreateTableAsPlan(
            analyzedCreateTableAs.analyzedCreateTable(),
            postponedInsertPlan,
            tableCreator,
            numberOfShards
        );
    }

    public CreateTableAsPlan(AnalyzedCreateTable analyzedCreateTable,
                                Supplier<LogicalPlan> postponedInsertPlan,
                                TableCreator tableCreator,
                                NumberOfShards numberOfShards) {
        this.analyzedCreateTable = analyzedCreateTable;
        this.postponedInsertPlan = postponedInsertPlan;
        this.tableCreator = tableCreator;
        this.numberOfShards = numberOfShards;
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
        BoundCreateTable boundCreateTable = analyzedCreateTable.bind(
            numberOfShards,
            dependencies.fulltextAnalyzerResolver(),
            plannerContext.nodeContext(),
            plannerContext.transactionContext(),
            params,
            subQueryResults
        );
        tableCreator.create(boundCreateTable, plannerContext.clusterState().nodes().getMinNodeVersion())
            .thenRun(() -> postponedInsertPlan.get().execute(
                dependencies,
                plannerContext,
                consumer,
                params,
                subQueryResults));
    }
}

