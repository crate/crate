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

import io.crate.analyze.AnalyzedCreateTable;
import io.crate.analyze.AnalyzedCreateTableAs;
import io.crate.analyze.BoundCreateTable;
import io.crate.analyze.NumberOfShards;
import io.crate.data.Row;
import io.crate.data.RowConsumer;
import io.crate.execution.ddl.tables.TableCreator;
import io.crate.metadata.Schemas;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.Plan;
import io.crate.planner.PlannerContext;
import io.crate.planner.node.ddl.CreateTablePlan;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.operators.LogicalPlanner;
import io.crate.planner.operators.SubQueryResults;

import java.util.function.Supplier;

public final class CreateTableAsPlanner {

    private CreateTableAsPlanner() {
    }

    public static Plan plan(AnalyzedCreateTableAs analyzedCreateTableAs,
                            NumberOfShards numberOfShards,
                            TableCreator tableCreator,
                            Schemas schemas,
                            PlannerContext context,
                            LogicalPlanner logicalPlanner) {
        Supplier<LogicalPlan> postponedInsertPlan =
            () -> logicalPlanner.plan(analyzedCreateTableAs.analyzePostponedInsertStatement(), context);
        return new CreateTableAsPlan(analyzedCreateTableAs.analyzedCreateTable(),
                                     postponedInsertPlan,
                                     tableCreator,
                                     numberOfShards,
                                     schemas);
    }

    public static final class CreateTableAsPlan implements Plan {

        private final AnalyzedCreateTable analyzedCreateTable;
        private final Supplier<LogicalPlan> postponedInsertPlan;
        private final TableCreator tableCreator;
        private final NumberOfShards numberOfShards;
        private final Schemas schemas;

        public CreateTableAsPlan(AnalyzedCreateTable analyzedCreateTable,
                                 Supplier<LogicalPlan> postponedInsertPlan,
                                 TableCreator tableCreator,
                                 NumberOfShards numberOfShards,
                                 Schemas schemas) {
            this.analyzedCreateTable = analyzedCreateTable;
            this.postponedInsertPlan = postponedInsertPlan;
            this.tableCreator = tableCreator;
            this.numberOfShards = numberOfShards;
            this.schemas = schemas;
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
            BoundCreateTable boundCreateTable = CreateTablePlan.bind(
                analyzedCreateTable,
                plannerContext.transactionContext(),
                dependencies.nodeContext(),
                params,
                subQueryResults,
                numberOfShards,
                schemas,
                dependencies.fulltextAnalyzerResolver());

            tableCreator.create(boundCreateTable)
                .thenRun(() -> postponedInsertPlan.get().execute(dependencies,
                                                                 plannerContext,
                                                                 consumer,
                                                                 params,
                                                                 subQueryResults));
        }
    }
}

