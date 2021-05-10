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

package io.crate.planner.node.management;

import io.crate.analyze.AnalyzedShowCreateTable;
import io.crate.analyze.MetadataToASTNodeResolver;
import io.crate.data.InMemoryBatchIterator;
import io.crate.data.Row;
import io.crate.data.Row1;
import io.crate.data.RowConsumer;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.Plan;
import io.crate.planner.PlannerContext;
import io.crate.planner.operators.SubQueryResults;
import io.crate.sql.SqlFormatter;
import io.crate.sql.tree.CreateTable;

import static io.crate.data.SentinelRow.SENTINEL;

public class ShowCreateTablePlan implements Plan {

    private final AnalyzedShowCreateTable statement;

    public ShowCreateTablePlan(AnalyzedShowCreateTable statement) {
        this.statement = statement;
    }

    @Override
    public StatementType type() {
        return StatementType.MANAGEMENT;
    }

    @Override
    public void executeOrFail(DependencyCarrier dependencies,
                              PlannerContext plannerContext,
                              RowConsumer consumer,
                              Row params,
                              SubQueryResults subQueryResults) {
        Row1 row;
        try {
            CreateTable createTable = MetadataToASTNodeResolver.resolveCreateTable(statement.tableInfo());
            row = new Row1(SqlFormatter.formatSql(createTable));
        } catch (Throwable t) {
            consumer.accept(null, t);
            return;
        }
        consumer.accept(InMemoryBatchIterator.of(row, SENTINEL), null);
    }
}
