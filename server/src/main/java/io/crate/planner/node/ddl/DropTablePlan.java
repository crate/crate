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

import static io.crate.data.SentinelRow.SENTINEL;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.index.IndexNotFoundException;
import org.jetbrains.annotations.VisibleForTesting;

import io.crate.analyze.AnalyzedDropTable;
import io.crate.data.InMemoryBatchIterator;
import io.crate.data.Row;
import io.crate.data.Row1;
import io.crate.data.RowConsumer;
import io.crate.exceptions.SQLExceptions;
import io.crate.execution.ddl.tables.DropTableRequest;
import io.crate.metadata.table.TableInfo;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.Plan;
import io.crate.planner.PlannerContext;
import io.crate.planner.operators.SubQueryResults;

public class DropTablePlan implements Plan {

    private static final Logger LOGGER = LogManager.getLogger(DropTablePlan.class);
    private static final Row ROW_ZERO = new Row1(0L);
    private static final Row ROW_ONE = new Row1(1L);

    private final AnalyzedDropTable<?> dropTable;

    public DropTablePlan(AnalyzedDropTable<?> dropTable) {
        this.dropTable = dropTable;
    }

    @VisibleForTesting
    public TableInfo tableInfo() {
        return dropTable.table();
    }

    @VisibleForTesting
    public AnalyzedDropTable<?> dropTable() {
        return dropTable;
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
        TableInfo table = dropTable.table();
        DropTableRequest request;
        if (table == null) {
            if (dropTable.maybeCorrupt()) {
                request = new DropTableRequest(dropTable.tableName());
            } else {
                // no-op, table is already gone
                assert dropTable.dropIfExists() : "If table is null, IF EXISTS flag must have been present";
                consumer.accept(InMemoryBatchIterator.of(ROW_ZERO, SENTINEL), null);
                return;
            }
        } else {
            request = new DropTableRequest(table.ident());
        }
        dependencies.transportDropTableAction().execute(request).whenComplete((response, err) -> {
            if (err == null) {
                if (!response.isAcknowledged() && LOGGER.isWarnEnabled()) {
                    if (LOGGER.isWarnEnabled()) {
                        LOGGER.warn("Dropping table {} was not acknowledged. This could lead to inconsistent state.", dropTable.tableName());
                    }
                }
                consumer.accept(InMemoryBatchIterator.of(ROW_ONE, SENTINEL), null);
            } else {
                err = SQLExceptions.unwrap(err);
                if (dropTable.dropIfExists() && err instanceof IndexNotFoundException) {
                    consumer.accept(InMemoryBatchIterator.of(ROW_ZERO, SENTINEL), null);
                } else {
                    consumer.accept(null, err);
                }
            }
        });
    }
}
