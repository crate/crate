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
import io.crate.common.annotations.VisibleForTesting;

import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;

import io.crate.analyze.AnalyzedDropTable;
import io.crate.data.InMemoryBatchIterator;
import io.crate.data.Row;
import io.crate.data.Row1;
import io.crate.data.RowConsumer;
import io.crate.exceptions.RelationUnknown;
import io.crate.exceptions.SQLExceptions;
import io.crate.execution.ddl.tables.DropTableRequest;
import io.crate.execution.ddl.tables.TransportDropTable;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.Plan;
import io.crate.planner.PlannerContext;
import io.crate.planner.operators.SubQueryResults;

public class DropTablePlan implements Plan {

    private static final Logger LOGGER = LogManager.getLogger(DropTablePlan.class);

    private final AnalyzedDropTable dropTable;

    public DropTablePlan(AnalyzedDropTable dropTable) {
        this.dropTable = dropTable;
    }

    @VisibleForTesting
    public AnalyzedDropTable dropTable() {
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
        var targets = dropTable.tables();
        ArrayList<CompletableFuture<Long>> futures = new ArrayList<>(targets.size());
        for (var target : targets) {
            var request = new DropTableRequest(target.tableName(), target.tableOid());
            var future = dependencies.client().execute(TransportDropTable.ACTION, request)
                .handle((response, err) -> {
                    if (err == null) {
                        if (!response.isAcknowledged() && LOGGER.isWarnEnabled()) {
                            LOGGER.warn("Dropping table {} was not acknowledged. This could lead to inconsistent state.",
                                target.tableName());
                        }
                        return 1L;
                    }
                    var unwrapped = SQLExceptions.unwrap(err);
                    boolean doesntExist = unwrapped instanceof IndexNotFoundException
                        || unwrapped instanceof RelationUnknown;
                    if (dropTable.dropIfExists() && doesntExist) {
                        return 0L;
                    }
                    throw new RuntimeException(unwrapped);
                });
            futures.add(future);
        }
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
            .whenComplete((ignored, err) -> {
                if (err == null) {
                    long count = 0;
                    for (var f : futures) {
                        count += f.join();
                    }
                    consumer.accept(InMemoryBatchIterator.of(new Row1(count), SENTINEL), null);
                } else {
                    consumer.accept(null, SQLExceptions.unwrap(err));
                }
            });
    }
}

