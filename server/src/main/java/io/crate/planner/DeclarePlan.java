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

package io.crate.planner;

import java.util.concurrent.CompletableFuture;

import org.jetbrains.annotations.Nullable;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;

import io.crate.session.Cursor;
import io.crate.session.Cursors;
import io.crate.analyze.AnalyzedDeclare;
import io.crate.data.BatchIterator;
import io.crate.data.InMemoryBatchIterator;
import io.crate.data.Row;
import io.crate.data.RowConsumer;
import io.crate.data.SentinelRow;
import io.crate.planner.operators.SubQueryResults;
import io.crate.protocols.postgres.TransactionState;
import io.crate.sql.SqlFormatter;
import io.crate.sql.tree.Declare;
import io.crate.sql.tree.Declare.Hold;

public class DeclarePlan implements Plan {

    private final AnalyzedDeclare declare;
    private final Plan queryPlan;

    public DeclarePlan(AnalyzedDeclare declare, Plan queryPlan) {
        this.declare = declare;
        this.queryPlan = queryPlan;
    }

    @Override
    public StatementType type() {
        return StatementType.SELECT;
    }

    @Override
    public void executeOrFail(DependencyCarrier dependencies,
                              PlannerContext plannerContext,
                              RowConsumer consumer,
                              Row params,
                              SubQueryResults subQueryResults) throws Exception {
        if (declare.declare().hold() == Hold.WITHOUT
                && plannerContext.transactionState() != TransactionState.IN_TRANSACTION) {
            throw new UnsupportedOperationException("DECLARE CURSOR can only be used in transaction blocks");
        }
        CursorRowConsumer cursorRowConsumer = new CursorRowConsumer(consumer);
        Cursors cursors = plannerContext.cursors();
        Declare declareStmt = declare.declare();
        CircuitBreaker circuitBreaker = dependencies.circuitBreaker(HierarchyCircuitBreakerService.QUERY);
        Cursor cursor = new Cursor(
            circuitBreaker,
            declareStmt.cursorName(),
            SqlFormatter.formatSql(declareStmt),
            declareStmt.scroll(),
            declareStmt.hold(),
            cursorRowConsumer.declareResult,
            cursorRowConsumer.finalResult,
            declare.query().outputs()
        );
        cursors.add(declareStmt.cursorName(), cursor);
        queryPlan.execute(dependencies, plannerContext, cursorRowConsumer, params, subQueryResults);
    }

    private static class CursorRowConsumer implements RowConsumer {

        private final CompletableFuture<BatchIterator<Row>> declareResult = new CompletableFuture<>();
        private final CompletableFuture<Void> finalResult = new CompletableFuture<>();
        private final RowConsumer consumer;

        public CursorRowConsumer(RowConsumer consumer) {
            this.consumer = consumer;
        }

        @Override
        public void accept(BatchIterator<Row> iterator, @Nullable Throwable failure) {
            if (failure != null) {
                declareResult.completeExceptionally(failure);
            } else {
                // The result of DECLARE itself, not of the query
                consumer.accept(InMemoryBatchIterator.empty(SentinelRow.SENTINEL), null);
                declareResult.complete(iterator);
            }
        }

        @Override
        public CompletableFuture<?> completionFuture() {
            return finalResult;
        }
    }
}
