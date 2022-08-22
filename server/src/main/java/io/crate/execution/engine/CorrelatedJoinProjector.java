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

package io.crate.execution.engine;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import io.crate.data.AsyncFlatMapBatchIterator;
import io.crate.data.AsyncFlatMapper;
import io.crate.data.BatchIterator;
import io.crate.data.BatchIterators;
import io.crate.data.BiArrayRow;
import io.crate.data.CapturingRowConsumer;
import io.crate.data.CloseableIterator;
import io.crate.data.Projector;
import io.crate.data.Row;
import io.crate.expression.symbol.SelectSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.PlannerContext;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.operators.SubQueryResults;

public final class CorrelatedJoinProjector implements Projector {

    private final LogicalPlan subQueryPlan;
    private final PlannerContext plannerContext;
    private final DependencyCarrier executor;
    private final SubQueryResults subQueryResults;
    private final Row params;

    public CorrelatedJoinProjector(LogicalPlan subQueryPlan,
                                   SelectSymbol correlatedSubQuery,
                                   PlannerContext plannerContext,
                                   DependencyCarrier executor,
                                   SubQueryResults subQueryResults,
                                   Row params,
                                   List<Symbol> inputPlanOutputs) {
        this.subQueryPlan = subQueryPlan;
        this.plannerContext = plannerContext;
        this.executor = executor;
        this.subQueryResults = subQueryResults.forCorrelation(correlatedSubQuery, inputPlanOutputs);
        this.params = params;
    }

    @Override
    public BatchIterator<Row> apply(BatchIterator<Row> it) {
        var bindAndExecuteSubQuery = new BindAndExecuteSubQuery();
        return new AsyncFlatMapBatchIterator<>(it, bindAndExecuteSubQuery);
    }

    private final class BindAndExecuteSubQuery implements AsyncFlatMapper<Row, Row> {

        // See `CorrelatedJoin` operator. The output is the output of the left relation + the sub-query result
        final BiArrayRow outputRow = new BiArrayRow();
        final Function<Row, Row> materialize = subQueryRow -> {
            outputRow.secondCells(subQueryRow.materialize());
            return outputRow;
        };
        final Collector<Row, ?, List<Row>> toList = Collectors.mapping(materialize, Collectors.toList());

        @Override
        public CompletableFuture<? extends CloseableIterator<Row>> apply(Row inputRow, boolean isLastCall) {
            try {
                subQueryResults.bindOuterColumnInputRow(inputRow);
                var capturingRowConsumer = new CapturingRowConsumer(false);
                subQueryPlan.execute(
                    executor,
                    PlannerContext.forSubPlan(plannerContext),
                    capturingRowConsumer,
                    params,
                    subQueryResults
                );

                // Scalar sub-query returns 1 row, the materialization here shouldn't be too bad
                outputRow.firstCells(inputRow.materialize());
                return capturingRowConsumer.capturedBatchIterator()
                    .thenCompose(it -> BatchIterators.collect(it, toList))
                    .thenApply(rows -> {
                        if (rows.size() > 1) {
                            throw new UnsupportedOperationException(
                                "Subquery returned more than 1 row when it shouldn't.");
                        }
                        return CloseableIterator.fromIterator(rows.iterator());
                    });
            } catch (Throwable t) {
                return CompletableFuture.failedFuture(t);
            }
        }
    }
}
