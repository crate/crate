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

package io.crate.execution.engine.join;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;
import java.util.function.ToIntFunction;

import org.elasticsearch.common.breaker.CircuitBreaker;

import io.crate.concurrent.CompletionListenable;
import io.crate.data.BatchIterator;
import io.crate.data.CapturingRowConsumer;
import io.crate.data.Paging;
import io.crate.data.Row;
import io.crate.data.RowConsumer;
import io.crate.data.breaker.RowAccounting;
import io.crate.data.join.CombinedRow;
import io.crate.execution.engine.collect.CollectExpression;
import io.crate.expression.InputFactory;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.TransactionContext;

public class HashJoinOperation implements CompletionListenable {

    private final CapturingRowConsumer leftConsumer;
    private final CapturingRowConsumer rightConsumer;
    private final RowConsumer resultConsumer;

    public HashJoinOperation(int numLeftCols,
                             int numRightCols,
                             RowConsumer nlResultConsumer,
                             Predicate<Row> joinPredicate,
                             List<Symbol> joinLeftInputs,
                             List<Symbol> joinRightInputs,
                             RowAccounting<Object[]> rowAccounting,
                             TransactionContext txnCtx,
                             InputFactory inputFactory,
                             CircuitBreaker circuitBreaker,
                             long estimatedRowSizeForLeft) {

        this.resultConsumer = nlResultConsumer;
        this.leftConsumer = new CapturingRowConsumer(nlResultConsumer.requiresScroll(), nlResultConsumer.completionFuture());
        this.rightConsumer = new CapturingRowConsumer(true, nlResultConsumer.completionFuture());
        CompletableFuture.allOf(leftConsumer.capturedBatchIterator(), rightConsumer.capturedBatchIterator())
            .whenComplete((result, failure) -> {
                if (failure == null) {
                    BatchIterator<Row> joinIterator;
                    try {
                        joinIterator = createHashJoinIterator(
                            leftConsumer.capturedBatchIterator().join(),
                            numLeftCols,
                            rightConsumer.capturedBatchIterator().join(),
                            numRightCols,
                            joinPredicate,
                            getHashBuilderFromSymbols(txnCtx, inputFactory, joinLeftInputs),
                            getHashBuilderFromSymbols(txnCtx, inputFactory, joinRightInputs),
                            rowAccounting,
                            new RamBlockSizeCalculator(
                                Paging.PAGE_SIZE,
                                circuitBreaker,
                                estimatedRowSizeForLeft
                            )
                        );
                        nlResultConsumer.accept(joinIterator, null);
                    } catch (Exception e) {
                        nlResultConsumer.accept(null, e);
                    }
                } else {
                    nlResultConsumer.accept(null, failure);
                }
            });
    }

    @Override
    public CompletableFuture<?> completionFuture() {
        return resultConsumer.completionFuture();
    }

    public RowConsumer leftConsumer() {
        return leftConsumer;
    }

    public RowConsumer rightConsumer() {
        return rightConsumer;
    }

    private static ToIntFunction<Row> getHashBuilderFromSymbols(TransactionContext txnCtx,
                                                                InputFactory inputFactory,
                                                                List<Symbol> inputs) {
        InputFactory.Context<? extends CollectExpression<Row, ?>> ctx = inputFactory.ctxForInputColumns(txnCtx, inputs);
        var topLevelInputs = ctx.topLevelInputs();
        var expressions = ctx.expressions();
        return row -> {
            for (int i = 0; i < expressions.size(); i++) {
                expressions.get(i).setNextRow(row);
            }
            int hash = 0;
            for (int i = 0; i < topLevelInputs.size(); i++) {
                Object value = topLevelInputs.get(i).value();
                hash = 31 * hash + (value == null ? 0 : value.hashCode());
            }
            return hash;
        };
    }

    private static BatchIterator<Row> createHashJoinIterator(BatchIterator<Row> left,
                                                             int leftNumCols,
                                                             BatchIterator<Row> right,
                                                             int rightNumCols,
                                                             Predicate<Row> joinCondition,
                                                             ToIntFunction<Row> hashBuilderForLeft,
                                                             ToIntFunction<Row> hashBuilderForRight,
                                                             RowAccounting<Object[]> rowAccounting,
                                                             RamBlockSizeCalculator blockSizeCalculator) {
        CombinedRow combiner = new CombinedRow(leftNumCols, rightNumCols);
        return new HashInnerJoinBatchIterator(
            left,
            right,
            rowAccounting,
            combiner,
            joinCondition,
            hashBuilderForLeft,
            hashBuilderForRight,
            blockSizeCalculator);
    }
}
