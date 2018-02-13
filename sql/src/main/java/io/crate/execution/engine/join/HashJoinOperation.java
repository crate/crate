/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.execution.engine.join;

import io.crate.breaker.RowAccounting;
import io.crate.concurrent.CompletionListenable;
import io.crate.data.BatchIterator;
import io.crate.data.ListenableBatchIterator;
import io.crate.data.Row;
import io.crate.data.RowConsumer;
import io.crate.data.join.CombinedRow;
import io.crate.data.join.JoinBatchIterators;
import io.crate.execution.engine.collect.CollectExpression;
import io.crate.expression.InputFactory;
import io.crate.expression.symbol.Symbol;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Predicate;

public class HashJoinOperation implements CompletionListenable {

    private final CompletableFuture<BatchIterator<Row>> leftBatchIterator = new CompletableFuture<>();
    private final CompletableFuture<BatchIterator<Row>> rightBatchIterator = new CompletableFuture<>();
    private final CompletableFuture<Void> completionFuture = new CompletableFuture<>();

    public HashJoinOperation(int numLeftCols,
                             int numRightCols,
                             RowConsumer nlResultConsumer,
                             Predicate<Row> joinPredicate,
                             List<Symbol> joinLeftInputs,
                             List<Symbol> joinRightInputs,
                             RowAccounting rowAccounting,
                             InputFactory inputFactory,
                             int leftSize) {

        CompletableFuture.allOf(leftBatchIterator, rightBatchIterator)
            .whenComplete((result, failure) -> {
                if (failure == null) {
                    BatchIterator<Row> joinIterator = new ListenableBatchIterator<>(createHashJoinIterator(
                        leftBatchIterator.join(),
                        numLeftCols,
                        rightBatchIterator.join(),
                        numRightCols,
                        joinPredicate,
                        getHashBuilderFromSymbols(inputFactory, joinLeftInputs),
                        getHashBuilderFromSymbols(inputFactory, joinRightInputs),
                        rowAccounting,
                        leftSize
                    ), completionFuture);
                    nlResultConsumer.accept(joinIterator, null);
                } else {
                    nlResultConsumer.accept(null, failure);
                }
            });
    }

    @Override
    public CompletableFuture<?> completionFuture() {
        return completionFuture;
    }

    public RowConsumer leftConsumer() {
        return JoinOperations.getBatchConsumer(leftBatchIterator, false);
    }

    public RowConsumer rightConsumer() {
        return JoinOperations.getBatchConsumer(rightBatchIterator, false);
    }

    private static Function<Row, Integer> getHashBuilderFromSymbols(InputFactory inputFactory, List<Symbol> inputs) {
        InputFactory.Context<? extends CollectExpression<Row, ?>> ctx = inputFactory.ctxForInputColumns(inputs);
        Object[] values = new Object[ctx.topLevelInputs().size()];

        return row -> {
            for (int i = 0; i < ctx.expressions().size(); i++) {
                ctx.expressions().get(i).setNextRow(row);
            }
            for (int i = 0; i < ctx.topLevelInputs().size(); i++) {
                values[i] = ctx.topLevelInputs().get(i).value();
            }
            return Objects.hash(values);
        };
    }

    private static BatchIterator<Row> createHashJoinIterator(BatchIterator<Row> left,
                                                             int leftNumCols,
                                                             BatchIterator<Row> right,
                                                             int rightNumCols,
                                                             Predicate<Row> joinCondition,
                                                             Function<Row, Integer> hashBuilderForLeft,
                                                             Function<Row, Integer> hashBuilderForRight,
                                                             RowAccounting rowAccounting,
                                                             int leftSize) {
        CombinedRow combiner = new CombinedRow(leftNumCols, rightNumCols);
        return JoinBatchIterators.hashInnerJoin(
            new RamAccountingBatchIterator<>(left, rowAccounting),
        right, combiner, joinCondition, hashBuilderForLeft, hashBuilderForRight, leftSize);

    }
}
