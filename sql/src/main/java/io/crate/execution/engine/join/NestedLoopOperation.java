/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

import io.crate.concurrent.CompletionListenable;
import io.crate.data.BatchIterator;
import io.crate.data.FilteringBatchIterator;
import io.crate.data.ListenableBatchIterator;
import io.crate.data.Row;
import io.crate.data.RowConsumer;
import io.crate.data.join.CombinedRow;
import io.crate.data.join.JoinBatchIterators;
import io.crate.planner.node.dql.join.JoinType;

import javax.annotation.Nullable;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;


public class NestedLoopOperation implements CompletionListenable {

    private final CompletableFuture<BatchIterator<Row>> leftBatchIterator = new CompletableFuture<>();
    private final CompletableFuture<BatchIterator<Row>> rightBatchIterator = new CompletableFuture<>();
    private final CompletableFuture<Void> completionFuture = new CompletableFuture<>();

    public NestedLoopOperation(int numLeftCols,
                               int numRightCols,
                               RowConsumer nlResultConsumer,
                               Predicate<Row> joinPredicate,
                               JoinType joinType) {

        CompletableFuture.allOf(leftBatchIterator, rightBatchIterator)
            .whenComplete((result, failure) -> {
                if (failure == null) {
                    BatchIterator<Row> nlIterator = new ListenableBatchIterator<>(createNestedLoopIterator(
                        leftBatchIterator.join(),
                        numLeftCols,
                        rightBatchIterator.join(),
                        numRightCols,
                        joinType,
                        joinPredicate
                    ), completionFuture);
                    nlResultConsumer.accept(nlIterator, null);
                } else {
                    nlResultConsumer.accept(null, failure);
                }
            });
    }

    private static BatchIterator<Row> createNestedLoopIterator(BatchIterator<Row> left,
                                                               int leftNumCols,
                                                               BatchIterator<Row> right,
                                                               int rightNumCols,
                                                               JoinType joinType,
                                                               Predicate<Row> joinCondition) {
        CombinedRow combiner = new CombinedRow(leftNumCols, rightNumCols);
        switch (joinType) {
            case CROSS:
                return JoinBatchIterators.crossJoin(left, right, combiner);

            case INNER:
                return new FilteringBatchIterator<>(
                    JoinBatchIterators.crossJoin(left, right, combiner), joinCondition);

            case LEFT:
                return JoinBatchIterators.leftJoin(left, right, combiner, joinCondition);

            case RIGHT:
                return JoinBatchIterators.rightJoin(left, right, combiner, joinCondition);

            case FULL:
                return JoinBatchIterators.fullOuterJoin(left, right, combiner, joinCondition);

            case SEMI:
                return JoinBatchIterators.semiJoin(left, right, combiner, joinCondition);

            case ANTI:
                return JoinBatchIterators.antiJoin(left, right, combiner, joinCondition);

            default:
                throw new AssertionError("Invalid joinType: " + joinType);
        }
    }

    public RowConsumer leftConsumer() {
        return getBatchConsumer(leftBatchIterator, false);
    }

    public RowConsumer rightConsumer() {
        return getBatchConsumer(rightBatchIterator, true);
    }

    private RowConsumer getBatchConsumer(CompletableFuture<BatchIterator<Row>> future, boolean requiresRepeat) {
        return new RowConsumer() {
            @Override
            public void accept(BatchIterator<Row> iterator, @Nullable Throwable failure) {
                if (failure == null) {
                    future.complete(iterator);
                } else {
                    future.completeExceptionally(failure);
                }
            }

            @Override
            public boolean requiresScroll() {
                return requiresRepeat;
            }
        };
    }

    @Override
    public CompletableFuture<?> completionFuture() {
        return completionFuture;
    }

}
