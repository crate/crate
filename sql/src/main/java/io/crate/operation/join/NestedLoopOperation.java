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

package io.crate.operation.join;

import io.crate.concurrent.CompletionListenable;
import io.crate.data.*;
import io.crate.data.join.NestedLoopBatchIterator;
import io.crate.planner.node.dql.join.JoinType;

import javax.annotation.Nullable;
import java.util.concurrent.CompletableFuture;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.Predicate;


public class NestedLoopOperation implements CompletionListenable {

    private final CompletableFuture<BatchIterator> leftBatchIterator = new CompletableFuture<>();
    private final CompletableFuture<BatchIterator> rightBatchIterator = new CompletableFuture<>();
    private final CompletableFuture<Void> completionFuture = new CompletableFuture<>();

    public NestedLoopOperation(BatchConsumer nlResultConsumer,
                               Predicate<Row> joinPredicate,
                               JoinType joinType) {

        CompletableFuture.allOf(leftBatchIterator, rightBatchIterator)
            .whenComplete((result, failure) -> {
                if (failure == null) {
                    BatchIterator nlIterator = new ListenableBatchIterator(createNestedLoopIterator(
                        leftBatchIterator.join(),
                        rightBatchIterator.join(),
                        joinType,
                        joinPredicate
                    ), completionFuture);
                    nlResultConsumer.accept(nlIterator, null);
                } else {
                    nlResultConsumer.accept(null, failure);
                }
            });
    }

    private static BatchIterator createNestedLoopIterator(BatchIterator left,
                                                          BatchIterator right,
                                                          JoinType joinType,
                                                          Predicate<Row> joinCondition) {
        switch (joinType) {
            case CROSS:
                return NestedLoopBatchIterator.crossJoin(left, right);

            case INNER:
                return new FilteringBatchIterator(
                    NestedLoopBatchIterator.crossJoin(left, right), getJoinCondition(joinCondition));

            case LEFT:
                return NestedLoopBatchIterator.leftJoin(left, right, getJoinCondition(joinCondition));

            case RIGHT:
                return NestedLoopBatchIterator.rightJoin(left, right, getJoinCondition(joinCondition));

            case FULL:
                return NestedLoopBatchIterator.fullOuterJoin(left, right, getJoinCondition(joinCondition));

            case SEMI:
                return NestedLoopBatchIterator.semiJoin(left, right, getJoinCondition(joinCondition));
        }
        throw new AssertionError("Invalid joinType: " + joinType);
    }

    private static Function<Columns, BooleanSupplier> getJoinCondition(Predicate<Row> joinCondition) {
        return columns -> {
            final Row row = RowBridging.toRow(columns);
            return () -> joinCondition.test(row);
        };
    }

    public BatchConsumer leftConsumer() {
        return getBatchConsumer(leftBatchIterator, false);
    }

    public BatchConsumer rightConsumer() {
        return getBatchConsumer(rightBatchIterator, true);
    }

    private BatchConsumer getBatchConsumer(CompletableFuture<BatchIterator> future, boolean requiresRepeat) {
        return new BatchConsumer() {
            @Override
            public void accept(BatchIterator iterator, @Nullable Throwable failure) {
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
