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

import com.google.common.annotations.VisibleForTesting;
import io.crate.breaker.RamAccountingContext;
import io.crate.breaker.RowAccountingWithEstimators;
import io.crate.concurrent.CompletionListenable;
import io.crate.data.BatchIterator;
import io.crate.data.CapturingRowConsumer;
import io.crate.data.FilteringBatchIterator;
import io.crate.data.Paging;
import io.crate.data.Row;
import io.crate.data.RowConsumer;
import io.crate.data.join.CombinedRow;
import io.crate.data.join.JoinBatchIterators;
import io.crate.planner.node.dql.join.JoinType;
import io.crate.types.DataType;
import org.elasticsearch.common.breaker.CircuitBreaker;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.IntSupplier;
import java.util.function.Predicate;


public class NestedLoopOperation implements CompletionListenable {

    private final CapturingRowConsumer leftConsumer;
    private final CapturingRowConsumer rightConsumer;
    private final RowConsumer resultConsumer;

    public NestedLoopOperation(int numLeftCols,
                               int numRightCols,
                               RowConsumer nlResultConsumer,
                               Predicate<Row> joinPredicate,
                               JoinType joinType,
                               CircuitBreaker circuitBreaker,
                               RamAccountingContext ramAccountingContext,
                               List<DataType> leftSideColumnTypes,
                               long estimatedRowsSizeLeft,
                               long estimatedNumberOfRowsLeft,
                               boolean blockNestedLoop) {
        this.resultConsumer = nlResultConsumer;
        this.leftConsumer = new CapturingRowConsumer(false, nlResultConsumer.completionFuture());
        this.rightConsumer = new CapturingRowConsumer(true, nlResultConsumer.completionFuture());
        CompletableFuture.allOf(leftConsumer.capturedBatchIterator(), rightConsumer.capturedBatchIterator())
            .whenComplete((result, failure) -> {
                if (failure == null) {
                    BatchIterator<Row> nlIterator = createNestedLoopIterator(
                        leftConsumer.capturedBatchIterator().join(),
                        numLeftCols,
                        rightConsumer.capturedBatchIterator().join(),
                        numRightCols,
                        joinType,
                        joinPredicate,
                        circuitBreaker,
                        ramAccountingContext,
                        leftSideColumnTypes,
                        estimatedRowsSizeLeft,
                        estimatedNumberOfRowsLeft,
                        blockNestedLoop
                    );
                    nlResultConsumer.accept(nlIterator, null);
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

    @VisibleForTesting
    static BatchIterator<Row> createNestedLoopIterator(BatchIterator<Row> left,
                                                       int leftNumCols,
                                                       BatchIterator<Row> right,
                                                       int rightNumCols,
                                                       JoinType joinType,
                                                       Predicate<Row> joinCondition,
                                                       CircuitBreaker circuitBreaker,
                                                       RamAccountingContext ramAccountingContext,
                                                       List<DataType> leftSideColumnTypes,
                                                       long estimatedRowsSizeLeft,
                                                       long estimatedNumberOfRowsLeft,
                                                       boolean blockNestedLoop) {
        final CombinedRow combiner = new CombinedRow(leftNumCols, rightNumCols);
        switch (joinType) {
            case CROSS:
                return buildCrossJoinBatchIterator(left, right, combiner,
                    circuitBreaker, ramAccountingContext, leftSideColumnTypes,
                    estimatedRowsSizeLeft, estimatedNumberOfRowsLeft, blockNestedLoop);

            case INNER:
                return new FilteringBatchIterator<>(
                    buildCrossJoinBatchIterator(left, right, combiner,
                        circuitBreaker, ramAccountingContext, leftSideColumnTypes,
                        estimatedRowsSizeLeft, estimatedNumberOfRowsLeft, blockNestedLoop),
                    joinCondition);

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

    private static BatchIterator<Row> buildCrossJoinBatchIterator(BatchIterator<Row> left,
                                                                  BatchIterator<Row> right,
                                                                  CombinedRow combiner,
                                                                  CircuitBreaker circuitBreaker,
                                                                  RamAccountingContext ramAccountingContext,
                                                                  List<DataType> leftSideColumnTypes,
                                                                  long estimatedRowsSizeLeft,
                                                                  long estimatedNumberOfRowsLeft,
                                                                  boolean blockNestedLoop) {
        if (blockNestedLoop) {
            IntSupplier blockSizeCalculator = new RamBlockSizeCalculator(
                Paging.PAGE_SIZE, circuitBreaker, estimatedRowsSizeLeft, estimatedNumberOfRowsLeft);
            RowAccountingWithEstimators rowAccounting = new RowAccountingWithEstimators(leftSideColumnTypes, ramAccountingContext);
            return JoinBatchIterators.crossJoinBlockNL(left, right, combiner, blockSizeCalculator, rowAccounting);
        } else {
            return JoinBatchIterators.crossJoinNL(left, right, combiner);
        }
    }
}
