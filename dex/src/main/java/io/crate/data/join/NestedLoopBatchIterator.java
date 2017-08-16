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

package io.crate.data.join;

import io.crate.data.BatchIterator;
import io.crate.data.Columns;

import javax.annotation.Nonnull;
import java.util.concurrent.CompletionStage;
import java.util.function.BooleanSupplier;
import java.util.function.Function;

/**
 * NestedLoop BatchIterator implementations
 *
 * - {@link #crossJoin(BatchIterator, BatchIterator)}
 * - {@link #leftJoin(BatchIterator, BatchIterator, Function)}
 * - {@link #rightJoin(BatchIterator, BatchIterator, Function)}
 * - {@link #fullOuterJoin(BatchIterator, BatchIterator, Function)}
 * - {@link #semiJoin(BatchIterator, BatchIterator, Function)}
 */
public class NestedLoopBatchIterator implements BatchIterator {


    /**
     * Create a BatchIterator that creates a full-outer-join of {@code left} and {@code right}.
     */
    public static BatchIterator fullOuterJoin(BatchIterator left,
                                              BatchIterator right,
                                              Function<Columns, BooleanSupplier> joinCondition) {
        return new FullOuterJoinBatchIterator(left, right, joinCondition);
    }

    /**
     * Create a BatchIterator that creates a cross-join of {@code left} and {@code right}.
     */
    public static BatchIterator crossJoin(BatchIterator left, BatchIterator right) {
        return new NestedLoopBatchIterator(left, right);
    }

    /**
     * Create a BatchIterator that creates the left-outer-join result of {@code left} and {@code right}.
     */
    public static BatchIterator leftJoin(BatchIterator left,
                                         BatchIterator right,
                                         Function<Columns, BooleanSupplier> joinCondition) {
        return new LeftJoinBatchIterator(left, right, joinCondition);
    }

    /**
     * Create a BatchIterator that creates the right-outer-join result of {@code left} and {@code right}.
     */
    public static BatchIterator rightJoin(BatchIterator left,
                                          BatchIterator right,
                                          Function<Columns, BooleanSupplier> joinCondition) {
        return new RightJoinBatchIterator(left, right, joinCondition);
    }

    /**
     * Create a BatchIterator that creates the semi-join result of {@code left} and {@code right}.
     */
    public static BatchIterator semiJoin(BatchIterator left,
                                         BatchIterator right,
                                         Function<Columns, BooleanSupplier> joinCondition) {
        return new SemiJoinBatchIterator(left, right, joinCondition);
    }

    final CombinedColumn rowData;
    final BatchIterator left;
    final BatchIterator right;

    /**
     * points to the batchIterator which will be used on the next {@link #moveNext()} call
     */
    BatchIterator activeIt;

    NestedLoopBatchIterator(BatchIterator left, BatchIterator right) {
        this.left = left;
        this.right = right;
        this.activeIt = left;
        this.rowData = new CombinedColumn(left.rowData(), right.rowData());
    }

    @Override
    public Columns rowData() {
        return rowData;
    }

    @Override
    public void moveToStart() {
        left.moveToStart();
        right.moveToStart();
        activeIt = left;
    }

    @Override
    public boolean moveNext() {
        if (activeIt == left) {
            return tryAdvanceLeftAndRight();
        } else {
            return tryAdvanceRight();
        }
    }

    private boolean tryAdvanceRight() {
        if (right.moveNext()) {
            return true;
        }
        if (right.allLoaded() == false) {
            return false;
        }

        // new outer loop iteration
        right.moveToStart();
        if (left.moveNext()) {
            return right.moveNext();
        }
        activeIt = left; // left is either out of rows or needs to load more data - whatever the case,
                         // next moveNext must operate on left
        return false;
    }

    private boolean tryAdvanceLeftAndRight() {
        while (left.moveNext()) {
            activeIt = right;
            if (right.moveNext()) {
                return true;
            }
            if (right.allLoaded() == false) {
                return false;
            }
            right.moveToStart();
        }
        return false;
    }

    @Override
    public void close() {
        left.close();
        right.close();
    }

    @Override
    public CompletionStage<?> loadNextBatch() {
        return activeIt.loadNextBatch();
    }

    @Override
    public boolean allLoaded() {
        return activeIt.allLoaded();
    }


    @Override
    public void kill(@Nonnull Throwable throwable) {
        left.kill(throwable);
        right.kill(throwable);
    }
}
