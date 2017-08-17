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

import javax.annotation.Nonnull;
import java.util.concurrent.CompletionStage;
import java.util.function.Predicate;

/**
 * NestedLoop BatchIterator implementations
 *
 * - {@link #crossJoin(BatchIterator, BatchIterator, ElementCombiner)}
 * - {@link #leftJoin(BatchIterator, BatchIterator, ElementCombiner, Predicate)}
 * - {@link #rightJoin(BatchIterator, BatchIterator, ElementCombiner, Predicate)}
 * - {@link #fullOuterJoin(BatchIterator, BatchIterator, ElementCombiner, Predicate)}
 */
public class NestedLoopBatchIterator<L, R, C> implements BatchIterator<C> {


    /**
     * Create a BatchIterator that creates a full-outer-join of {@code left} and {@code right}.
     */
    public static <L, R, C> BatchIterator<C> fullOuterJoin(BatchIterator<L> left,
                                                           BatchIterator<R> right,
                                                           ElementCombiner<L, R, C> combiner,
                                                           Predicate<C> joinCondition) {
        return new FullOuterJoinBatchIterator<>(left, right, combiner, joinCondition);
    }

    /**
     * Create a BatchIterator that creates a cross-join of {@code left} and {@code right}.
     */
    public static <L, R, C> BatchIterator<C> crossJoin(BatchIterator<L> left,
                                                       BatchIterator<R> right,
                                                       ElementCombiner<L, R, C> combiner) {
        return new NestedLoopBatchIterator<>(left, right, combiner);
    }

    /**
     * Create a BatchIterator that creates the left-outer-join result of {@code left} and {@code right}.
     */
    public static <L, R, C> BatchIterator<C> leftJoin(BatchIterator<L> left,
                                                      BatchIterator<R> right,
                                                      ElementCombiner<L, R, C> combiner,
                                                      Predicate<C> joinCondition) {
        return new LeftJoinBatchIterator<>(left, right, combiner, joinCondition);
    }

    /**
     * Create a BatchIterator that creates the right-outer-join result of {@code left} and {@code right}.
     */
    public static <L, R, C> BatchIterator<C> rightJoin(BatchIterator<L> left,
                                                       BatchIterator<R> right,
                                                       ElementCombiner<L, R, C> combiner,
                                                       Predicate<C> joinCondition) {
        return new RightJoinBatchIterator<>(left, right, combiner, joinCondition);
    }


    final ElementCombiner<L, R, C> combiner;
    final BatchIterator<L> left;
    final BatchIterator<R> right;

    /**
     * points to the batchIterator which will be used on the next {@link #moveNext()} call
     */
    BatchIterator activeIt;

    NestedLoopBatchIterator(BatchIterator<L> left, BatchIterator<R> right, ElementCombiner<L, R, C> combiner) {
        this.left = left;
        this.right = right;
        this.activeIt = left;
        this.combiner = combiner;
    }

    @Override
    public C currentElement() {
        return combiner.currentElement();
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
        if (tryMoveRight()) {
            return true;
        }
        if (right.allLoaded() == false) {
            return false;
        }

        // new outer loop iteration
        right.moveToStart();
        if (tryMoveLeft()) {
            return tryMoveRight();
        }
        activeIt = left; // left is either out of rows or needs to load more data - whatever the case,
                         // next moveNext must operate on left
        return false;
    }

    boolean tryMoveLeft() {
        if (left.moveNext()) {
            combiner.setLeft(left.currentElement());
            return true;
        }
        return false;
    }

    boolean tryMoveRight() {
        if (right.moveNext()) {
            combiner.setRight(right.currentElement());
            return true;
        }
        return false;
    }

    private boolean tryAdvanceLeftAndRight() {
        while (tryMoveLeft()) {
            activeIt = right;
            if (tryMoveRight()) {
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
