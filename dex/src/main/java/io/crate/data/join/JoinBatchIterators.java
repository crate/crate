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

import java.util.function.Predicate;

/**
 * BatchIterator implementations for Joins
 * <ul>
 *   <li>{@link #crossJoin(BatchIterator, BatchIterator, ElementCombiner)}</li>
 *   <li>{@link #leftJoin(BatchIterator, BatchIterator, ElementCombiner, Predicate)}</li>
 *   <li>{@link #rightJoin(BatchIterator, BatchIterator, ElementCombiner, Predicate)}</li>
 *   <li>{@link #fullOuterJoin(BatchIterator, BatchIterator, ElementCombiner, Predicate)}</li>
 *   <li>{@link #semiJoin(BatchIterator, BatchIterator, ElementCombiner, Predicate)}</li>
 *   <li>{@link #antiJoin(BatchIterator, BatchIterator, ElementCombiner, Predicate)}</li>
 * </ul>
 */
public final class JoinBatchIterators {

    private JoinBatchIterators() {
    }

    /**
     * Create a BatchIterator that creates a full-outer-join of {@code left} and {@code right}.
     */
    public static <L, R, C> BatchIterator<C> fullOuterJoin(BatchIterator<L> left,
                                                           BatchIterator<R> right,
                                                           ElementCombiner<L, R, C> combiner,
                                                           Predicate<C> joinCondition) {
        return new FullOuterJoinNLBatchIterator<>(left, right, combiner, joinCondition);
    }

    /**
     * Create a BatchIterator that creates a cross-join of {@code left} and {@code right}.
     */
    public static <L, R, C> BatchIterator<C> crossJoin(BatchIterator<L> left,
                                                       BatchIterator<R> right,
                                                       ElementCombiner<L, R, C> combiner) {
        return new CrossJoinNLBatchIterator<>(left, right, combiner);
    }

    /**
     * Create a BatchIterator that creates the left-outer-join result of {@code left} and {@code right}.
     */
    public static <L, R, C> BatchIterator<C> leftJoin(BatchIterator<L> left,
                                                      BatchIterator<R> right,
                                                      ElementCombiner<L, R, C> combiner,
                                                      Predicate<C> joinCondition) {
        return new LeftJoinNLBatchIterator<>(left, right, combiner, joinCondition);
    }

    /**
     * Create a BatchIterator that creates the right-outer-join result of {@code left} and {@code right}.
     */
    public static <L, R, C> BatchIterator<C> rightJoin(BatchIterator<L> left,
                                                       BatchIterator<R> right,
                                                       ElementCombiner<L, R, C> combiner,
                                                       Predicate<C> joinCondition) {
        return new RightJoinNLBatchIterator<>(left, right, combiner, joinCondition);
    }

    /**
     * Create a BatchIterator that creates the semi-join result of {@code left} and {@code right}.
     */
    public static <L, R, C> BatchIterator<L> semiJoin(BatchIterator<L> left,
                                                      BatchIterator<R> right,
                                                      ElementCombiner<L, R, C> combiner,
                                                      Predicate<C> joinCondition) {
        return new SemiJoinNLBatchIterator<>(left, right, combiner, joinCondition);
    }

    /**
     * Create a BatchIterator that creates the anti-join result of {@code left} and {@code right}.
     */
    public static <L, R, C> BatchIterator<L> antiJoin(BatchIterator<L> left,
                                                      BatchIterator<R> right,
                                                      ElementCombiner<L, R, C> combiner,
                                                      Predicate<C> joinCondition) {
        return new AntiJoinNLBatchIterator<>(left, right, combiner, joinCondition);
    }
}
