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

import java.util.concurrent.CompletionStage;
import java.util.function.Predicate;

/**
 * <pre>
 *     for (leftRow in left) {
 *         for (rightRow in right) {
 *             match?
 *                  onRow
 *         }
 *         if (noRightRowMatched) {
 *              onRow // with right side null
 *         }
 *     }
 * </pre>
 */
class LeftJoinBatchIterator<L, R, C> extends NestedLoopBatchIterator<L, R, C> {

    private final Predicate<C> joinCondition;
    private boolean hadMatch = false;

    LeftJoinBatchIterator(BatchIterator<L> left,
                          BatchIterator<R> right,
                          ElementCombiner<L, R, C> combiner,
                          Predicate<C> joinCondition) {
        super(left, right, combiner);
        this.joinCondition = joinCondition;
    }

    @Override
    public boolean moveNext() {
        while (true) {
            if (activeIt == left) {
                return moveLeftSide();
            }

            Boolean x = tryAdvanceRight();
            if (x != null) {
                return x;
            }
            activeIt = left;
        }
    }

    private boolean moveLeftSide() {
        activeIt = right;
        while (tryMoveLeft()) {
            Boolean x = tryAdvanceRight();
            if (x != null) {
                return x;
            }
        }
        activeIt = left;
        return false;
    }

    @Override
    public CompletionStage<?> loadNextBatch() {
        return super.loadNextBatch();
    }

    /**
     * try to move the right side
     *
     * @return true  -> moved and matched
     * false -> need to load more data
     * null  -> reached it's end and moved back to start -> left side needs to continue
     */
    private Boolean tryAdvanceRight() {
        while (tryMoveRight()) {
            if (joinCondition.test(combiner.currentElement())) {
                hadMatch = true;
                return true;
            }
        }
        if (right.allLoaded() == false) {
            return false;
        }
        right.moveToStart();
        if (hadMatch == false) {
            activeIt = left;
            combiner.nullRight();
            return true;
        }
        hadMatch = false;
        return null;
    }
}
