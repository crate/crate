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

package io.crate.data.join;

import java.util.concurrent.CompletionStage;
import java.util.function.Predicate;

import org.jetbrains.annotations.NotNull;

import io.crate.data.BatchIterator;

/**
 * <pre>
 *     for (leftRow in left) {
 *         for (rightRow in right) {
 *             match?
 *                  onRow;
 *                  break;
 *         }
 *     }
 * </pre>
 */
public class SemiJoinNLBatchIterator<L, R, C> implements BatchIterator<L> {

    final BatchIterator<L> left;
    final BatchIterator<R> right;
    final ElementCombiner<L, R, C> combiner;
    final Predicate<C> joinCondition;

    BatchIterator<?> activeIt;

    public SemiJoinNLBatchIterator(BatchIterator<L> left,
                                   BatchIterator<R> right,
                                   ElementCombiner<L, R, C> combiner,
                                   Predicate<C> joinCondition) {
        this.left = left;
        this.right = right;
        this.combiner = combiner;
        this.joinCondition = joinCondition;
        this.activeIt = left;
    }

    @Override
    public L currentElement() {
        return left.currentElement();
    }

    @Override
    public void moveToStart() {
        left.moveToStart();
        right.moveToStart();
        activeIt = left;
    }

    @Override
    public boolean moveNext() {
        while (true) {
            if (activeIt == left) {
                return moveLeftSide();
            }

            Boolean rightAdvanced = tryAdvanceRight();
            if (rightAdvanced != null) {
                return rightAdvanced;
            }
        }
    }

    @Override
    public void close() {
        left.close();
        right.close();
    }

    @Override
    public CompletionStage<?> loadNextBatch() throws Exception {
        return activeIt.loadNextBatch();
    }

    @Override
    public boolean allLoaded() {
        return activeIt.allLoaded();
    }

    private boolean moveLeftSide() {
        while (tryMoveLeft()) {
            activeIt = right;
            Boolean rightAdvanced = tryAdvanceRight();
            if (rightAdvanced != null) {
                return rightAdvanced;
            }
        }
        activeIt = left;
        return false;
    }

    /**
     * try to move the right side
     *
     * @return true  -> matched -> move right to start, move left to next
     * false -> need to load more data
     * null  -> reached it's end and moved back to start -> left side needs to continue
     */
    protected Boolean tryAdvanceRight() {
        while (tryMoveRight()) {
            if (joinCondition.test(combiner.currentElement())) {
                right.moveToStart();
                combiner.setRight(right.currentElement());
                activeIt = left;
                return true;
            }
        }
        if (right.allLoaded() == false) {
            return false;
        }
        right.moveToStart();
        combiner.setRight(right.currentElement());
        activeIt = left;
        return null;
    }

    @Override
    public void kill(@NotNull Throwable throwable) {
        left.kill(throwable);
        right.kill(throwable);
    }

    @Override
    public boolean isKilled() {
        return left.isKilled() || right.isKilled();
    }

    private boolean tryMoveLeft() {
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

    @Override
    public boolean hasLazyResultSet() {
        return false;
    }
}
