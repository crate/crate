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


import io.crate.data.BatchIterator;

/**
 * This BatchIterator is used for both CrossJoins and InnerJoins as for the InnerJoins
 * the joinCondition is tested later on as a filter Projection.
 *
 * <pre>
 *     for (leftRow in left) {
 *         for (rightRow in right) {
 *             match?
 *                  onRow
 *         }
 *     }
 * </pre>
 */
public class CrossJoinNLBatchIterator<L, R, C> extends JoinBatchIterator<L, R, C> {

    public CrossJoinNLBatchIterator(BatchIterator<L> left,
                                    BatchIterator<R> right,
                                    ElementCombiner<L, R, C> combiner) {
        super(left, right, combiner);
    }

    @Override
    public boolean moveNext() {
        if (activeIt == left) {
            return tryAdvanceLeftAndRight();
        } else {
            return tryAdvanceRight();
        }
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
        // left is either out of rows or needs to load more data - whatever the case,
        // next moveNext must operate on left
        activeIt = left;
        return false;
    }
}
