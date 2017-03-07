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

import java.util.concurrent.CompletionStage;
import java.util.function.BooleanSupplier;
import java.util.function.Function;

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
class LeftJoinBatchIterator extends NestedLoopBatchIterator {

    private final BooleanSupplier joinCondition;
    private boolean hadMatch = false;
    private boolean loading = false;

    LeftJoinBatchIterator(BatchIterator left,
                          BatchIterator right,
                          Function<Columns, BooleanSupplier> joinCondition) {
        super(left, right);
        this.joinCondition = joinCondition.apply(rowData());
    }

    @Override
    public boolean moveNext() {
        if (loading) {
            throw new IllegalStateException("BatchIterator is loading");
        }
        while (true) {
            rowData.resetRight();
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
        while (left.moveNext()) {
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
        loading = true;
        return super.loadNextBatch().whenComplete((result, failure) -> loading = false);
    }

    /**
     * try to move the right side
     * @return true  -> moved and matched
     *         false -> need to load more data
     *         null  -> reached it's end and moved back to start -> left side needs to continue
     */
    private Boolean tryAdvanceRight() {
        while (right.moveNext()) {
            if (joinCondition.getAsBoolean()) {
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
            rowData.nullRight();
            return true;
        }
        hadMatch = false;
        return null;
    }
}
