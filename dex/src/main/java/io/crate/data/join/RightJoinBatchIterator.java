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

import java.util.function.BooleanSupplier;
import java.util.function.Function;

/**
 * Nested Loop + additional loop afterwards to emit any rows that had no matches
 *
 * <pre>
 *     for (leftRow in left) {
 *         for (rightRow in right) {
 *             if matched {
 *                 markPosition(pos)
 *                 onRow
 *             }
 *         }
 *     }
 *
 *     for (rightRow in right) {
 *         if (noMatch(position)) {
 *             onRow (left-side-null)
 *         }
 *     }
 * </pre>
 */
class RightJoinBatchIterator extends NestedLoopBatchIterator {

    private final LuceneLongBitSetWrapper matchedRows = new LuceneLongBitSetWrapper();
    private final BooleanSupplier joinCondition;

    private boolean postNL = false;
    private int position = -1;

    RightJoinBatchIterator(BatchIterator left, BatchIterator right, Function<Columns, BooleanSupplier> joinCondition) {
        super(left, right);
        this.joinCondition = joinCondition.apply(rowData());
    }

    @Override
    public void moveToStart() {
        super.moveToStart();
        rowData.resetLeft();
        activeIt = left;
        postNL = false;
        position = -1;
    }

    @Override
    public boolean moveNext() {
        if (postNL) {
            return moveRightPostNL();
        }
        while (true) {
            if (activeIt == left) {
                return moveLeft();
            }
            Boolean x = tryAdvanceRight();
            if (x != null) {
                return x;
            }
            activeIt = left;
        }
    }

    private boolean moveLeft() {
        while (left.moveNext()) {
            activeIt = right;
            Boolean x = tryAdvanceRight();
            if (x != null) {
                return x;
            }
        }
        activeIt = left;
        postNL = left.allLoaded();
        if (postNL) {
            position = -1;
            activeIt = right;
            rowData.nullLeft();
            return moveRightPostNL();
        }
        return false;
    }

    /**
     * @return true  -> right moved
     *         false -> need to load more data
     *         null  -> reached its end, need to continue on left
     */
    private Boolean tryAdvanceRight() {
        while (right.moveNext()) {
            position++;
            if (joinCondition.getAsBoolean()) {
                matchedRows.set(position);
                return true;
            }
        }
        if (right.allLoaded() == false) {
            return false;
        }
        position = -1;
        right.moveToStart();
        return null;
    }

    private boolean moveRightPostNL() {
        while (right.moveNext()) {
            position++;
            if (matchedRows.get(position) == false) {
                return true;
            }
        }
        return false;
    }
}
