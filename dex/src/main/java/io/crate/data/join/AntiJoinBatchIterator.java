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
 * <pre>
 *     for (leftRow in left) {
 *         for (rightRow in right) {
 *             match?
 *                  break;
 *         }
 *         onRow;
 *     }
 * </pre>
 */
class AntiJoinBatchIterator extends SemiJoinBatchIterator {

    AntiJoinBatchIterator(BatchIterator left,
                          BatchIterator right,
                          Function<Columns, BooleanSupplier> joinCondition) {
        super(left, right, joinCondition);
    }

    /**
     * try to move the right side
     *
     * @return true  -> not matched -> move right to start, move left emits and moves to next
     * false -> need to load more data
     * null  -> matched -> move right to start and left side needs to continue without emitting
     */
    protected Boolean tryAdvanceRight() {
        while (right.moveNext()) {
            if (joinCondition.getAsBoolean()) {
                right.moveToStart();
                activeIt = left;
                return null;
            }
        }
        if (right.allLoaded() == false) {
            return false;
        }
        right.moveToStart();
        activeIt = left;
        return true;
    }
}
