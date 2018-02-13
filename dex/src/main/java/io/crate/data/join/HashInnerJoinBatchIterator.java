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

import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import io.crate.data.ArrayRow;
import io.crate.data.BatchIterator;
import io.crate.data.Row;

import java.util.Collection;
import java.util.Iterator;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * <pre>
 *     Build Phase:
 *     for (leftRow in left) {
 *         calculate hash and put in Buffer (HashMap)
 *     }
 *     // All rows from left processed and inserted in the Buffer
 *
 *     Probe Phase:
 *     // From now on we only iterate on the right
 *     for (rightRow in right) {
 *         if (hash(rightRow) found in Buffer {
 *            for (row in matchedInBuffer) { // Handle duplicate values from left and hash collisions
 *                if (joinCondition matches) {
 *                    // We need to check that the joinCondition matches as we can have a hash collision
 *                    // or the join condition can contain more operators.
 *                    //
 *                    // Row-lookup-by-hash-code can only work by the EQ operators of a join condition,
 *                    // all other possible operators must be checked afterwards.
 *                    emmit(combinedRow)
 *                }
 *            }
 *         }
 *     }
 * </pre>
 * <p>
 * The caller of the ctor needs to pass two functions {@link #hashBuilderForLeft} and {@link #hashBuilderForRight}.
 * Those functions are called on each row of the left and right side respectively and they return the hash value of
 * the relevant columns of the row.
 * <p>
 * This information is not available for the {@link HashInnerJoinBatchIterator}, so it's the responsibility of the
 * caller to provide those two functions that operate on the left and right rows accordingly and return the hash values.
 */
class HashInnerJoinBatchIterator<L extends Row, R extends Row, C> extends JoinBatchIterator<L, R, C> {

    private static int DEFAULT_BUFFER_SIZE = 10_000;
    private final Predicate<C> joinCondition;
    private final Multimap<Integer, Object[]> buffer;
    /**
     * Used to avoid instantiating multiple times RowN in {@link #findMatchingRows()}
     */
    private final ArrayRow leftRow = new ArrayRow();
    private final Function<L, Integer> hashBuilderForLeft;
    private final Function<R, Integer> hashBuilderForRight;
    private Iterator<Object[]> leftMatchingRowsIterator;

    HashInnerJoinBatchIterator(BatchIterator<L> left,
                               BatchIterator<R> right,
                               ElementCombiner<L, R, C> combiner,
                               Predicate<C> joinCondition,
                               Function<L, Integer> hashBuilderForLeft,
                               Function<R, Integer> hashBuilderForRight,
                               int leftSize) {
        super(left, right, combiner);
        this.joinCondition = joinCondition;
        this.hashBuilderForLeft = hashBuilderForLeft;
        this.hashBuilderForRight = hashBuilderForRight;
        if (leftSize <= 0) {
            this.buffer = LinkedListMultimap.create(DEFAULT_BUFFER_SIZE);
        } else {
            this.buffer = LinkedListMultimap.create(leftSize);
        }
        this.activeIt = left;
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
        buffer.clear();
        leftMatchingRowsIterator = null;
    }

    @Override
    public boolean moveNext() {
        // Build HashMap from left relation
        if (activeIt == left) {
            while (left.moveNext()) {
                Object[] currentRow = left.currentElement().materialize();
                buffer.put(hashBuilderForLeft.apply(left.currentElement()), currentRow);
            }
            if (left.allLoaded()) {
                activeIt = right;
            } else {
                return false;
            }
        }

        // In case of multiple matches on the left side (duplicate values or hash collisions)
        if (leftMatchingRowsIterator != null && findMatchingRows()) {
            return true;
        }
        leftMatchingRowsIterator = null;
        while (right.moveNext()) {
            int rightHash = hashBuilderForRight.apply(right.currentElement());
            Collection<Object[]> leftMatchingRows = buffer.get(rightHash);
            if (leftMatchingRows != null) {
                leftMatchingRowsIterator = leftMatchingRows.iterator();
                combiner.setRight(right.currentElement());
                if (findMatchingRows()) {
                    return true;
                }
            }
        }
        return false;
    }

    private boolean findMatchingRows() {
        while (leftMatchingRowsIterator.hasNext()) {
            leftRow.cells(leftMatchingRowsIterator.next());
            combiner.setLeft((L) leftRow);
            if (joinCondition.test(combiner.currentElement())) {
                return true;
            }
        }
        return false;
    }
}
