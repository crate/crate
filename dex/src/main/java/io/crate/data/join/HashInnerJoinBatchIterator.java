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

import com.carrotsearch.hppc.IntObjectHashMap;
import io.crate.data.ArrayRow;
import io.crate.data.BatchIterator;
import io.crate.data.Paging;
import io.crate.data.Row;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * <pre>
 *     Build Phase:
 *     for (leftRow in left) {
 *         calculate hash and put in Buffer (HashMap) until the blockSize is reached
 *     }
 *
 *     Probe Phase:
 *     // We iterate on the right until we find a matching row or the right side needs to be loaded a next batch of data
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
 *
 *     When the right side is all loaded, we reset the right iterator to start, clear the buffer and switch back to
 *     iterate the next elements in the left side and re-build the buffer based on the next items in the left until we
 *     reach the blockSize again.
 *
 *     Repeat until both sides are all loaded and processed.
 * </pre>
 * <p>
 * The caller of the constructor needs to pass two functions {@link #hashBuilderForLeft} and {@link #hashBuilderForRight}.
 * Those functions are called on each row of the left and right side respectively and they return the hash value of
 * the relevant columns of the row.
 * <p>
 * This information is not available for the {@link HashInnerJoinBatchIterator}, so it's the responsibility of the
 * caller to provide those two functions that operate on the left and right rows accordingly and return the hash values.
 */
public class HashInnerJoinBatchIterator<L extends Row, R extends Row, C> extends JoinBatchIterator<L, R, C> {

    private static final int DEFAULT_BLOCK_SIZE = Paging.PAGE_SIZE;

    private final Predicate<C> joinCondition;
    private final IntObjectHashMap<List<Object[]>> buffer;

    /**
     * Used to avoid instantiating multiple times RowN in {@link #findMatchingRows()}
     */
    private final ArrayRow leftRow = new ArrayRow();
    private final Function<L, Integer> hashBuilderForLeft;
    private final Function<R, Integer> hashBuilderForRight;
    private final int blockSize;
    private Iterator<Object[]> leftMatchingRowsIterator;

    HashInnerJoinBatchIterator(BatchIterator<L> left,
                               BatchIterator<R> right,
                               ElementCombiner<L, R, C> combiner,
                               Predicate<C> joinCondition,
                               Function<L, Integer> hashBuilderForLeft,
                               Function<R, Integer> hashBuilderForRight,
                               int blockSize) {
        super(left, right, combiner);
        this.joinCondition = joinCondition;
        this.hashBuilderForLeft = hashBuilderForLeft;
        this.hashBuilderForRight = hashBuilderForRight;
        if (blockSize > 0) {
            this.blockSize = blockSize;
        } else {
            this.blockSize = DEFAULT_BLOCK_SIZE;
        }
        this.buffer = new IntObjectHashMap<>(this.blockSize);
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
        while (buildBufferAndMatchRight() == false) {
            if (right.allLoaded() && left.allLoaded()) {
                // both sides are fully loaded, we're done here
                return false;
            } else if (right.allLoaded()) {
                right.moveToStart();
                activeIt = left;
                buffer.clear();
            } else {
                // activeIt needs the next batch loaded
                return false;
            }
        }

        // match found
        return true;
    }

    private boolean buildBufferAndMatchRight() {
        if (activeIt == left) {
            while (left.moveNext()) {
                Object[] currentRow = left.currentElement().materialize();
                int hash = hashBuilderForLeft.apply(left.currentElement());
                addToBuffer(currentRow, hash);
            }
            if (buffer.size() == blockSize || left.allLoaded()) {
                activeIt = right;
            } else {
                // need to load the next batch of the left relation
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
            List<Object[]> leftMatchingRows = buffer.get(rightHash);
            if (leftMatchingRows != null) {
                leftMatchingRowsIterator = leftMatchingRows.iterator();
                combiner.setRight(right.currentElement());
                if (findMatchingRows()) {
                    return true;
                }
            }
        }

        // need to load the next batch of the right relation
        return false;
    }

    private void addToBuffer(Object[] currentRow, int hash) {
        List<Object[]> existingRows = buffer.get(hash);
        if (existingRows == null) {
            existingRows = new ArrayList<>();
            buffer.put(hash, existingRows);
        }
        existingRows.add(currentRow);
    }

    @SuppressWarnings("unchecked")
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
