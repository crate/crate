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

package io.crate.execution.engine.join;

import java.util.ArrayList;
import java.util.List;
import java.util.function.LongToIntFunction;
import java.util.function.Predicate;
import java.util.function.ToIntFunction;

import org.elasticsearch.common.breaker.CircuitBreaker;

import io.crate.data.BatchIterator;
import io.crate.data.Row;
import io.crate.data.UnsafeArrayRow;
import io.crate.data.breaker.RowAccounting;
import io.crate.data.join.CombinedRow;
import io.crate.data.join.JoinBatchIterator;
import io.netty.util.collection.IntObjectHashMap;

/**
 * <pre>
 *     Build Phase:
 *     for (leftRow in left) {
 *         calculate hash and put in Buffer (HashMap) until the blockSize is reached
 *         // for left outer set a marker which indicates that a pair was found
 *         buffer.marker = false
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
 *                    // for left outer joins mark in the buffer that a match was found
 *                    matchedBuffer.matched = true
 *                }
 *            }
 *         }
 *     }
 *     // for left outer joins now iterate over the buffer and emit all non-marked values
 *     for (values in buffer) {
 *         if (matched == false) {
 *             emit(value, null)
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
 * This information is not available for the {@link HashJoinBatchIterator}, so it's the responsibility of the
 * caller to provide those two functions that operate on the left and right rows accordingly and return the hash values.
 */
public class HashJoinBatchIterator extends JoinBatchIterator<Row, Row, Row> {

    private final RowAccounting<Object[]> leftRowAccounting;
    private final Predicate<Row> joinCondition;

    /**
     * Used to avoid instantiating multiple times RowN in {@link #findMatchingRows()}
     */
    private final UnsafeArrayRow leftRow = new UnsafeArrayRow();
    private final CircuitBreaker circuitBreaker;
    private final ToIntFunction<Row> hashBuilderForLeft;
    private final ToIntFunction<Row> hashBuilderForRight;
    private final LongToIntFunction calculateBlockSize;
    private final IntObjectHashMap<HashGroup> buffer;
    private final boolean emitUnmatchedRows;

    private final UnsafeArrayRow unsafeArrayRow = new UnsafeArrayRow();

    private int leftAverageRowSize = -1;
    private int blockSize;
    private int numberOfHashGroupsInBuffer = 0;
    private boolean leftBatchHasItems = false;

    private HashGroup matchedHashGroup;
    private int matchedHashGroupIdx = 0;

    private final List<Object[]> unmatchedRows;
    private int unmatchedRowsIdx = 0;

    public HashJoinBatchIterator(CircuitBreaker circuitBreaker,
                                 BatchIterator<Row> left,
                                 BatchIterator<Row> right,
                                 RowAccounting<Object[]> leftRowAccounting,
                                 CombinedRow combiner,
                                 Predicate<Row> joinCondition,
                                 ToIntFunction<Row> hashBuilderForLeft,
                                 ToIntFunction<Row> hashBuilderForRight,
                                 LongToIntFunction calculateBlockSize,
                                 boolean emitUnmatchedRows) {
        super(left, right, combiner);
        this.circuitBreaker = circuitBreaker;
        this.leftRowAccounting = leftRowAccounting;
        this.joinCondition = joinCondition;
        this.hashBuilderForLeft = hashBuilderForLeft;
        this.hashBuilderForRight = hashBuilderForRight;
        this.calculateBlockSize = calculateBlockSize;
        // resized upon block size calculation
        this.buffer = new IntObjectHashMap<>();
        resetBuffer();
        this.activeIt = left;
        this.emitUnmatchedRows = emitUnmatchedRows;
        this.unmatchedRows = new ArrayList<>();
    }

    @Override
    public Row currentElement() {
        return combiner.currentElement();
    }

    @Override
    public void moveToStart() {
        left.moveToStart();
        right.moveToStart();
        activeIt = left;
        resetBuffer();
        matchedHashGroup = null;
        matchedHashGroupIdx = 0;
        unmatchedRows.clear();
        unmatchedRowsIdx = 0;
    }

    @Override
    public boolean moveNext() {
        while (buildBufferAndMatchRight() == false) {
            if (right.allLoaded() && leftBatchHasItems == false && left.allLoaded()) {
                // both sides are fully loaded
                if (emitUnmatchedRows) {
                    extractUnmatchedRows();
                    if (hasMoreUnmatchedKeys()) {
                        return emitUnmatchedRows();
                    }
                }
                // we are fully done
                return false;
            } else if (activeIt == left) {
                // left needs the next batch loaded
                return false;
            } else if (right.allLoaded()) {
                // one batch completed
                if (emitUnmatchedRows) {
                    extractUnmatchedRows();
                    if (hasMoreUnmatchedKeys()) {
                        return emitUnmatchedRows();
                    }
                }
                // get ready for the next batch
                right.moveToStart();
                activeIt = left;
                resetBuffer();
                matchedHashGroup = null;
                matchedHashGroupIdx = 0;
                unmatchedRows.clear();
                unmatchedRowsIdx = 0;
            } else {
                return false;
            }
        }

        // match found
        return true;
    }

    private void extractUnmatchedRows() {
        if (!unmatchedRows.isEmpty()) {
            return;
        }
        for (var bufferEntry : buffer.entries()) {
            HashGroup hashGroup = bufferEntry.value();
            List<Object[]> hashGroupRows = hashGroup.rows;
            List<Boolean> rowIsJoinedFlags = hashGroup.rowIsJoinedFlags;
            for (int i = 0; i < hashGroupRows.size(); i++) {
                if (rowIsJoinedFlags.get(i) == false) {
                    unmatchedRows.add(hashGroupRows.get(i));
                }
            }
        }
    }

    private boolean hasMoreUnmatchedKeys() {
        return unmatchedRowsIdx < unmatchedRows.size();
    }

    private boolean emitUnmatchedRows() {
        Object[] unmatchedRow = unmatchedRows.get(unmatchedRowsIdx);
        combiner.setLeft(unsafeArrayRow.cells(unmatchedRow));
        combiner.nullRight();
        unmatchedRowsIdx++;
        return true;
    }

    private void resetBuffer() {
        blockSize = calculateBlockSize.applyAsInt(leftAverageRowSize);
        buffer.clear();
        numberOfHashGroupsInBuffer = 0;
        leftRowAccounting.release();
    }

    private boolean buildBufferAndMatchRight() {
        if (activeIt == left) {
            // Build phase to create a block (hash map buffer) for the left side.
            //
            // It finishes and switches to probing the right side if:
            //
            // 1. Block size has been reached
            // 2. There is hardly any free memory left
            // 3. The current source "page" is exhausted (left.moveNext() return false)
            //
            // In case 3) we don't return false to trigger a left.loadNextBatch() and try to
            // fill up the block buffer further because we need to ensure that all nodes
            // executing join operations consume the same amount of pages per block: Always 1
            //
            // Otherwise it could create a deadlock scenario where one
            // downstream wants to fetch a page for the left side and another
            // downstream a page for the right side.
            //
            // Due to the upstreams always collecting data for _all_ downstreams, they also
            // wait for all downstreams to request new data.
            long numItems = 0;
            long sum = 0;
            while (leftBatchHasItems = left.moveNext()) {
                Object[] leftRow = left.currentElement().materialize();
                long leftRowSize = leftRowAccounting.accountForAndMaybeBreak(leftRow);
                sum += leftRowSize;
                numItems++;
                int hash = hashBuilderForLeft.applyAsInt(unsafeArrayRow.cells(leftRow));
                addToBuffer(leftRow, hash);
                if (numberOfHashGroupsInBuffer == blockSize || circuitBreaker.getFree() < 512 * 1024) {
                    break;
                }
            }
            leftAverageRowSize = numItems > 0 ? (int) (sum / numItems) : -1;

            if (numberOfHashGroupsInBuffer == 0 && !left.allLoaded()) {
                return false;
            } else {
                activeIt = right;
            }
        }

        // In case of multiple matches on the left side (duplicate values or hash collisions)
        if (matchedHashGroup != null && findMatchingRows()) {
            return true;
        }

        while (right.moveNext()) {
            int rightHash = hashBuilderForRight.applyAsInt(right.currentElement());
            matchedHashGroup = buffer.get(rightHash);
            if (matchedHashGroup != null) {
                matchedHashGroupIdx = 0;
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
        HashGroup hashGroup = buffer.get(hash);
        if (hashGroup == null) {
            hashGroup = new HashGroup();
            buffer.put(hash, hashGroup);
        }
        hashGroup.add(currentRow);
        numberOfHashGroupsInBuffer++;
    }

    private boolean findMatchingRows() {
        List<Object[]> matchedRows = matchedHashGroup.rows;
        List<Boolean> rowIsJoinedFlags = matchedHashGroup.rowIsJoinedFlags;
        for (; matchedHashGroupIdx < matchedRows.size(); matchedHashGroupIdx++) {
            leftRow.cells(matchedRows.get(matchedHashGroupIdx));
            combiner.setLeft(leftRow);
            if (joinCondition.test(combiner.currentElement())) {
                if (emitUnmatchedRows) {
                    rowIsJoinedFlags.set(matchedHashGroupIdx, true);
                }
                matchedHashGroupIdx++;
                return true;
            }
        }
        matchedHashGroup = null;
        matchedHashGroupIdx = 0;
        return false;
    }

    private static final class HashGroup {

        private final List<Object[]> rows = new ArrayList<>();
        private final List<Boolean> rowIsJoinedFlags = new ArrayList<>();

        public void add(Object[] row) {
            rows.add(row);
            rowIsJoinedFlags.add(Boolean.FALSE);
        }
    }


}
