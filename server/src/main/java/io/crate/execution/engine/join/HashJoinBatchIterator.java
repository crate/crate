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
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.function.LongToIntFunction;
import java.util.function.Predicate;
import java.util.function.ToIntFunction;

import io.crate.common.collections.Tuple;
import io.crate.data.BatchIterator;
import io.crate.data.Paging;
import io.crate.data.Row;
import io.crate.data.RowN;
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
 *                    matchedBuffer.marker = true
 *                }
 *            }
 *         }
 *     }
 *     // for left outer joins now iterate over the buffer and emit all non-marked values
 *     for (values in buffer) {
 *         if (marked == false) {
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
    private final ToIntFunction<Row> hashBuilderForLeft;
    private final ToIntFunction<Row> hashBuilderForRight;
    private final LongToIntFunction calculateBlockSize;
    private final IntObjectHashMap<Tuple<List<Object[]>, Boolean>> buffer;
    private final boolean emitNullValues;

    private final UnsafeArrayRow unsafeArrayRow = new UnsafeArrayRow();

    private int leftAverageRowSize = -1;
    private int blockSize;
    private int numberOfRowsInBuffer = 0;
    private boolean leftBatchHasItems = false;
    private int numberOfLeftBatchesForBlock;
    private int numberOfLeftBatchesLoadedForBlock;
    private Iterator<Object[]> leftMatchingRowsIterator;
    private ArrayList<Integer> nonMatchingKeys;

    public HashJoinBatchIterator(BatchIterator<Row> left,
                                 BatchIterator<Row> right,
                                 RowAccounting<Object[]> leftRowAccounting,
                                 CombinedRow combiner,
                                 Predicate<Row> joinCondition,
                                 ToIntFunction<Row> hashBuilderForLeft,
                                 ToIntFunction<Row> hashBuilderForRight,
                                 LongToIntFunction calculateBlockSize,
                                 boolean emitNullValues) {
        super(left, right, combiner);
        this.leftRowAccounting = leftRowAccounting;
        this.joinCondition = joinCondition;
        this.hashBuilderForLeft = hashBuilderForLeft;
        this.hashBuilderForRight = hashBuilderForRight;
        this.calculateBlockSize = calculateBlockSize;
        // resized upon block size calculation
        this.buffer = new IntObjectHashMap<>();
        resetBuffer();
        numberOfLeftBatchesLoadedForBlock = 0;
        this.activeIt = left;
        this.emitNullValues = emitNullValues;
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
        leftMatchingRowsIterator = null;
        nonMatchingKeys = null;
    }

    @Override
    public CompletionStage<?> loadNextBatch() throws Exception {
        if (activeIt == left) {
            numberOfLeftBatchesLoadedForBlock++;
        }
        return super.loadNextBatch();
    }

    @Override
    public boolean moveNext() {
        while (buildBufferAndMatchRight() == false) {
            if (right.allLoaded() && leftBatchHasItems == false && left.allLoaded()) {
                // both sides are fully loaded
                if (emitNullValues) {
                    fetchKeysFromBuffer();
                    if (!nonMatchingKeys.isEmpty()) {
                        return emitNullValuesPairs();
                    }
                }
                // we are fully done
                return false;
            } else if (activeIt == left) {
                // left needs the next batch loaded
                return false;
            } else if (right.allLoaded()) {
                // one batch completed
                if (emitNullValues) {
                    fetchKeysFromBuffer();
                    if (!nonMatchingKeys.isEmpty()) {
                        return emitNullValuesPairs();
                    }
                }
                // get ready for the next batch
                right.moveToStart();
                activeIt = left;
                resetBuffer();
                nonMatchingKeys = null;
            } else {
                return false;
            }
        }

        // match found
        return true;
    }

    private void fetchKeysFromBuffer() {
        if (nonMatchingKeys == null) {
            nonMatchingKeys = new ArrayList<>(buffer.size());
            for (var values : buffer.entrySet()) {
                if (values.getValue().v2() == false) {
                    nonMatchingKeys.add(values.getKey());
                }
            }
        }
    }

    private boolean emitNullValuesPairs() {
        Integer key = nonMatchingKeys.getFirst();
        List<Object[]> values = buffer.remove(key).v1();
        Object[] nonMatches = values.removeFirst();

        combiner.setLeft(new RowN(nonMatches));
        combiner.setRight(new RowN(new Object[nonMatches.length]));

        // This requires rehashing and we might be better off with
        // storing the current nonMatches in an array
        // and consume it from there
        if (!values.isEmpty()) {
            buffer.put(key, new Tuple<>(values, false));
        } else {
            nonMatchingKeys.removeFirst();
        }
        return true;
    }

    private void resetBuffer() {
        blockSize = calculateBlockSize.applyAsInt(leftAverageRowSize);
        buffer.clear();
        numberOfRowsInBuffer = 0;
        leftRowAccounting.release();

        // A batch is not guaranteed to deliver PAGE_SIZE number of rows. It could be more or less.
        // So we cannot rely on that to decide if processing 1 block is done, we must also know and track how much
        // batches should be required for processing 1 block.
        numberOfLeftBatchesForBlock = Math.max(1, (int) Math.ceil((double) blockSize / Paging.PAGE_SIZE));
        numberOfLeftBatchesLoadedForBlock = leftBatchHasItems ? 1 : 0;
    }

    private boolean buildBufferAndMatchRight() {
        if (activeIt == left) {
            long numItems = 0;
            long sum = 0;
            while (leftBatchHasItems = left.moveNext()) {
                Object[] leftRow = left.currentElement().materialize();
                long leftRowSize = leftRowAccounting.accountForAndMaybeBreak(leftRow);
                sum += leftRowSize;
                numItems++;
                int hash = hashBuilderForLeft.applyAsInt(unsafeArrayRow.cells(leftRow));
                addToBuffer(leftRow, hash);
                if (numberOfRowsInBuffer == blockSize) {
                    break;
                }
            }
            leftAverageRowSize = numItems > 0 ? (int) (sum / numItems) : -1;

            if (mustLoadLeftNextBatch()) {
                // we should load the left side
                return false;
            }

            if (mustSwitchToRight()) {
                activeIt = right;
            }
        }

        // In case of multiple matches on the left side (duplicate values or hash collisions)
        if (leftMatchingRowsIterator != null && findMatchingRows()) {
            return true;
        }
        leftMatchingRowsIterator = null;
        while (right.moveNext()) {
            int rightHash = hashBuilderForRight.applyAsInt(right.currentElement());
            Tuple<List<Object[]>, Boolean> leftMatchingRows = buffer.get(rightHash);
            if (leftMatchingRows != null) {
                leftMatchingRowsIterator = leftMatchingRows.v1().iterator();
                if (emitNullValues) {
                    // Mark the values if null values should be emitted for later postprocessing
                    if (leftMatchingRows.v2() == false) {
                        leftMatchingRows = new Tuple<>(leftMatchingRows.v1(), true);
                        buffer.put(rightHash, leftMatchingRows);
                    }
                }
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
        Tuple<List<Object[]>, Boolean> existingRows = buffer.get(hash);
        if (existingRows == null) {
            existingRows = new Tuple<>(new ArrayList<>(), false);
            buffer.put(hash, existingRows);
        }
        existingRows.v1().add(currentRow);
        numberOfRowsInBuffer++;
    }

    private boolean findMatchingRows() {
        while (leftMatchingRowsIterator.hasNext()) {
            leftRow.cells(leftMatchingRowsIterator.next());
            combiner.setLeft(leftRow);
            if (joinCondition.test(combiner.currentElement())) {
                return true;
            }
        }
        return false;
    }

    private boolean mustSwitchToRight() {
        return left.allLoaded()
               || numberOfRowsInBuffer == blockSize
               || (leftBatchHasItems == false && numberOfLeftBatchesLoadedForBlock == numberOfLeftBatchesForBlock);
    }

    private boolean mustLoadLeftNextBatch() {
        return leftBatchHasItems == false
               && left.allLoaded() == false
               && numberOfRowsInBuffer < blockSize
               && numberOfLeftBatchesLoadedForBlock < numberOfLeftBatchesForBlock;
    }
}
