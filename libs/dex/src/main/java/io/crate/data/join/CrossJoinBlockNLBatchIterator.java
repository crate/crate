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


import java.util.ArrayList;
import java.util.function.LongToIntFunction;

import io.crate.data.BatchIterator;
import io.crate.data.Row;
import io.crate.data.UnsafeArrayRow;
import io.crate.data.breaker.RowAccounting;

/**
 * This BatchIterator is used for both CrossJoins and InnerJoins as for the InnerJoins
 * the joinCondition is tested later on as a filter Projection.
 *
 * The Block Nested Loop algorithm is used to implement this cross join. For the regular
 * Nested Loop, see {@link CrossJoinNLBatchIterator}.
 *
 * <pre>
 *     // Block Nested Loop
 *     fill buffer with next items from the left
 *     for (row in buffer) {
 *         for (rightRow in right) {
 *             match?
 *         }
 *     }
 *     repeat
 * </pre>
 */
public class CrossJoinBlockNLBatchIterator extends JoinBatchIterator<Row, Row, Row> {

    private final LongToIntFunction blockSizeCalculator;
    private final ArrayList<Object[]> blockBuffer;
    private final UnsafeArrayRow rowWrapper;
    private final RowAccounting<Object[]> rowAccounting;

    private long totalBufferedRowSize = 0;
    private int blockBufferMaxSize;
    private int bufferPos;
    private boolean rightInitialized;

    public CrossJoinBlockNLBatchIterator(BatchIterator<Row> left,
                                         BatchIterator<Row> right,
                                         ElementCombiner<Row, Row, Row> combiner,
                                         LongToIntFunction blockSizeCalculator,
                                         RowAccounting<Object[]> rowAccounting) {
        super(left, right, combiner);
        this.blockSizeCalculator = blockSizeCalculator;
        this.blockBuffer = new ArrayList<>(0);
        this.rowWrapper = new UnsafeArrayRow();
        this.rowAccounting = rowAccounting;
        resizeBlockBuffer();
    }

    private void resizeBlockBuffer() {
        rowAccounting.release();
        int avgRowSize = blockBuffer.isEmpty() ? -1 : (int) totalBufferedRowSize / blockBuffer.size();
        totalBufferedRowSize = 0;
        blockBufferMaxSize = blockSizeCalculator.applyAsInt(avgRowSize);
        blockBuffer.clear();
        bufferPos = -1;
    }

    @Override
    public void moveToStart() {
        left.moveToStart();
        right.moveToStart();
        activeIt = right;
        rightInitialized = false;
        resizeBlockBuffer();
    }

    @Override
    public boolean moveNext() {
        do {
            if (bufferPos == -1) {
                // block buffer needs to be filled
                activeIt = right;
                // try advancing the right side first to check if we have items on the right
                if (!rightInitialized) {
                    if (right.moveNext()) {
                        rightInitialized = true;
                    } else {
                        return false;
                    }
                }
                activeIt = left;
                while (blockBuffer.size() < blockBufferMaxSize) {
                    if (left.moveNext()) {
                        Object[] row = left.currentElement().materialize();
                        totalBufferedRowSize += rowAccounting.accountForAndMaybeBreak(row);
                        blockBuffer.add(row);
                    } else {
                        if (left.allLoaded()) {
                            break;
                        } else {
                            return false;
                        }
                    }
                }
                bufferPos = 0;
            }
            if (blockBuffer.isEmpty()) {
                // last buffer is empty we're done
                return false;
            }

            activeIt = right;
            // we have iterated through the entire left block,
            // go to the next item on right.
            if (bufferPos == blockBuffer.size()) {
                if (right.moveNext()) {
                    // emit items in block again with the new right item
                    bufferPos = 0;
                } else {
                    if (right.allLoaded()) {
                        // right side is done, need to trigger loading of next block on the left side
                        right.moveToStart();
                        rightInitialized = false;
                        resizeBlockBuffer();
                    } else {
                        return false;
                    }
                }
            }
        // we need to re-fill the buffer if we have iterated once through the right side
        } while (bufferPos == -1);

        combiner.setRight(right.currentElement());
        rowWrapper.cells(blockBuffer.get(bufferPos));
        combiner.setLeft(rowWrapper);
        bufferPos++;
        return true;
    }

    @Override
    public void close() {
        super.close();
        blockBuffer.clear();
    }
}
