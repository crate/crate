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

package io.crate.action.sql;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.elasticsearch.common.breaker.CircuitBreaker;

import io.crate.breaker.BlockBasedRamAccounting;
import io.crate.breaker.RowAccounting;
import io.crate.breaker.RowCellsAccountingWithEstimators;
import io.crate.common.collections.Lists2;
import io.crate.data.ArrayRow;
import io.crate.data.BatchIterator;
import io.crate.data.BatchIterators;
import io.crate.data.CompositeBatchIterator;
import io.crate.data.ForwardingBatchIterator;
import io.crate.data.InMemoryBatchIterator;
import io.crate.data.LimitingBatchIterator;
import io.crate.data.Row;
import io.crate.data.RowConsumer;
import io.crate.data.SentinelRow;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.Symbols;
import io.crate.sql.tree.Declare.Hold;
import io.crate.sql.tree.Fetch.ScrollMode;

public final class Cursor implements AutoCloseable {

    private final Hold hold;
    private final CompletableFuture<BatchIterator<Row>> queryIterator;
    private final List<Symbol> outputs;
    private final boolean scroll;
    private final List<Object[]> rows = new ArrayList<>();
    private final ArrayRow sharedRow = new ArrayRow();
    private boolean exhausted = false;
    private int cursorPosition = 0;
    private RowAccounting<Object[]> rowAccounting;

    public Cursor(CircuitBreaker circuitBreaker,
                  boolean scroll,
                  Hold hold,
                  CompletableFuture<BatchIterator<Row>> queryIterator,
                  List<Symbol> outputs) {
        this.scroll = scroll;
        this.hold = hold;
        this.queryIterator = queryIterator;
        this.outputs = outputs;
        this.rowAccounting = new RowCellsAccountingWithEstimators(
            Symbols.typeView(outputs),
            new BlockBasedRamAccounting(
                bytes -> circuitBreaker.addEstimateBytesAndMaybeBreak(bytes, "cursor-scroll"),
                BlockBasedRamAccounting.MAX_BLOCK_SIZE_IN_BYTES
            ),
            0
        );
    }

    public Hold hold() {
        return hold;
    }

    public void fetch(RowConsumer consumer, ScrollMode scrollMode, long count) {
        if (queryIterator.isDone()) {
            try {
                BatchIterator<Row> bi = queryIterator.join();
                triggerConsumer(consumer, new BufferingBatchIterator(bi), scrollMode, count);
            } catch (Throwable t) {
                consumer.accept(null, t);
                return;
            }
        } else {
            queryIterator.whenComplete((bi, err) -> {
                if (err == null) {
                    try {
                        triggerConsumer(consumer, new BufferingBatchIterator(bi), scrollMode, count);
                    } catch (Throwable t) {
                        consumer.accept(null, t);
                    }
                } else {
                    consumer.accept(null, err);
                }
            });
        }
    }

    private class BufferingBatchIterator extends ForwardingBatchIterator<Row> {

        private final BatchIterator<Row> delegate;

        BufferingBatchIterator(BatchIterator<Row> delegate) {
            this.delegate = delegate;
        }

        @Override
        protected BatchIterator<Row> delegate() {
            return delegate;
        }

        @Override
        public boolean moveNext() {
            boolean moveNext = delegate.moveNext();
            if (moveNext) {
                cursorPosition++;
                if (scroll) {
                    Object[] row = currentElement().materialize();
                    rowAccounting.accountForAndMaybeBreak(row);
                    rows.add(row);
                }
                return true;
            } else {
                exhausted = true;
                return false;
            }
        }

        @Override
        public void close() {
            // Close is deferred to when the cursor gets closed
        }
    }

    private BatchIterator<Row> bufferedRowOrNone(int idx) {
        if (idx < 0 || (cursorPosition >= rows.size() && exhausted)) {
            return InMemoryBatchIterator.empty(SentinelRow.SENTINEL);
        } else {
            Object[] cells = rows.get(idx);
            sharedRow.cells(cells);
            return InMemoryBatchIterator.of(sharedRow, SentinelRow.SENTINEL);
        }
    }

    private void triggerConsumer(RowConsumer consumer, BatchIterator<Row> fullResult, ScrollMode mode, long lCount) {
        // Long.MAX_VALUE is used as "ALL"
        if (lCount == Long.MAX_VALUE) {
            lCount = Integer.MAX_VALUE;
        }
        if (lCount > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Count must not exceed " + Integer.MAX_VALUE);
        }
        int count = (int) lCount;
        boolean moveForward = mode == ScrollMode.RELATIVE && count >= 0 || mode == ScrollMode.ABSOLUTE && count > cursorPosition;
        if (!moveForward && !scroll) {
            throw new IllegalArgumentException("Cannot move backward if cursor was created with NO SCROLL");
        }

        if (mode == ScrollMode.ABSOLUTE) {
            // Absolute jumps to a position and returns that row (or none if before start; after end)

            if (count < rows.size()) {
                cursorPosition = count;
                consumer.accept(bufferedRowOrNone(count - 1), null);
            } else {
                int steps = count - cursorPosition + 1;
                fullResult.move(steps, row -> {}, err -> {
                    cursorPosition--;
                    if (err == null) {
                        consumer.accept(bufferedRowOrNone(count - 1), null);
                        cursorPosition = count == rows.size() ? count : count - 1;
                    } else {
                        consumer.accept(null, err);
                    }
                });
            }
        } else if (moveForward) {
            if (count == 0) {
                int idx = cursorPosition - 1;
                if (cursorPosition > rows.size()) {
                    idx--;
                }
                consumer.accept(bufferedRowOrNone(idx), null);
                return;
            }
            BatchIterator<Row> delegate;
            if (!scroll || cursorPosition >= rows.size()) {
                delegate = fullResult;
            } else {
                // Cursor and resultBatchIterator position can go out of sync due to backward movement
                // Need to re-use buffered results to fill the gap between cursor and resultBatchIterator
                List<Object[]> items = rows.subList(cursorPosition, rows.size());
                BatchIterator<Row> bufferedBi = biFromItems(items);
                delegate = CompositeBatchIterator.seqComposite(bufferedBi, fullResult);
            }
            // There is no moveNext() call to update the position
            // When rows.size() + 1, last row exceeded and next backwards movement must include the last row
            cursorPosition = Math.min(cursorPosition + count, rows.size() + 1);

            if (count <= Integer.MAX_VALUE) {
                consumer.accept(LimitingBatchIterator.newInstance(delegate, count), null);
            } else {
                consumer.accept(delegate, null);
            }
        } else {
            int start = cursorPosition + count;
            assert start < cursorPosition : "count must be negative";
            List<Object[]> items = Lists2.reverse(rows.subList(Math.max(start - 1, 0), Math.max(cursorPosition - 1, 0)));
            BatchIterator<Row> bi = biFromItems(items);
            cursorPosition = Math.max(start, 0);
            consumer.accept(bi, null);
        }
    }

    private BatchIterator<Row> biFromItems(List<Object[]> items) {
        BatchIterator<Object[]> objectRows;
        if (items.isEmpty()) {
            objectRows = InMemoryBatchIterator.empty(null);
        } else {
            objectRows = InMemoryBatchIterator.of(items, null, false);
        }
        return BatchIterators.map(objectRows, cells -> {
            sharedRow.cells(cells);
            return sharedRow;
        });
    }

    public List<Symbol> outputs() {
        return outputs;
    }

    @Override
    public void close() {
        rowAccounting.release();
        if (queryIterator.isDone() && !queryIterator.isCompletedExceptionally()) {
            queryIterator.join().close();
        } else {
            queryIterator.whenComplete((bi, err) -> {
                if (bi != null) {
                    bi.close();
                }
            });
        }
    }
}
