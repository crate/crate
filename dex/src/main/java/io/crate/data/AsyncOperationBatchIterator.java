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

package io.crate.data;

import com.google.common.collect.Iterators;
import io.crate.concurrent.CompletableFutures;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * BatchIterator implementation which consumes another BatchIterator source.
 * The source data is fed into a {@link BatchAccumulator}.
 * Every {@link BatchAccumulator#batchSize()} items, {@link BatchAccumulator#processBatch(boolean)} is used
 * to create the data for *this* BatchIterator.
 *
 * <pre>
 *
 *  (encapsulated via {@link #loadNextBatch()})
 *
 *   +-> source data
 *   |      |
 *   |    batchAccumulator.onItem
 *   |      |
 *   |    batchSize reached?
 *   |          |
 *   +----------+---+
 *                  |
 *                batchAccumulator.processBatch
 *                      onResult -> fill-up data of *this* BatchIterator
 *
 * </pre>
 */
public class AsyncOperationBatchIterator implements BatchIterator {

    private final BatchIterator source;
    private final int batchSize;
    private final BatchAccumulator<Row, Iterator<? extends Row>> batchAccumulator;
    private final RowColumns rowData;
    private final Row sourceRow;

    private Iterator<? extends Row> it;
    private int idxWithinBatch = 0;
    private boolean sourceExhausted = false;
    private boolean closed = false;
    private boolean loading = false;

    public AsyncOperationBatchIterator(BatchIterator source,
                                       int numColumns,
                                       BatchAccumulator<Row, Iterator<? extends Row>> batchAccumulator) {
        this.source = source;
        this.batchSize = batchAccumulator.batchSize();
        this.batchAccumulator = batchAccumulator;
        this.it = Collections.emptyIterator();

        this.rowData = new RowColumns(numColumns);
        this.sourceRow = RowBridging.toRow(source.rowData());
    }

    @Override
    public Columns rowData() {
        return rowData;
    }

    @Override
    public void moveToStart() {
        raiseIfClosed();
        raiseIfLoading();

        source.moveToStart();
        batchAccumulator.reset();
        sourceExhausted = false;
        it = Collections.emptyIterator();
        rowData.updateRef(RowBridging.OFF_ROW);
    }

    @Override
    public boolean moveNext() {
        raiseIfClosed();
        raiseIfLoading();

        if (it.hasNext()) {
            rowData.updateRef(it.next());
            return true;
        }
        rowData.updateRef(RowBridging.OFF_ROW);
        return false;
    }

    @Override
    public void close() {
        source.close();
        batchAccumulator.close();
        closed = true;
    }

    private void concatRows(Iterator<? extends Row> rows) {
        idxWithinBatch = 0;
        it = Iterators.concat(it, rows);
        loading = false;
    }

    @Override
    public CompletionStage<?> loadNextBatch() {
        if (sourceExhausted) {
            return CompletableFutures.failedFuture(new IllegalStateException("BatchIterator already fully loaded"));
        }
        if (loading) {
            return CompletableFutures.failedFuture(new IllegalStateException("BatchIterator already loading"));
        }
        return uncheckedLoadNextBatch();
    }

    private CompletionStage<?> uncheckedLoadNextBatch() {
        loading = true;
        CompletionStage<?> batchProcessResult = tryProcessBatchFromLoadedSource();
        if (batchProcessResult == null) {
            if (source.allLoaded()) {
                return processRemaining();
            }
            return source.loadNextBatch().thenCompose(ignored -> this.uncheckedLoadNextBatch());
        }
        return batchProcessResult;
    }


    private CompletionStage<?> processRemaining() {
        sourceExhausted = true;
        if (idxWithinBatch > 0) {
            return batchAccumulator.processBatch(true).thenAccept(this::concatRows);
        }
        return CompletableFuture.completedFuture(null).thenAccept(ignored -> loading = false);
    }

    @Nullable
    private CompletionStage<?> tryProcessBatchFromLoadedSource() {
        try {
            while (source.moveNext()) {
                idxWithinBatch++;
                batchAccumulator.onItem(sourceRow);
                if (batchSize > 0 && idxWithinBatch == batchSize) {
                    return batchAccumulator.processBatch(false).thenAccept(this::concatRows);
                }
            }
        } catch (Throwable t) {
            return CompletableFutures.failedFuture(t);
        }
        return null;
    }

    @Override
    public boolean allLoaded() {
        raiseIfClosed();
        return sourceExhausted;
    }

    private void raiseIfClosed() {
        if (closed) {
            throw new IllegalStateException("BatchIterator is closed");
        }
    }

    private void raiseIfLoading() {
        if (loading) {
            throw new IllegalStateException("BatchIterator is already loading");
        }
    }
}
