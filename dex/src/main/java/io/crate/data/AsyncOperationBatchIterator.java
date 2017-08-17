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
import io.crate.exceptions.Exceptions;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Iterator;
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
public class AsyncOperationBatchIterator<T> implements BatchIterator<T> {

    private final BatchIterator<T> source;
    private final int batchSize;
    private final BatchAccumulator<T, Iterator<? extends T>> batchAccumulator;

    private Iterator<? extends T> it;
    private int idxWithinBatch = 0;
    private boolean sourceExhausted = false;
    private boolean closed = false;
    private volatile Throwable killed = null;
    private T current = null;

    public AsyncOperationBatchIterator(BatchIterator<T> source,
                                       BatchAccumulator<T, Iterator<? extends T>> batchAccumulator) {
        this.source = source;
        this.batchSize = batchAccumulator.batchSize();
        this.batchAccumulator = batchAccumulator;
        this.it = Collections.emptyIterator();
    }

    @Override
    public T currentElement() {
        return current;
    }

    @Override
    public void moveToStart() {
        raiseIfClosedOrKilled();

        source.moveToStart();
        batchAccumulator.reset();
        sourceExhausted = false;
        it = Collections.emptyIterator();
        current = null;
    }

    @Override
    public boolean moveNext() {
        raiseIfClosedOrKilled();

        if (it.hasNext()) {
            current = it.next();
            return true;
        }
        current = null;
        return false;
    }

    @Override
    public void close() {
        source.close();
        batchAccumulator.close();
        closed = true;
    }

    private void concatRows(Iterator<? extends T> rows) {
        idxWithinBatch = 0;
        it = Iterators.concat(it, rows);
    }

    @Override
    public CompletionStage<?> loadNextBatch() {
        if (sourceExhausted) {
            return CompletableFutures.failedFuture(new IllegalStateException("BatchIterator already fully loaded"));
        }
        return uncheckedLoadNextBatch();
    }

    private CompletionStage<?> uncheckedLoadNextBatch() {
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
        return processBatch(true);
    }

    @Nullable
    private CompletionStage<?> tryProcessBatchFromLoadedSource() {
        try {
            while (source.moveNext()) {
                idxWithinBatch++;
                batchAccumulator.onItem(source.currentElement());
                if (batchSize > 0 && idxWithinBatch == batchSize) {
                    return processBatch(false);
                }
            }
        } catch (Throwable t) {
            return CompletableFutures.failedFuture(t);
        }
        return null;
    }

    private CompletionStage<?> processBatch(boolean isLastBatch) {
        return batchAccumulator.processBatch(isLastBatch)
            .exceptionally(this::maybeRaiseKilled)
            .thenAccept(this::concatRows);
    }

    private Iterator<? extends T> maybeRaiseKilled(Throwable throwable) {
        if (killed == null) {
            Exceptions.rethrowUnchecked(throwable);
        } else {
            Exceptions.rethrowUnchecked(killed);
        }
        throw new AssertionError("Previous lines must throw an exception");
    }

    @Override
    public boolean allLoaded() {
        return sourceExhausted;
    }

    private void raiseIfClosedOrKilled() {
        if (killed != null) {
            Exceptions.rethrowUnchecked(killed);
        }
        if (closed) {
            throw new IllegalStateException("BatchIterator is closed");
        }
    }

    @Override
    public void kill(@Nonnull Throwable throwable) {
        killed = throwable;
    }
}
