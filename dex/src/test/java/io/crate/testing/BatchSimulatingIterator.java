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

package io.crate.testing;

import io.crate.concurrent.CompletableFutures;
import io.crate.data.BatchIterator;
import io.crate.data.Columns;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.PrimitiveIterator;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;

/**
 * An BatchIterator implementation which delegates to another one, but adds "fake" batches.
 */
public class BatchSimulatingIterator implements BatchIterator {

    private final BatchIterator delegate;
    private final int batchSize;
    private final Executor executor;
    private final PrimitiveIterator.OfLong loadNextDelays;
    private final int numBatches;

    private int numSuccessMoveNextCallsInBatch = 0;
    private int currentBatch = 0;
    private final CompletableFuture[] loadingStatus = new CompletableFuture[1];

    /**
     * @param batchSize                how many {@link #moveNext()} calls are allowed per batch before it returns false
     * @param maxAdditionalFakeBatches how many {@link #loadNextBatch()} calls are allowed after {@code delegate.allLoaded()} is true.
     *                                 (This is an upper limit, if a consumer calls moveNext correctly, the actual number may be lower)
     */
    public BatchSimulatingIterator(BatchIterator delegate,
                                   int batchSize,
                                   int maxAdditionalFakeBatches,
                                   @Nullable Executor executor) {
        assert batchSize > 0 : "batchSize must be greater than 0. It is " + batchSize;
        assert maxAdditionalFakeBatches > 0
            : "maxAdditionalFakeBatches must be greater than 0. It is " + maxAdditionalFakeBatches;

        this.delegate = delegate;
        this.numBatches = maxAdditionalFakeBatches;
        this.batchSize = batchSize;

        this.loadNextDelays = new Random(System.currentTimeMillis()).longs(0, 100).iterator();
        this.executor = executor == null ? ForkJoinPool.commonPool() : executor;
    }

    @Override
    public Columns rowData() {
        return delegate.rowData();
    }

    @Override
    public void moveToStart() {
        ensureNotLoading();
        currentBatch = 0;
        delegate.moveToStart();
        numSuccessMoveNextCallsInBatch = 0;
    }

    @Override
    public boolean moveNext() {
        ensureNotLoading();
        if (numSuccessMoveNextCallsInBatch == batchSize) {
            return false;
        }
        if (delegate.moveNext()) {
            numSuccessMoveNextCallsInBatch++;
            return true;
        }

        if (delegate.allLoaded()) {
            currentBatch = numBatches;
        }
        return false;
    }

    private void ensureNotLoading() {
        if (isLoading()) {
            throw new IllegalStateException("Call not allowed during load operation");
        }
    }

    @Override
    public void close() {
        delegate.close();
    }

    @Override
    public CompletionStage<?> loadNextBatch() {
        if (isLoading()) {
            return CompletableFutures.failedFuture(new IllegalStateException("loadNextBatch call during load operation"));
        }
        if (delegate.allLoaded()) {
            currentBatch++;
            if (currentBatch > numBatches) {
                loadingStatus[0] = null;
                return CompletableFutures.failedFuture(new IllegalStateException("Iterator already fully loaded"));
            }
            CompletableFuture loadingFuture = CompletableFuture.runAsync(() -> {
                try {
                    Thread.sleep(loadNextDelays.nextLong());
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                numSuccessMoveNextCallsInBatch = 0;
            }, executor);
            loadingStatus[0] = loadingFuture;
            loadingFuture.whenComplete((r, t) -> loadingStatus[0] = null);
            return loadingFuture;
        } else {
            CompletableFuture loadingFuture = delegate.loadNextBatch().toCompletableFuture();
            loadingStatus[0] = loadingFuture;
            loadingFuture.whenComplete((r, t) -> loadingStatus[0] = null);
            return loadingFuture;
        }
    }

    private boolean isLoading() {
        CompletableFuture loadingFuture = loadingStatus[0];
        return loadingFuture != null && loadingFuture.isDone() == false;
    }

    @Override
    public boolean allLoaded() {
        return delegate.allLoaded() && currentBatch >= numBatches;
    }

    @Override
    public void kill(@Nonnull Throwable throwable) {
        delegate.kill(throwable);
    }
}
