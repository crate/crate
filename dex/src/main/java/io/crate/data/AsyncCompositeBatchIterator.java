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

import io.crate.concurrent.CompletableFutures;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.function.Supplier;

/**
 * Similar to {@link CompositeBatchIterator} this is a BatchIterator implementation which is backed by multiple
 * other BatchIterators.
 *
 * The difference is that this will eagerly load all sources in {@link #loadNextBatch()} using a separate executor.
 * E.g. if the source BatchIterators are {@link CollectingBatchIterator} which can have a fairly expensive {@link #loadNextBatch()}
 * their work can be executed concurrently.
 */
public class AsyncCompositeBatchIterator implements BatchIterator {

    private final BatchIterator[] iterators;
    private final CompositeColumns columns;
    private final Executor executor;

    private int idx = 0;
    private boolean loading = false;

    public AsyncCompositeBatchIterator(Executor executor, BatchIterator... iterators) {
        assert iterators.length > 0 : "Must have at least 1 iterator";

        this.executor = executor;
        this.iterators = iterators;
        this.columns = new CompositeColumns(iterators);
    }

    @Override
    public Columns rowData() {
        return columns;
    }

    @Override
    public void moveToStart() {
        raiseIfLoading();
        for (BatchIterator iterator : iterators) {
            iterator.moveToStart();
        }
        resetIndex();
    }

    private void resetIndex() {
        idx = 0;
        columns.updateInputs(idx);
    }

    @Override
    public boolean moveNext() {
        raiseIfLoading();
        while (idx < iterators.length) {
            BatchIterator iterator = iterators[idx];
            if (iterator.moveNext()) {
                return true;
            }
            idx++;
            columns.updateInputs(idx);
        }
        resetIndex();
        return false;
    }

    @Override
    public void close() {
        for (BatchIterator iterator : iterators) {
            iterator.close();
        }
    }

    @Override
    public CompletionStage<?> loadNextBatch() {
        if (loading) {
            return CompletableFutures.failedFuture(new IllegalStateException("BatchIterator already loading"));
        }
        if (allLoaded()) {
            return CompletableFutures.failedFuture(new IllegalStateException("BatchIterator already loaded"));
        }

        List<CompletableFuture<CompletableFuture<?>>> asyncInvocationFutures = new ArrayList<>();
        loading = true;
        for (BatchIterator iterator : iterators) {
            if (iterator.allLoaded()) {
                continue;
            }
            Supplier<CompletableFuture<?>> loadNextBatchFuture = () -> iterator.loadNextBatch().toCompletableFuture();

            // Every "supplyAsync future" completes with the iterator::loadNextBatch() future, so in order to
            // access the loadNextBatch future, the asyncInvocation one should be completed and the result extracted
            CompletableFuture<CompletableFuture<?>> supplyAsyncFuture =
                CompletableFuture.supplyAsync(loadNextBatchFuture, executor);
            asyncInvocationFutures.add(supplyAsyncFuture);
        }

        CompletableFuture<List<CompletableFuture<?>>> loadNextBatchListFuture =
            CompletableFutures.allAsList(asyncInvocationFutures);

        return loadNextBatchListFuture.thenCompose(
            (List<CompletableFuture<?>> loadNextBatchFutures) ->
                // Future that'll wait for all loadNextBatch futures to complete
                CompletableFuture.allOf(loadNextBatchFutures.toArray(new CompletableFuture[0]))
                    // loading is complete when all loadNextBatch futures are complete
                    .whenComplete((r, t) -> loading = false)
        );
    }

    @Override
    public boolean allLoaded() {
        raiseIfLoading();
        for (BatchIterator iterator : iterators) {
            if (iterator.allLoaded() == false) {
                return false;
            }
        }
        return true;
    }

    private void raiseIfLoading() {
        if (loading) {
            throw new IllegalStateException("BatchIterator already loading");
        }
    }
}
