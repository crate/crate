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

import com.google.common.collect.Iterables;
import io.crate.concurrent.CompletableFutures;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.function.IntSupplier;

import static io.crate.concurrent.CompletableFutures.supplyAsync;

/**
 * Similar to {@link CompositeBatchIterator} this is a BatchIterator implementation which is backed by multiple
 * other BatchIterators.
 * <p>
 * The difference is that this will eagerly load all sources in {@link #loadNextBatch()} using a separate executor.
 * E.g. if the source BatchIterators are {@link CollectingBatchIterator} which can have a fairly expensive {@link #loadNextBatch()}
 * their work can be executed concurrently.
 * <p>
 * The provided {@link Executor} is expected to deal with rejected tasks (ie. via delegation to other components for
 * execution or other strategies).
 * This iterator does not handle rejected tasks in any way.
 */
public class AsyncCompositeBatchIterator<T> implements BatchIterator<T> {

    private final BatchIterator<T>[] iterators;
    private final Executor executor;
    private final IntSupplier availableThreads;

    private int idx = 0;

    @SafeVarargs
    public AsyncCompositeBatchIterator(Executor executor,
                                       IntSupplier availableThreads,
                                       BatchIterator<T>... iterators) {
        assert iterators.length > 0 : "Must have at least 1 iterator";

        this.availableThreads = availableThreads;
        this.executor = executor;
        this.iterators = iterators;
    }

    @Override
    public T currentElement() {
        return iterators[idx].currentElement();
    }

    @Override
    public void moveToStart() {
        for (BatchIterator iterator : iterators) {
            iterator.moveToStart();
        }
        resetIndex();
    }

    private void resetIndex() {
        idx = 0;
    }

    @Override
    public boolean moveNext() {
        while (idx < iterators.length) {
            BatchIterator iterator = iterators[idx];
            if (iterator.moveNext()) {
                return true;
            }
            idx++;
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
        if (allLoaded()) {
            return CompletableFutures.failedFuture(new IllegalStateException("BatchIterator already loaded"));
        }
        int availableThreads = this.availableThreads.getAsInt();
        List<BatchIterator<T>> itToLoad = getIteratorsToLoad(iterators);

        List<CompletableFuture<CompletableFuture>> nestedFutures = new ArrayList<>();
        if (availableThreads < itToLoad.size()) {
            Iterable<List<BatchIterator<T>>> iteratorsPerThread = Iterables.partition(
                itToLoad, itToLoad.size() / availableThreads);

            for (List<BatchIterator<T>> batchIterators: iteratorsPerThread) {
                CompletableFuture<CompletableFuture> future = supplyAsync(() -> {
                    ArrayList<CompletableFuture<?>> futures = new ArrayList<>(batchIterators.size());
                    for (BatchIterator<T> batchIterator: batchIterators) {
                        futures.add(batchIterator.loadNextBatch().toCompletableFuture());
                    }
                    return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
                }, executor);
                nestedFutures.add(future);
            }
        } else {
            for (BatchIterator<T> iterator: itToLoad) {
                nestedFutures.add(supplyAsync(() -> iterator.loadNextBatch().toCompletableFuture(), executor));
            }
        }
        return CompletableFutures.allAsList(nestedFutures)
            .thenCompose(innerFutures -> CompletableFuture.allOf(innerFutures.toArray(new CompletableFuture[0])));
    }

    private static <T> List<BatchIterator<T>> getIteratorsToLoad(BatchIterator<T>[] allIterators) {
        ArrayList<BatchIterator<T>> itToLoad = new ArrayList<>(allIterators.length);
        for (BatchIterator<T> iterator: allIterators) {
            if (!iterator.allLoaded()) {
                itToLoad.add(iterator);
            }
        }
        return itToLoad;
    }

    @Override
    public boolean allLoaded() {
        for (BatchIterator iterator : iterators) {
            if (iterator.allLoaded() == false) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void kill(@Nonnull Throwable throwable) {
        for (BatchIterator iterator : iterators) {
            iterator.kill(throwable);
        }
    }

    @Override
    public boolean involvesIO() {
        for (BatchIterator iterator : iterators) {
            if (iterator.involvesIO()) {
                return true;
            }
        }
        return false;
    }
}
