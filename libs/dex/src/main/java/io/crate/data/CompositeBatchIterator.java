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

package io.crate.data;

import static io.crate.common.concurrent.CompletableFutures.supplyAsync;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.function.IntSupplier;

import org.jetbrains.annotations.NotNull;

import io.crate.common.concurrent.CompletableFutures;
import io.crate.common.exceptions.Exceptions;

/**
 * BatchIterator implementations backed by multiple other BatchIterators.
 */
public final class CompositeBatchIterator {

    @SuppressWarnings("unchecked")
    public static <T> BatchIterator<T> seqComposite(Collection<? extends BatchIterator<T>> iterators) {
        return seqComposite(iterators.toArray(new BatchIterator[0]));
    }

    /**
     * Composite batchIterator that consumes each individual iterator fully before moving to the next.
     */
    @SafeVarargs
    public static <T> BatchIterator<T> seqComposite(BatchIterator<T> ... iterators) {
        switch (iterators.length) {
            case 0:
                return InMemoryBatchIterator.empty(null);

            case 1:
                return iterators[0];

            default:
                // prefer loaded iterators over unloaded to improve performance in case only a subset of data is consumed
                Comparator<BatchIterator<T>> comparing = Comparator.comparing(BatchIterator::allLoaded);
                Arrays.sort(iterators, comparing.reversed());
                return new SeqCompositeBI<>(iterators);
        }
    }

    /**
     * Composite batchIterator that eagerly loads the individual iterators on `loadNext` multi-threaded
     */
    @SuppressWarnings("unchecked")
    public static <T> BatchIterator<T> asyncComposite(Executor executor,
                                                      IntSupplier availableThreads,
                                                      Collection<? extends BatchIterator<T>> iterators) {
        if (iterators.size() == 1) {
            return iterators.iterator().next();
        }
        return new AsyncCompositeBI<>(executor, availableThreads, iterators.toArray(new BatchIterator[0]));
    }

    private abstract static class AbstractCompositeBI<T> implements BatchIterator<T> {

        protected final BatchIterator<T>[] iterators;
        protected int idx = 0;

        AbstractCompositeBI(BatchIterator<T>[] iterators) {
            assert iterators.length > 0 : "Must have at least 1 iterator";
            this.iterators = iterators;
        }

        @Override
        public T currentElement() {
            return iterators[idx].currentElement();
        }

        @Override
        public void moveToStart() {
            for (BatchIterator<T> iterator : iterators) {
                iterator.moveToStart();
            }
            idx = 0;
        }

        @Override
        public void close() {
            for (BatchIterator<T> iterator : iterators) {
                iterator.close();
            }
        }

        @Override
        public boolean allLoaded() {
            for (BatchIterator<T> iterator : iterators) {
                if (iterator.allLoaded() == false) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public void kill(@NotNull Throwable throwable) {
            for (BatchIterator<T> iterator : iterators) {
                iterator.kill(throwable);
            }
        }

        @Override
        public boolean hasLazyResultSet() {
            for (BatchIterator<T> iterator : iterators) {
                if (iterator.hasLazyResultSet()) {
                    return true;
                }
            }
            return false;
        }
    }

    private static class SeqCompositeBI<T> extends AbstractCompositeBI<T> {

        SeqCompositeBI(BatchIterator<T>[] iterators) {
            super(iterators);
        }

        @Override
        public boolean moveNext() {
            while (idx < iterators.length) {
                BatchIterator<T> iterator = iterators[idx];
                if (iterator.moveNext()) {
                    return true;
                }
                if (iterator.allLoaded() == false) {
                    return false;
                }
                idx++;
            }
            idx = 0;
            return false;
        }

        @Override
        public CompletionStage<?> loadNextBatch() throws Exception {
            for (BatchIterator<T> iterator : iterators) {
                if (iterator.allLoaded()) {
                    continue;
                }
                return iterator.loadNextBatch();
            }
            throw new IllegalStateException("BatchIterator already fully loaded");
        }
    }

    private static class AsyncCompositeBI<T> extends AbstractCompositeBI<T> {

        private final Executor executor;
        private final IntSupplier availableThreads;

        AsyncCompositeBI(Executor executor, IntSupplier availableThreads, BatchIterator<T>[] iterators) {
            super(iterators);
            this.executor = executor;
            this.availableThreads = availableThreads;
        }

        @Override
        public boolean moveNext() {
            while (idx < iterators.length) {
                BatchIterator<T> iterator = iterators[idx];
                if (iterator.moveNext()) {
                    return true;
                }
                idx++;
            }
            idx = 0;
            return false;
        }

        private int numIteratorsActive() {
            int activeIts = 0;
            for (var it : iterators) {
                if (!it.allLoaded()) {
                    activeIts++;
                }
            }
            return activeIts;
        }

        @Override
        public CompletionStage<?> loadNextBatch() throws Exception {
            if (allLoaded()) {
                throw new IllegalStateException("BatchIterator already loaded");
            }
            int activeIts = numIteratorsActive();
            int numThreads = Math.max(1, availableThreads.getAsInt());
            final int usedThreads = Math.min(numThreads, activeIts);
            ArrayList<CompletableFuture<CompletableFuture<?>>> nestedFutures = new ArrayList<>(usedThreads);
            for (int t = 0; t < usedThreads; t++) {
                final int thread = t;
                nestedFutures.add(supplyAsync(() -> {
                    ArrayList<CompletableFuture<?>> futures = new ArrayList<>();
                    for (int i = 0; i < iterators.length; i++) {
                        var it = iterators[i];
                        if (it.allLoaded() || i % usedThreads != thread) {
                            continue;
                        }
                        try {
                            futures.add(it.loadNextBatch().toCompletableFuture());
                        } catch (Exception e) {
                            throw Exceptions.toRuntimeException(e);
                        }
                    }
                    return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
                }, executor));
            }
            return CompletableFutures.allAsList(nestedFutures)
                .thenCompose(innerFutures -> CompletableFuture.allOf(innerFutures.toArray(new CompletableFuture[0])));
        }
    }
}
