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

import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import org.jetbrains.annotations.NotNull;

import io.crate.common.exceptions.Exceptions;

/**
 * A BatchIterator that is initially empty and can be loaded with {@link #loadNextBatch()} via {@code loadItems}
 */
public final class CollectingBatchIterator<T> implements BatchIterator<T> {

    private final Supplier<CompletableFuture<? extends Iterable<? extends T>>> loadItems;
    private final Runnable onClose;
    private final Consumer<? super Throwable> onKill;
    private final boolean involvesIO;

    private CompletableFuture<? extends Iterable<? extends T>> resultFuture;
    private Iterator<? extends T> it = Collections.emptyIterator();
    private T current;
    private volatile Throwable killed;

    private CollectingBatchIterator(Supplier<CompletableFuture<? extends Iterable<? extends T>>> loadItems,
                                    Runnable onClose,
                                    Consumer<? super Throwable> onKill,
                                    boolean involvesIO) {
        this.loadItems = loadItems;
        this.onClose = onClose;
        this.onKill = onKill;
        this.involvesIO = involvesIO;
    }

    /**
     * Create a BatchIterator which will consume the source, summing up the first column (must be of type long).
     *
     * <pre>
     *     source BatchIterator:
     *     [ 1, 2, 2, 1 ]
     *
     *     output:
     *     [ 6 ]
     * </pre>
     */
    public static BatchIterator<Row> summingLong(BatchIterator<Row> source) {
        return newInstance(
            source,
            Collectors.collectingAndThen(
                Collectors.summingLong(r -> (long) r.get(0)),
                sum -> Collections.singletonList(new Row1(sum))));
    }

    public static <T, A> BatchIterator<T> newInstance(BatchIterator<T> source,
                                                      Collector<T, A, ? extends Iterable<? extends T>> collector) {
        return newInstance(
            source::close,
            source::kill,
            () -> source.collect(collector),
            source.hasLazyResultSet()
        );
    }

    public static <I, O> BatchIterator<O> newInstance(BatchIterator<I> source,
                                                      Function<BatchIterator<I>, CompletableFuture<? extends Iterable<? extends O>>> processSource,
                                                      boolean involvesIO) {
        return newInstance(
            source::close,
            source::kill,
            () -> processSource.apply(source).whenComplete((r, f) -> source.close()),
            involvesIO);
    }

    public static <T> BatchIterator<T> newInstance(Runnable onClose,
                                                   Consumer<? super Throwable> onKill,
                                                   Supplier<CompletableFuture<? extends Iterable<? extends T>>> loadItems,
                                                   boolean involvesIO) {
        return new CollectingBatchIterator<>(loadItems, onClose, onKill, involvesIO);
    }

    @Override
    public T currentElement() {
        return current;
    }

    @Override
    public void moveToStart() {
        raiseIfKilled();
        if (resultFuture != null) {
            if (resultFuture.isDone() == false) {
                throw new IllegalStateException("BatchIterator is loading");
            }
            it = resultFuture.join().iterator();
        }
        current = null;
    }

    @Override
    public boolean moveNext() {
        raiseIfKilled();
        if (it.hasNext()) {
            current = it.next();
            return true;
        }
        current = null;
        return false;
    }

    @Override
    public void close() {
        onClose.run();
        killed = BatchIterator.CLOSED;
    }

    @Override
    public CompletionStage<?> loadNextBatch() throws Exception {
        if (resultFuture == null) {
            resultFuture = loadItems.get()
                .whenComplete((r, t) -> {
                    if (t == null) {
                        it = r.iterator();
                    } else if (t instanceof RuntimeException) {
                        throw (RuntimeException) t;
                    } else {
                        throw new RuntimeException(t);
                    }
                });
            return resultFuture;
        }
        throw new IllegalStateException("BatchIterator already loaded");
    }

    @Override
    public boolean allLoaded() {
        return resultFuture != null;
    }

    @Override
    public void kill(@NotNull Throwable throwable) {
        onKill.accept(throwable);
        killed = throwable;
    }

    @Override
    public boolean hasLazyResultSet() {
        return involvesIO;
    }

    private void raiseIfKilled() {
        if (killed != null) {
            Exceptions.rethrowUnchecked(killed);
        }
    }
}
