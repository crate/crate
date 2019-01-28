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

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Collectors;

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
            () -> BatchIterators.collect(source, collector),
            source.involvesIO()
        );
    }

    public static <T> BatchIterator<T> newInstance(BatchIterator<T> source,
                                                   Function<BatchIterator<T>, CompletableFuture<? extends Iterable<? extends T>>> processSource,
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
        return new CloseAssertingBatchIterator<>(new CollectingBatchIterator<>(loadItems, onClose, onKill, involvesIO));
    }

    @Override
    public T currentElement() {
        return current;
    }

    @Override
    public void moveToStart() {
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
    }

    @Override
    public CompletionStage<?> loadNextBatch() {
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
        return CompletableFutures.failedFuture(new IllegalStateException("BatchIterator already loaded"));
    }

    @Override
    public boolean allLoaded() {
        return resultFuture != null;
    }

    @Override
    public void kill(@Nonnull Throwable throwable) {
        onKill.accept(throwable);
        // rest is handled by CloseAssertingBatchIterator
    }

    @Override
    public boolean involvesIO() {
        return involvesIO;
    }
}
