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

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.crate.common.concurrent.Killable;


/**
 * <p>
 * An iterator used to navigate over data organized in batches. Though not required most
 * implementations already hold an initial batch of data ready for use and further data can be loaded asynchronously
 * via the {@link #loadNextBatch()} method.
 * </p>
 *
 * <p>
 * The loaded data can be accessed by moving the iterator via the movement methods {@link #moveNext()} and
 * {@link #moveToStart()} and then using the object returned by {@link #currentElement()} to access the data
 * at the current position.
 * </p>
 *
 * <p>
 * Once all loaded data has been consumed, more data can be loaded with {@link #loadNextBatch()} unless {@link
 * #allLoaded()} is true in which case the iterator is exhausted.
 * </p>
 *
 * <p>
 * A BatchIterator starts either *before* the first row or in an unloaded state.
 * This means a consumer can consume a BatchIterator like this:
 * </p>
 *
 * <pre>
 * {@code
 *     while (it.moveNext()) {
 *         // do something with it.currentElement()
 *     }
 *     if (it.allLoaded()) {
 *          // iterator is exhausted
 *     } else {
 *         it.loadNextBatch().whenComplete((r, t) -> {
 *             // continue consumption
 *         }
 *     }
 * }
 * </pre>
 *
 *
 * Thread-safety notes:
 * Concurrent move of a BatchIterator is not supported.
 */
public interface BatchIterator<T> extends Killable {


    IllegalStateException CLOSED = new IllegalStateException("BatchIterator is closed");

    /**
     * This method returns the item that is at the current position of the BatchIterator.
     *
     * The behaviour of this method if the BatchIterator is "not on a position" is undefined.
     */
    T currentElement();


    /**
     * Moves the Iterator back to the starting position.
     *
     * A consumer can then iterate over the rows again by calling into
     * {@link #moveNext()} and {@link #loadNextBatch()} as appropriate.
     *
     * @throws IllegalStateException if the cursor is closed
     */
    void moveToStart();

    /**
     * Advances the iterator.
     *
     * @return true if the iterator moved to the next valid row.
     *         false if the iterator is no longer on a valid row.
     *         If {@link #allLoaded()} returns true the iterator is out of data. Otherwise {@link #loadNextBatch()}
     *         can be used to load the next batch - after which {@code moveNext()} can be used again.
     *
     * @throws IllegalStateException if the cursor is closed
     */
    boolean moveNext();

    /**
     * Closes the iterator and frees all resources.
     * After this method has been called all methods on the iterator will result in an error.
     *
     * There are two exceptions to that:
     *   - close: Close itself can be called multiple times, but only the first call has an effect.
     *   - loadNextBatch: This method never raises, but instead returns a failed CompletionStage.
     */
    void close();

    /**
     * Loads the next batch if there is still data available for loading.
     * This may only be called if {@link #allLoaded()} returns false.
     * If {@link #allLoaded()} returns true, calling this method will return a failed CompletionStage.
     *
     * <p>
     * NOTE: while loading takes place the iterator must not be moved.
     * The iterator behaviour in this case is undetermined.
     *
     * @return a future which will be completed once the loading is done.
     *         Once the future completes the iterator is still in an "off-row" state, but {@link #moveNext()}
     *         can be called again if the next batch contains more data.
     */
    CompletionStage<?> loadNextBatch() throws Exception;

    /**
     * @return true if no more batches can be loaded
     */
    boolean allLoaded();

    /**
     * @return true if the records returned by this BatchIterator are generated on-demand.
     *              If the underlying data is materialized in-memory
     *              *and* could be re-released after this batch-iterator is done,
     *              then this should return false - even if there is an on-demand transformation.
     */
    boolean hasLazyResultSet();


    /**
     * Move the batchIterator by {@code steps} utilizing {@link #moveNext()
     *
     * @param onNext called for each successful moveNext.
     * @param onFinish called after {@code steps}, if exhausted, or on error.
     */
    default void move(int steps, Consumer<T> onNext, Consumer<Throwable> onFinish) {
        assert steps >= 0 : "steps must be positive";
        try {
            for (int i = 0; i < steps; i++) {
                if (moveNext()) {
                    onNext.accept(currentElement());
                } else {
                    if (allLoaded()) {
                        onFinish.accept(null);
                        return;
                    } else {
                        var nextBatch = loadNextBatch().toCompletableFuture();
                        if (nextBatch.isDone()) {
                            if (nextBatch.isCompletedExceptionally()) {
                                // trigger exception
                                nextBatch.join();
                            }
                            i--;
                            continue;
                        } else {
                            int remainingSteps = steps - i;
                            nextBatch.whenComplete((res, err) -> {
                                if (err == null) {
                                    move(remainingSteps, onNext, onFinish);
                                } else {
                                    onFinish.accept(err);
                                }
                            });
                            return;
                        }
                    }
                }
            }
            onFinish.accept(null);
        } catch (Throwable ex) {
            onFinish.accept(ex);
        }
    }


    default <O> BatchIterator<O> map(Function<? super T, ? extends O> mapper) {
        final BatchIterator<T> source = this;
        return new MappedForwardingBatchIterator<T, O>() {

            @Override
            public O currentElement() {
                return mapper.apply(source.currentElement());
            }

            @Override
            protected BatchIterator<T> delegate() {
                return source;
            }
        };
    }


    /**
     * Use {@code collector} to consume all elements from {@code it}
     *
     * @param <A> state type
     * @param <R> result type
     * @return future containing the result
     */
    default <A, R> CompletableFuture<R> collect(Collector<T, A, R> collector, boolean close) {
        BiConsumer<A, T> accumulator = collector.accumulator();
        Function<A, R> finisher = collector.finisher();
        A state = collector.supplier().get();
        CompletableFuture<R> result = new CompletableFuture<>();
        move(Integer.MAX_VALUE, row -> accumulator.accept(state, row), err -> {
            if (close) {
                close();
            }
            if (err == null) {
                result.complete(finisher.apply(state));
            } else {
                result.completeExceptionally(err);
            }
        });
        return result;
    }

    /**
     * Use {@code collector} to consume all elements from {@code it}
     *
     * @param <A> state type
     * @param <R> result type
     * @return future containing the result
     */
    default <A, R> CompletableFuture<R> collect(Collector<T, A, R> collector) {
        return collect(collector, true);
    }

    /**
     * Shortcut for {@link #collect(Collector)} with {@link Collectors#toList()}.
     * To mirror {@link Stream#toList()}
     */
    default <R> CompletableFuture<List<T>> toList() {
        return collect(Collectors.toList());
    }
}
