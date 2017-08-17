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
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;

/**
 * A BatchIterator implementation which always fully consumes another BatchIterator before it can generate it's result.
 *
 * Result generation and row-processing is handled by a {@link Collector}
 *
 */
public class CollectingBatchIterator<T> implements BatchIterator<T> {

    private final BatchIterator<T> source;
    private final Function<BatchIterator<T>, CompletableFuture<? extends Iterable<? extends T>>> consumer;

    private Iterator<? extends T> it = Collections.emptyIterator();
    private CompletableFuture<? extends Iterable<? extends T>> resultFuture;
    private T current;

    private CollectingBatchIterator(BatchIterator<T> source,
                                    Function<BatchIterator<T>, CompletableFuture<? extends Iterable<? extends T>>> consumer) {
        this.source = source;
        this.consumer = consumer;
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
        return new CloseAssertingBatchIterator<>(
            new CollectingBatchIterator<>(
                source,
                bi -> BatchIterators.collect(source, collector)
            )
        );
    }

    public static <T> BatchIterator<T> newInstance(BatchIterator<T> source,
                                                   Function<BatchIterator<T>, CompletableFuture<? extends Iterable<? extends T>>> consumer) {
        return new CloseAssertingBatchIterator<>(new CollectingBatchIterator<>(source, consumer));
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
        source.close();
    }

    @Override
    public CompletionStage<?> loadNextBatch() {
        if (resultFuture == null) {
            resultFuture = consumer.apply(source)
                .whenComplete((r, t) -> {
                    source.close();
                    if (t == null) {
                        it = r.iterator();
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
        source.kill(throwable);
        // rest is handled by CloseAssertingBatchIterator
    }
}
