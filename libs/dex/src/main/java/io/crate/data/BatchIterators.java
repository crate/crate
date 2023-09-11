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

import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collector;

public class BatchIterators {

    /**
     * Use {@code collector} to consume all elements from {@code it}
     *
     * @param <T> element type
     * @param <A> state type
     * @param <R> result type
     * @return future containing the result, this is the future that has been provided as argument.
     */
    public static <T, A, R> CompletableFuture<R> collect(BatchIterator<T> it,
                                                         A state,
                                                         Collector<T, A, R> collector,
                                                         CompletableFuture<R> resultFuture) {
        BiConsumer<A, T> accumulator = collector.accumulator();
        Function<A, R> finisher = collector.finisher();
        it.move(Integer.MAX_VALUE, row -> accumulator.accept(state, row), err -> {
            it.close();
            if (err == null) {
                resultFuture.complete(finisher.apply(state));
            } else {
                resultFuture.completeExceptionally(err);
            }
        });
        return resultFuture;
    }

    /**
     * Create chunks of items of a BatchIterator of {@code size}.
     *
     * Example:
     * <pre>
     * {@code
     *     inputBi: [1, 2, 3, 4, 5]
     *
     *     partition(inputBi, 2, ArrayList::new, List::add) -> [[1, 2], [3, 4], [5]]
     * }
     * </pre>
     * @param supplier Used to create the state per chunk
     * @param accumulator Used to add items to the chunk
     * @param stateLimiter Used to dynamically adjust the chunk size.
     * @param <T> input item type
     * @param <A> output item type
     */
    public static <T, A> BatchIterator<A> chunks(BatchIterator<T> bi,
                                                 int size,
                                                 Supplier<A> supplier,
                                                 BiConsumer<A, T> accumulator,
                                                 Predicate<? super A> stateLimiter) {
        return new MappedForwardingBatchIterator<T, A>() {

            private A element = null;
            private A state = supplier.get();
            private int idx = 0;

            @Override
            protected BatchIterator<T> delegate() {
                return bi;
            }

            @Override
            public boolean moveNext() {
                boolean stateLimitReached = false;
                while (idx < size && stateLimitReached == false && bi.moveNext()) {
                    accumulator.accept(state, bi.currentElement());
                    stateLimitReached = stateLimiter.test(state);
                    idx++;
                }
                if (idx == size || stateLimitReached || (idx > 0 && bi.allLoaded())) {
                    element = state;
                    state = supplier.get();
                    idx = 0;
                    return true;
                }
                element = null;
                return false;
            }

            @Override
            public A currentElement() {
                return element;
            }
        };
    }
}
