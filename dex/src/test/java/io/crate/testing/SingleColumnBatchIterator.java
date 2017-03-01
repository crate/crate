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
import io.crate.data.CloseAssertingBatchIterator;
import io.crate.data.Columns;

import java.util.Iterator;
import java.util.concurrent.CompletionStage;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

/**
 * A batch iterator with a single column.
 *
 * @param <T> the type of the column values
 */
public class SingleColumnBatchIterator<T> implements BatchIterator {

    private final Iterable<T> iterable;
    private final Columns inputs;
    private T value;
    private Iterator<T> currentIterator;

    /**
     * Returns a batch iterator getting its data from an iterable representing the values for each row.
     *
     * @param iterable the value iterable
     * @param <T>      the type of the values
     * @return a new batch currentIterator
     */
    public static <T> BatchIterator fromIterable(Iterable<T> iterable) {
        return new CloseAssertingBatchIterator(new SingleColumnBatchIterator<>(iterable));
    }

    /**
     * Returns a batch iterator containing a range of integers.
     */
    public static BatchIterator range(int startInclusive, int endExclusive) {
        return fromIterable(() -> IntStream.range(startInclusive, endExclusive).iterator());
    }

    /**
     * Returns a batch iterator containing a range of longs.
     */
    public static BatchIterator range(long startInclusive, long endExclusive) {
        return fromIterable(() -> LongStream.range(startInclusive, endExclusive).iterator());
    }

    private SingleColumnBatchIterator(Iterable<T> iterable) {
        this.iterable = iterable;
        this.inputs = Columns.singleCol(() -> value);
        moveToStart();
    }

    @Override
    public Columns rowData() {
        return inputs;
    }

    @Override
    public void moveToStart() {
        currentIterator = iterable.iterator();
        value = null;
    }

    @Override
    public boolean moveNext() {
        if (currentIterator.hasNext()) {
            value = currentIterator.next();
            return true;
        }
        value = null;
        return false;
    }

    @Override
    public void close() {

    }

    @Override
    public CompletionStage<?> loadNextBatch() {
        return CompletableFutures.failedFuture(new IllegalStateException("loadNextBatch with allLoaded"));
    }

    @Override
    public boolean allLoaded() {
        return true;
    }
}
