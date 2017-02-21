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

import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collector;
import java.util.stream.Collectors;

/**
 * A BatchIterator implementation which always fully consumes another BatchIterator before it can generate it's result.
 *
 * Result generation and row-processing is handled by a {@link Collector}
 *
 * @param <A> the state type of the {@link Collector}
 */
public class CollectingBatchIterator<A> implements BatchIterator {

    private final BatchIterator source;
    private final Collector<Row, A, Iterable<Row>> collector;

    private Row currentRow = OFF_ROW;
    private Iterator<Row> it = Collections.emptyIterator();
    private CompletableFuture<Iterable<Row>> resultFuture;

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
    public static BatchIterator summingLong(BatchIterator source) {
        return newInstance(
            source,
            Collectors.collectingAndThen(
                Collectors.summingLong((Row r) -> (long) r.get(0)), sum -> Collections.singletonList(new Row1(sum))));
    }

    public static <A> BatchIterator newInstance(BatchIterator source, Collector<Row, A, Iterable<Row>> collector) {
        return new CloseAssertingBatchIterator(new CollectingBatchIterator<>(source, collector));
    }

    private CollectingBatchIterator(BatchIterator source, Collector<Row, A, Iterable<Row>> collector) {
        this.source = source;
        this.collector = collector;
    }

    @Override
    public void moveToStart() {
        if (resultFuture != null) {
            if (resultFuture.isDone() == false) {
                throw new IllegalStateException("BatchIterator is loading");
            }
            it = resultFuture.join().iterator();
        }
        currentRow = OFF_ROW;
    }

    @Override
    public boolean moveNext() {
        raiseIfLoading();
        if (it.hasNext()) {
            currentRow = it.next();
            return true;
        }
        currentRow = OFF_ROW;
        return false;
    }

    @Override
    public Row currentRow() {
        raiseIfLoading();
        return currentRow;
    }

    @Override
    public void close() {
        source.close();
    }

    @Override
    public CompletionStage<?> loadNextBatch() {
        if (resultFuture == null) {
            resultFuture = BatchRowVisitor.visitRows(source, collector)
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

    private void raiseIfLoading() {
        if (resultFuture != null && resultFuture.isDone() == false) {
            throw new IllegalStateException("BatchIterator is loading");
        }
    }
}
