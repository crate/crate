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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import io.crate.data.testing.BatchIteratorTester;
import io.crate.data.testing.BatchSimulatingIterator;
import io.crate.data.testing.TestingBatchIterators;

public class AsyncFlatMapBatchIteratorTest {

    @SafeVarargs
    private static <T> CloseableIterator<T> mkIter(T... rows) {
        return CloseableIterator.fromIterator(Arrays.asList(rows).iterator());
    }

    @Test
    public void test_async_flat_map_on_fully_loaded_source() throws Exception {
        InMemoryBatchIterator<Integer> source = new InMemoryBatchIterator<>(Arrays.asList(1, 2, 3), null, false);
        AsyncFlatMapBatchIterator<Integer, Integer[]> twiceAsArray = new AsyncFlatMapBatchIterator<>(
            source,
            (x, isLast) -> CompletableFuture.completedFuture(mkIter(new Integer[] {x, x}, new Integer[] {x, x}))
        );
        List<Integer[]> integers = twiceAsArray.toList().get(1, TimeUnit.SECONDS);
        assertThat(integers).containsExactly(new Integer[] {1, 1},
                                             new Integer[] {1, 1},
                                             new Integer[] {2, 2},
                                             new Integer[] {2, 2},
                                             new Integer[] {3, 3},
                                             new Integer[] {3, 3});
    }

    @Test
    public void test_async_flatMap_does_not_fail_if_consumer_calls_moveNext_after_negative_moveNext_result() {
        InMemoryBatchIterator<Integer> source = new InMemoryBatchIterator<>(Arrays.asList(1, 2, 3), null, false);
        var asyncFlatMap = new AsyncFlatMapBatchIterator<>(
            source,
            (x, isLast) -> CompletableFuture.completedFuture(mkIter(new Integer[] {x, x}, new Integer[] {x, x}))
        );
        assertThat(asyncFlatMap.moveNext()).as(
            "first moveNext must return false, because the async-mapper must run next").isFalse();
        assertThat(asyncFlatMap.moveNext()).as("Calling moveNext again must not fail").isFalse();
    }

    @Test
    public void test_async_flatMap_on_source_that_has_batches() throws Exception {
        ExecutorService executorService = Executors.newFixedThreadPool(3);
        var source = new BatchSimulatingIterator<>(
            new InMemoryBatchIterator<>(Arrays.asList(1, 2, 3), null, false),
            2,
            1,
            executorService
        );
        try {
            AsyncFlatMapBatchIterator<Integer, Integer[]> twiceAsArray = new AsyncFlatMapBatchIterator<>(
                source,
                (x, istLast) -> CompletableFuture.completedFuture(mkIter(new Integer[] {x, x}, new Integer[] {x, x}))
            );
            List<Integer[]> integers = twiceAsArray.toList().get(1, TimeUnit.SECONDS);
            assertThat(integers).containsExactly(new Integer[] {1, 1},
                                                 new Integer[] {1, 1},
                                                 new Integer[] {2, 2},
                                                 new Integer[] {2, 2},
                                                 new Integer[] {3, 3},
                                                 new Integer[] {3, 3});
        } finally {
            executorService.shutdown();
            executorService.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testFlatMapBatchIteratorFullFillsContracts() throws Exception {
        AsyncFlatMapper<Row, Row> duplicateRow = (row, isLast) ->
            CompletableFuture.completedFuture(mkIter(
                                                  new RowN(row.materialize()),
                                                  new RowN(row.materialize())
                                              )
            );
        var tester = BatchIteratorTester.forRows(() -> {
            BatchIterator<Row> source = TestingBatchIterators.range(1, 4);
            return new AsyncFlatMapBatchIterator<>(source, duplicateRow);
        });
        tester.verifyResultAndEdgeCaseBehaviour(
            Arrays.asList(
                new Object[] { 1 },
                new Object[] { 1 },
                new Object[] { 2 },
                new Object[] { 2 },
                new Object[] { 3 },
                new Object[] { 3 }
            )
        );
    }

    @Test
    public void test_repeats_source_after_intermediate_move_to_start() throws Exception {
        var source = TestingBatchIterators.range(1, 4);
        AsyncFlatMapper<Row, Row> duplicateRow = (row, isLast) -> {
            CloseableIterator<Row> result = mkIter(
                new RowN(row.materialize()),
                new RowN(row.materialize())
            );
            return CompletableFuture.completedFuture(result);
        };
        var it = new AsyncFlatMapBatchIterator<>(source, duplicateRow);
        assertThat(it.moveNext()).as("needs to load source first").isFalse();
        assertThat(it.loadNextBatch()).succeedsWithin(1, TimeUnit.SECONDS);
        // first duplicated row
        assertThat(it.moveNext()).isTrue();
        assertThat(it.currentElement().get(0)).isEqualTo(1);
        assertThat(it.moveNext()).isTrue();
        assertThat(it.currentElement().get(0)).isEqualTo(1);
        assertThat(it.moveNext()).isFalse();

        // load second duplicated row
        assertThat(it.loadNextBatch()).succeedsWithin(1, TimeUnit.SECONDS);
        assertThat(it.moveNext()).isTrue();
        assertThat(it.currentElement().get(0)).isEqualTo(2);

        it.moveToStart();
        // needs to start with first row again
        assertThat(it.moveNext()).isFalse();
        assertThat(it.loadNextBatch()).succeedsWithin(1, TimeUnit.SECONDS);
        assertThat(it.moveNext()).isTrue();
        assertThat(it.currentElement().get(0))
            .as("first row after moveToStart must contain 1")
            .isEqualTo(1);
    }

    @Test
    public void test_calls_mapper_with_isLast_set_to_false_after_move_to_start() throws Exception {
        AtomicInteger numCalls = new AtomicInteger();
        AtomicBoolean wasLast = new AtomicBoolean();
        AsyncFlatMapper<Row, Row> duplicateRow = (row, isLast) -> {
            wasLast.set(isLast);
            CloseableIterator<Row> result = mkIter(
                new RowN(row.materialize()),
                new RowN(row.materialize())
            );
            numCalls.incrementAndGet();
            return CompletableFuture.completedFuture(result);
        };
        var source = TestingBatchIterators.range(1, 2);
        var it = new AsyncFlatMapBatchIterator<>(source, duplicateRow);

        assertThat(it.moveNext()).isFalse();
        assertThat(it.loadNextBatch()).succeedsWithin(1, TimeUnit.SECONDS);
        assertThat(numCalls).hasValue(1);
        assertThat(wasLast).isFalse();
        assertThat(it.moveNext()).isTrue();
        assertThat(it.moveNext()).isTrue();
        assertThat(it.moveNext()).isFalse();
        assertThat(it.allLoaded()).isTrue();

        it.moveToStart();
        it.moveNext();
        assertThat(it.loadNextBatch()).succeedsWithin(1, TimeUnit.SECONDS);
        assertThat(wasLast).as("move to start needs to reset sourceExhausted").isFalse();
    }
}
