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

package io.crate.execution.engine.distribution.merge;

import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.crate.common.concurrent.KillableCompletionStage;
import io.crate.data.BatchIterator;
import io.crate.data.Row;
import io.crate.data.RowN;
import io.crate.data.testing.BatchIteratorTester;
import io.crate.data.testing.BatchSimulatingIterator;
import io.crate.data.testing.RowGenerator;
import io.crate.data.testing.TestingBatchIterators;

public class BatchPagingIteratorTest {

    private ExecutorService executor;

    @Before
    public void setUp() throws Exception {
        executor = Executors.newFixedThreadPool(2);
    }

    @After
    public void tearDown() throws Exception {
        executor.shutdown();
        executor.awaitTermination(5, TimeUnit.SECONDS);
    }

    @Test
    public void testBatchPagingIterator() throws Exception {
        Iterable<Row> rows = RowGenerator.range(0, 3);
        List<Object[]> expectedResult = StreamSupport.stream(rows.spliterator(), false)
            .map(Row::materialize)
            .collect(Collectors.toList());
        var tester = BatchIteratorTester.forRows(() -> {
            PassThroughPagingIterator<Integer, Row> pagingIterator = PassThroughPagingIterator.repeatable();
            pagingIterator.merge(singletonList(new KeyIterable<>(0, rows)));
            return new BatchPagingIterator<>(
                pagingIterator,
                exhaustedIt -> KillableCompletionStage.failed(new IllegalStateException("upstreams exhausted")),
                () -> true,
                throwable -> {}
            );
        });
        tester.verifyResultAndEdgeCaseBehaviour(expectedResult);
    }

    @Test
    public void testBatchPagingIteratorWithPagedSource() throws Exception {
        List<Object[]> expectedResult = StreamSupport.stream(RowGenerator.range(0, 10).spliterator(), false)
            .map(Row::materialize)
            .collect(Collectors.toList());

        Supplier<BatchIterator<Row>> batchIteratorSupplier = () -> {
            BatchSimulatingIterator<Row> source =
                new BatchSimulatingIterator<>(TestingBatchIterators.range(0, 10), 2, 5, executor);
            Function<Integer, KillableCompletionStage<? extends Iterable<? extends KeyIterable<Integer, Row>>>> fetchMore = exhausted -> {
                List<Row> rows = new ArrayList<>();
                while (source.moveNext()) {
                    rows.add(new RowN(source.currentElement().materialize()));
                }
                if (source.allLoaded()) {
                    return KillableCompletionStage.whenKilled(
                        CompletableFuture.completedFuture(singletonList(new KeyIterable<>(1, rows))),
                        t -> {});
                }
                // this is intentionally not recursive to not consume the whole source in the first `fetchMore` call
                // but to simulate multiple pages and fetchMore calls
                try {
                    return KillableCompletionStage.whenKilled(
                        source.loadNextBatch().toCompletableFuture().thenApply(ignored -> {
                            while (source.moveNext()) {
                                rows.add(new RowN(source.currentElement().materialize()));
                            }
                            return singleton(new KeyIterable<>(1, rows));
                        }),
                        t -> {});
                } catch (Exception e) {
                    return KillableCompletionStage.failed(e);
                }
            };
            return new BatchPagingIterator<>(
                PassThroughPagingIterator.repeatable(),
                fetchMore,
                source::allLoaded,
                throwable -> {
                    if (throwable == null) {
                        source.close();
                    } else {
                        source.kill(throwable);
                    }
                }
            );
        };
        var tester = BatchIteratorTester.forRows(batchIteratorSupplier);
        tester.verifyResultAndEdgeCaseBehaviour(expectedResult);
    }

    @Test
    public void testFinishPagingIteratorOnClose() {
        TestPagingIterator pagingIterator = new TestPagingIterator();
        BatchPagingIterator<Integer> iterator = new BatchPagingIterator<>(
            pagingIterator,
            exhaustedIt -> KillableCompletionStage.failed(new IllegalStateException("upstreams exhausted")),
            () -> true,
            throwable -> {}
        );

        iterator.close();
        assertThat(pagingIterator.finishedCalled, Matchers.is(true));
    }

    private static class TestPagingIterator implements PagingIterator<Integer, Row> {

        boolean finishedCalled = false;

        @Override
        public void merge(Iterable<? extends KeyIterable<Integer, Row>> keyIterables) {

        }

        @Override
        public void finish() {
            finishedCalled = true;
        }

        @Override
        public Integer exhaustedIterable() {
            return null;
        }

        @Override
        public Iterable<Row> repeat() {
            return null;
        }

        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public Row next() {
            return null;
        }
    }
}
