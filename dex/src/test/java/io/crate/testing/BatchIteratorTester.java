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

import io.crate.data.BatchIterator;
import io.crate.data.BatchIterators;
import io.crate.data.Row;
import io.crate.exceptions.Exceptions;
import org.hamcrest.Matchers;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static io.crate.concurrent.CompletableFutures.failedFuture;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.fail;

/**
 * A class which can be used to verify that a {@link io.crate.data.BatchIterator} implements
 * all contracts correctly
 */
public class BatchIteratorTester {

    private final Supplier<BatchIterator<Row>> it;

    public BatchIteratorTester(Supplier<BatchIterator<Row>> it) {
        this.it = it;
    }

    public void verifyResultAndEdgeCaseBehaviour(List<Object[]> expectedResult) throws Exception {
        testProperConsumption(it.get(), expectedResult);
        testBehaviourAfterClose(it.get());
        testIteratorAccessFromDifferentThreads(it.get(), expectedResult);
        testIllegalNextBatchCall(it.get());
        testMoveNextAfterMoveNextReturnedFalse(it.get());
        testMoveToStartAndReConsumptionMatchesRowsOnFirstConsumption(it.get());
        testAllLoadedNeverRaises(it);
    }

    private void testAllLoadedNeverRaises(Supplier<BatchIterator<Row>> batchIterator) {
        BatchIterator<Row> bi = batchIterator.get();
        bi.allLoaded();
        bi.close();
        bi.allLoaded();

        bi = batchIterator.get();
        bi.kill(new InterruptedException("KILLED"));
        bi.allLoaded();
    }

    private void testMoveToStartAndReConsumptionMatchesRowsOnFirstConsumption(BatchIterator<Row> it) throws Exception {
        List<Object[]> firstResult = BatchIterators.collect(
            it, Collectors.mapping(Row::materialize, Collectors.toList())).get(10, TimeUnit.SECONDS);

        it.moveToStart();

        List<Object[]> secondResult = BatchIterators.collect(
            it, Collectors.mapping(Row::materialize, Collectors.toList())).get(10, TimeUnit.SECONDS);
        it.close();
        checkResult(firstResult, secondResult);
    }

    private void testMoveNextAfterMoveNextReturnedFalse(BatchIterator<Row> it) throws Exception {
        TestingRowConsumer.moveToEnd(it).toCompletableFuture().get(10, TimeUnit.SECONDS);
        assertThat(it.moveNext(), is(false));
        it.close();
    }

    private void testIllegalNextBatchCall(BatchIterator<Row> it) throws Exception {
        while (!it.allLoaded()) {
            it.loadNextBatch().toCompletableFuture().get(10, TimeUnit.SECONDS);
        }
        CompletionStage<?> completionStage = it.loadNextBatch();
        assertThat(completionStage.toCompletableFuture().isCompletedExceptionally(), is(true));
        it.close();
    }

    private void testIteratorAccessFromDifferentThreads(BatchIterator<Row> it, List<Object[]> expectedResult) throws Exception {
        if (expectedResult.size() < 2) {
            it.close();
            return;
        }
        ExecutorService executor = Executors.newFixedThreadPool(3);
        try {
            CompletableFuture<Object[]> firstRow = CompletableFuture.supplyAsync(() -> {
                Row firstElement = getFirstElement(it);
                assertThat("it should have at least two rows, first missing", firstElement, Matchers.notNullValue());
                return firstElement.materialize();
            }, executor);
            CompletableFuture<Object[]> secondRow = firstRow.thenApplyAsync(row -> {
                Row firstElement = getFirstElement(it);
                assertThat("it should have at least two rows", firstElement, Matchers.notNullValue());
                return firstElement.materialize();
            }, executor);

            assertThat(firstRow.get(10, TimeUnit.SECONDS), is(expectedResult.get(0)));
            assertThat(secondRow.get(10, TimeUnit.SECONDS), is(expectedResult.get(1)));
        } finally {
            executor.shutdownNow();
            executor.awaitTermination(5, TimeUnit.SECONDS);
            it.close();
        }
    }

    private static <T> T getFirstElement(BatchIterator<T> it) {
        try {
            return getFirstElementFuture(it).get(10, TimeUnit.SECONDS);
        } catch (Exception e) {
            Exceptions.rethrowUnchecked(e);
            return null;
        }
    }

    private static <T> CompletableFuture<T> getFirstElementFuture(BatchIterator<T> it) {
        if (it.moveNext()) {
            return CompletableFuture.completedFuture(it.currentElement());
        }
        if (it.allLoaded()) {
            return failedFuture(new IllegalStateException("Iterator is exhausted"));
        }
        return it.loadNextBatch()
            .thenCompose(r -> getFirstElementFuture(it))
            .toCompletableFuture();
    }


    private void testBehaviourAfterClose(BatchIterator<Row> it) {
        it.close();
        assertThat("currentElement is not affected by close", it.currentElement(), is(it.currentElement()));

        expectFailure(it::moveNext, IllegalStateException.class, "moveNext must fail after close");
        expectFailure(it::moveToStart, IllegalStateException.class, "moveToStart must fail after close");
    }

    private void testProperConsumption(BatchIterator<Row> it, List<Object[]> expectedResult) throws Exception {
        TestingRowConsumer consumer = new TestingRowConsumer();
        consumer.accept(it, null);

        List<Object[]> result = consumer.getResult();
        checkResult(expectedResult, result);
    }

    private static void checkResult(List<Object[]> expected, List<Object[]> actual) {
        if (expected.isEmpty()) {
            assertThat(actual, empty());
        } else {
            assertThat(actual, Matchers.contains(expected.toArray(new Object[0])));
        }
    }

    private static void expectFailure(Runnable runnable,
                                      Class<? extends Exception> expectedException,
                                      String reason) {
        try {
            runnable.run();
            fail(reason);
        } catch (Exception e) {
            assertThat(e, instanceOf(expectedException));
        }
    }
}
