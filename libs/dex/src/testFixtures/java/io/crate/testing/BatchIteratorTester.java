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

package io.crate.testing;


import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.jetbrains.annotations.Nullable;

import io.crate.data.BatchIterator;
import io.crate.data.Row;
import io.crate.exceptions.Exceptions;

/**
 * A class which can be used to verify that a {@link io.crate.data.BatchIterator} implements
 * all contracts correctly
 */
public class BatchIteratorTester {

    private final Supplier<BatchIterator<Row>> it;

    public BatchIteratorTester(Supplier<BatchIterator<Row>> it) {
        this.it = it;
    }

    public void verifyResultAndEdgeCaseBehaviour(List<Object[]> expectedResult,
                                                 @Nullable Consumer<BatchIterator<Row>> verifyAfterProperConsumption) throws Exception {
        BatchIterator<Row> firstBatchIterator = this.it.get();
        testProperConsumption(firstBatchIterator, expectedResult);
        if (verifyAfterProperConsumption != null) {
            verifyAfterProperConsumption.accept(firstBatchIterator);
        }
        testBehaviourAfterClose(this.it.get());
        testBehaviourAfterKill(this.it.get());
        testIteratorAccessFromDifferentThreads(this.it.get(), expectedResult);
        testIllegalNextBatchCall(this.it.get());
        testMoveNextAfterMoveNextReturnedFalse(this.it.get());
        testMoveToStartAndReConsumptionMatchesRowsOnFirstConsumption(this.it.get());
        testAllLoadedNeverRaises(this.it);
        testLoadNextBatchFutureCompletesOnKill(this.it.get());
    }

    private void testLoadNextBatchFutureCompletesOnKill(BatchIterator<Row> bi) throws Exception {
        if (bi.allLoaded()) {
            return;
        }
        InterruptedException kill = new InterruptedException("KILL");
        CompletionStage<?> f = bi.loadNextBatch();
        bi.kill(kill);
        try {
            f.toCompletableFuture().get(5, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            assertThat(cause).isEqualTo(kill);
        }
    }

    public void verifyResultAndEdgeCaseBehaviour(List<Object[]> expectedResult) throws Exception {
        verifyResultAndEdgeCaseBehaviour(expectedResult, null);
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
        var firstConsumer = new TestingRowConsumer(false);
        firstConsumer.accept(it, null);
        final List<Object[]> firstResult = firstConsumer.getResult();

        it.moveToStart();

        var secondConsumer = new TestingRowConsumer(false);
        secondConsumer.accept(it, null);

        List<Object[]> secondResult = secondConsumer.getResult();
        it.close();
        checkResult(firstResult, secondResult);
    }

    private void testMoveNextAfterMoveNextReturnedFalse(BatchIterator<Row> it) throws Exception {
        TestingRowConsumer.moveToEnd(it).toCompletableFuture().get(10, TimeUnit.SECONDS);
        assertThat(it.moveNext()).isFalse();
        it.close();
    }

    private void testIllegalNextBatchCall(BatchIterator<Row> it) throws Exception {
        while (!it.allLoaded()) {
            it.loadNextBatch().toCompletableFuture().get(10, TimeUnit.SECONDS);
        }
        try {
            it.loadNextBatch();
            fail("loadNextBatch call should throw an exception if called after all is loaded, got none");
        } catch (Exception ignored) {
            // ignore
        }
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
                assertThat(firstElement)
                    .as("it should have at least two rows, first missing")
                    .isNotNull();
                return firstElement.materialize();
            }, executor);
            CompletableFuture<Object[]> secondRow = firstRow.thenApplyAsync(row -> {
                Row firstElement = getFirstElement(it);
                assertThat(firstElement)
                    .as("it should have at least two rows")
                    .isNotNull();
                return firstElement.materialize();
            }, executor);

            Object[] firstItem = firstRow.get(10, TimeUnit.SECONDS);
            Object[] secondItem = secondRow.get(10, TimeUnit.SECONDS);
            assertThat(expectedResult).contains(firstItem);
            assertThat(expectedResult).contains(secondItem);

            // retrieve and check the remaining items
            TestingRowConsumer consumer = new TestingRowConsumer();
            consumer.accept(it, null);
            List<Object[]> result = consumer.getResult();
            assertThat(result).hasSize(expectedResult.size() - 2);
            result.add(firstItem);
            result.add(secondItem);
            assertThat(expectedResult).containsExactlyInAnyOrderElementsOf(result);
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
            return CompletableFuture.failedFuture(new IllegalStateException("Iterator is exhausted"));
        }
        try {
            return it.loadNextBatch()
                .thenCompose(r -> getFirstElementFuture(it))
                .toCompletableFuture();
        } catch (Throwable t) {
            return CompletableFuture.failedFuture(t);
        }
    }


    private void testBehaviourAfterClose(BatchIterator<Row> it) {
        it.close();
        assertThat(it.currentElement())
            .as("currentElement is not affected by close")
            .isEqualTo(it.currentElement());

        expectFailure(it::moveNext, IllegalStateException.class, "moveNext must fail after close");
        expectFailure(it::moveToStart, IllegalStateException.class, "moveToStart must fail after close");
    }

    private void testBehaviourAfterKill(BatchIterator<Row> it) {
        it.kill(new InterruptedException("job killed"));
        assertThat(it.currentElement())
            .as("currentElement is not affected by kill")
            .isEqualTo(it.currentElement());

        expectFailure(it::moveNext, InterruptedException.class, "moveNext must fail after kill");
    }

    private void testProperConsumption(BatchIterator<Row> it, List<Object[]> expectedResult) throws Exception {
        TestingRowConsumer consumer = new TestingRowConsumer();
        consumer.accept(it, null);

        List<Object[]> result = consumer.getResult();
        checkResult(expectedResult, result);
    }

    private static void checkResult(List<Object[]> expected, List<Object[]> actual) {
        if (expected.isEmpty()) {
            assertThat(actual).isEmpty();
        } else {
            assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);
        }
    }

    private static void expectFailure(Runnable runnable,
                                      Class<? extends Exception> expectedException,
                                      String reason) {
        assertThatThrownBy(runnable::run).as(reason).isExactlyInstanceOf(expectedException);
    }
}
