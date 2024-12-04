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

package io.crate.data.testing;


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
import java.util.stream.Collectors;

import org.jetbrains.annotations.Nullable;

import io.crate.common.exceptions.Exceptions;
import io.crate.data.BatchIterator;
import io.crate.data.Row;

/**
 * A class which can be used to verify that a {@link io.crate.data.BatchIterator} implements
 * all contracts correctly
 */
public class BatchIteratorTester<T> {

    private final Supplier<BatchIterator<T>> it;
    private final ResultOrder resultOrder;

    public enum ResultOrder {
        /**
         * Result must match in exact order
         **/
        EXACT,

        /**
         * Result can match in any order
         **/
        ANY
    }

    public static BatchIteratorTester<Object[]> forRows(Supplier<BatchIterator<Row>> it, ResultOrder resultOrder) {
        return new BatchIteratorTester<>(
            () -> it.get().map(row -> row == null ? null : row.materialize()),
            resultOrder
        );
    }

    public BatchIteratorTester(Supplier<BatchIterator<T>> it, ResultOrder resultOrder) {
        this.it = it;
        this.resultOrder = resultOrder;
    }

    public void verifyResultAndEdgeCaseBehaviour(List<T> expectedResult,
                                                 @Nullable Consumer<BatchIterator<T>> verifyAfterProperConsumption) throws Exception {
        BatchIterator<T> firstBatchIterator = this.it.get();
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

    private void testLoadNextBatchFutureCompletesOnKill(BatchIterator<T> bi) throws Exception {
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

    public void verifyResultAndEdgeCaseBehaviour(List<T> expectedResult) throws Exception {
        verifyResultAndEdgeCaseBehaviour(expectedResult, null);
    }

    private void testAllLoadedNeverRaises(Supplier<BatchIterator<T>> batchIterator) {
        BatchIterator<T> bi = batchIterator.get();
        bi.allLoaded();
        bi.close();
        bi.allLoaded();

        bi = batchIterator.get();
        bi.kill(new InterruptedException("KILLED"));
        bi.allLoaded();
    }

    private void testMoveToStartAndReConsumptionMatchesRowsOnFirstConsumption(BatchIterator<T> it) throws Exception {
        List<T> firstResult = it.collect(Collectors.toList(), false).get(5, TimeUnit.SECONDS);
        it.moveToStart();
        List<T> secondResult = it.collect(Collectors.toList(), false).get(5, TimeUnit.SECONDS);
        it.close();
        checkResult(firstResult, secondResult, resultOrder);
    }

    private void testMoveNextAfterMoveNextReturnedFalse(BatchIterator<T> it) throws Exception {
        it.collect(Collectors.counting(), false).get(10, TimeUnit.SECONDS);
        assertThat(it.moveNext()).isFalse();
        it.close();
    }

    private void testIllegalNextBatchCall(BatchIterator<T> it) throws Exception {
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

    private void testIteratorAccessFromDifferentThreads(BatchIterator<T> it, List<T> expectedResult) throws Exception {
        if (expectedResult.size() < 2) {
            it.close();
            return;
        }
        ExecutorService executor = Executors.newFixedThreadPool(3);
        try {
            CompletableFuture<T> firstRow = CompletableFuture.supplyAsync(() -> {
                T firstElement = getFirstElement(it);
                assertThat(firstElement)
                    .as("it should have at least two rows, first missing")
                    .isNotNull();
                return firstElement;
            }, executor);
            CompletableFuture<T> secondRow = firstRow.thenApplyAsync(row -> {
                T firstElement = getFirstElement(it);
                assertThat(firstElement)
                    .as("it should have at least two rows")
                    .isNotNull();
                return firstElement;
            }, executor);

            T firstItem = firstRow.get(10, TimeUnit.SECONDS);
            T secondItem = secondRow.get(10, TimeUnit.SECONDS);
            assertThat(expectedResult).contains(firstItem);
            assertThat(expectedResult).contains(secondItem);

            List<T> result = it.toList().get(5, TimeUnit.SECONDS);
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


    private void testBehaviourAfterClose(BatchIterator<T> it) {
        it.close();
        assertThat(it.currentElement())
            .as("currentElement is not affected by close")
            .isEqualTo(it.currentElement());

        expectFailure(it::moveNext, IllegalStateException.class, "moveNext must fail after close");
        expectFailure(it::moveToStart, IllegalStateException.class, "moveToStart must fail after close");
    }

    private void testBehaviourAfterKill(BatchIterator<T> it) {
        it.kill(new InterruptedException("job killed"));
        assertThat(it.currentElement())
            .as("currentElement is not affected by kill")
            .isEqualTo(it.currentElement());

        expectFailure(it::moveNext, InterruptedException.class, "moveNext must fail after kill");
    }

    private void testProperConsumption(BatchIterator<T> it, List<T> expectedResult) throws Exception {
        List<T> result = it.toList().get(5, TimeUnit.SECONDS);
        checkResult(expectedResult, result, resultOrder);
    }

    private static <T> void checkResult(List<T> expected, List<T> actual, ResultOrder resultOrder) {
        if (expected.isEmpty()) {
            assertThat(actual).isEmpty();
        } else if (resultOrder == ResultOrder.EXACT) {
            assertThat(actual).containsExactlyElementsOf(expected);
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
