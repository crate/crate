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
import io.crate.exceptions.Exceptions;
import org.hamcrest.Matchers;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.function.Supplier;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.fail;

/**
 * A class which can be used to verify that a {@link io.crate.data.BatchIterator} implements
 * all contracts correctly
 */
public class BatchIteratorTester {

    private final Supplier<BatchIterator> it;
    private final List<Object[]> expectedResult;

    public BatchIteratorTester(Supplier<BatchIterator> it, List<Object[]> expectedResult) {
        this.it = it;
        this.expectedResult = expectedResult;
    }

    public void run() throws Exception {
        testProperConsumption(it.get());
        testFailsIfClosed(it.get());
        testIteratorAccessFromDifferentThreads(it.get());
        testIllegalNextBatchCall(it.get());
        testCurrentRowIsInvalidAfterMoveToFirst(it.get());
        testMoveNextAfterMoveNextReturnedFalse(it.get());
        testAccessRowAfterMoveNextReturnedFalse(it.get());
        testIllegalStateIsRaisedIfMoveIsCalledWhileLoadingNextBatch(it.get());
        testMoveToStartAndReConsumptionMatchesRowsOnFirstConsumption(it.get());
    }

    private void testMoveToStartAndReConsumptionMatchesRowsOnFirstConsumption(BatchIterator it) throws Exception {
        List<Object[]> firstResult = new ArrayList<>();
        BatchRowVisitor.visitRows(it, r -> firstResult.add(r.materialize())).get(10, TimeUnit.SECONDS);

        it.moveToStart();

        List<Object[]> secondResult = new ArrayList<>();
        BatchRowVisitor.visitRows(it, r -> secondResult.add(r.materialize())).get(10, TimeUnit.SECONDS);
        assertThat(firstResult, Matchers.contains(secondResult.toArray(new Object[0][])));
    }

    private void testIllegalStateIsRaisedIfMoveIsCalledWhileLoadingNextBatch(BatchIterator it) {
        while (it.allLoaded() == false) {
            CompletableFuture<?> nextBatchFuture = it.loadNextBatch().toCompletableFuture();
            while (nextBatchFuture.isDone() == false) {
                try {
                    it.moveNext();
                    if (nextBatchFuture.isDone()) {
                        // race condition, future completed too fast.
                    } else {
                        fail("moveNext must raise an IllegalStateException during loadNextBatch");
                    }
                } catch (IllegalStateException ignored) {
                    // expected, pass
                }
            }
        }
    }

    private void testCurrentRowIsInvalidAfterMoveToFirst(BatchIterator it) throws Exception {
        CollectingBatchConsumer.moveToEnd(it).toCompletableFuture().get(10, TimeUnit.SECONDS);
        it.moveToStart();
        assertMaterializeFails(it);
    }

    private void testAccessRowAfterMoveNextReturnedFalse(BatchIterator it) {
        CollectingBatchConsumer consumer = new CollectingBatchConsumer();
        consumer.accept(it, null);

        assertMaterializeFails(it);
    }

    private void assertMaterializeFails(BatchIterator it) {
        expectFailure(
            () -> it.currentRow().materialize(),
            IllegalStateException.class,
            "materialize call should fail if moveNext returned false");

    }

    private void testMoveNextAfterMoveNextReturnedFalse(BatchIterator it) throws Exception {
        CollectingBatchConsumer.moveToEnd(it).toCompletableFuture().get(10, TimeUnit.SECONDS);
        assertThat(it.moveNext(), is(false));
    }

    private void testIllegalNextBatchCall(BatchIterator it) throws Exception {
        while (it.allLoaded() == false) {
            it.loadNextBatch().toCompletableFuture().get(10, TimeUnit.SECONDS);
        }
        CompletionStage<?> completionStage = it.loadNextBatch();
        assertThat(completionStage.toCompletableFuture().isCompletedExceptionally(), is(true));
    }

    private void testIteratorAccessFromDifferentThreads(BatchIterator it) throws Exception {
        if (expectedResult.size() < 2) {
            return;
        }
        ExecutorService executor = Executors.newFixedThreadPool(3);
        try {
            CompletableFuture<Object[]> firstRow = CompletableFuture.supplyAsync(() -> {
                assertThat("it should have at least two rows, first missing", getBatchAwareMoveNext(it), is(true));
                return it.currentRow().materialize();
            }, executor);
            CompletableFuture<Object[]> secondRow = firstRow.thenApplyAsync(row -> {
                assertThat("it should have at least two rows", getBatchAwareMoveNext(it), is(true));
                return it.currentRow().materialize();
            }, executor);

            assertThat(firstRow.get(10, TimeUnit.SECONDS), is(expectedResult.get(0)));
            assertThat(secondRow.get(10, TimeUnit.SECONDS), is(expectedResult.get(1)));
        } finally {
            executor.shutdownNow();
            executor.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    private static boolean getBatchAwareMoveNext(BatchIterator it) {
        try {
            return batchAwareMoveNext(it).get(10, TimeUnit.SECONDS);
        } catch (Exception e) {
            Exceptions.rethrowUnchecked(e);
            return false;
        }
    }

    private static CompletableFuture<Boolean> batchAwareMoveNext(BatchIterator it) {
        if (it.moveNext()) {
            return CompletableFuture.completedFuture(true);
        }
        if (it.allLoaded()) {
            return CompletableFuture.completedFuture(false);
        }
        return it.loadNextBatch()
            .thenCompose(r -> batchAwareMoveNext(it))
            .toCompletableFuture();
    }

    private void testFailsIfClosed(BatchIterator it) {
        it.close();
        expectFailure(it::moveNext, IllegalStateException.class, "moveNext must fail after close");
        expectFailure(it::moveToStart, IllegalStateException.class, "moveToStart must fail after close");
        expectFailure(it::currentRow, IllegalStateException.class, "currentRow must fail after close");
        expectFailure(it::allLoaded, IllegalStateException.class, "allLoaded must fail after close");
    }

    private void testProperConsumption(BatchIterator it) {
        CollectingBatchConsumer consumer = new CollectingBatchConsumer();
        consumer.accept(it, null);

        List<Object[]> result = consumer.getResult();
        assertThat(result, Matchers.contains(expectedResult.toArray(new Object[0])));
    }

    private static void expectFailure(Runnable runnable,
                                      Class<? extends Exception> expectedException,
                                      String reason) {
        try {
            runnable.run();
            fail(reason);
        } catch (Exception e) {
            assertThat(expectedException.isInstance(e), is(true));
        }
    }
}
