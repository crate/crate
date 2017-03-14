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

import io.crate.data.*;
import io.crate.exceptions.Exceptions;
import junit.framework.TestCase;
import org.hamcrest.Matchers;

import java.util.List;
import java.util.concurrent.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.fail;

/**
 * A class which can be used to verify that a {@link io.crate.data.BatchIterator} implements
 * all contracts correctly
 */
public class BatchIteratorTester {

    private final Supplier<BatchIterator> it;

    public BatchIteratorTester(Supplier<BatchIterator> it) {
        this.it = it;
    }

    private static void assertIndexOutOfBounds(Columns inputs, int index) {
        try {
            inputs.get(index);
            TestCase.fail("expected a IndexOutOfBoundsException but call did not fail");
        } catch (Throwable ex) {
            assertThat(ex, instanceOf(IndexOutOfBoundsException.class));
        }

    }

    private static Columns assertValidRowData(BatchIterator it) {
        return assertValidColumns(it::rowData);
    }

    public static Columns assertValidColumns(Supplier<Columns> supplier) {
        Columns first = supplier.get();
        assertThat(first, notNullValue());
        Columns second = supplier.get();
        assertThat(first, sameInstance(second));

        List<Input<?>> inputs = IntStream.range(0, first.size()).mapToObj(first::get).collect(Collectors.toList());
        int pos = -1;
        for (Input<?> input : first) {
            pos++;
            assertThat(input, sameInstance(inputs.get(pos)));
            assertThat(first.get(pos), sameInstance(inputs.get(pos)));
            assertThat(second.get(pos), sameInstance(inputs.get(pos)));
        }
        assertThat(pos, is(first.size() - 1));
        assertIndexOutOfBounds(first, pos + 1);
        assertIndexOutOfBounds(first, -1);
        return first;
    }

    public void verifyResultAndEdgeCaseBehaviour(List<Object[]> expectedResult) throws Exception {
        testProperConsumption(it.get(), expectedResult);
        testBehaviourAfterClose(it.get());
        testIteratorAccessFromDifferentThreads(it.get(), expectedResult);
        testIllegalNextBatchCall(it.get());
        testMoveNextAfterMoveNextReturnedFalse(it.get());
        testIllegalStateIsRaisedIfMoveIsCalledWhileLoadingNextBatch(it.get());
        testMoveToStartAndReConsumptionMatchesRowsOnFirstConsumption(it.get());
        testColumnsBehaviour(it.get());
    }

    private void testMoveToStartAndReConsumptionMatchesRowsOnFirstConsumption(BatchIterator it) throws Exception {
        List<Object[]> firstResult = BatchRowVisitor.visitRows(
            it, Collectors.mapping(Row::materialize, Collectors.toList())).get(10, TimeUnit.SECONDS);

        it.moveToStart();

        List<Object[]> secondResult = BatchRowVisitor.visitRows(
            it, Collectors.mapping(Row::materialize, Collectors.toList())).get(10, TimeUnit.SECONDS);
        checkResult(firstResult, secondResult);
        //assertThat(firstResult, Matchers.contains(secondResult.toArray(new Object[0][])));
    }

    private void testIllegalStateIsRaisedIfMoveIsCalledWhileLoadingNextBatch(BatchIterator it) {
        while (!it.allLoaded()) {
            CompletableFuture<?> nextBatchFuture = it.loadNextBatch().toCompletableFuture();
            while (!nextBatchFuture.isDone()) {
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

    private void testMoveNextAfterMoveNextReturnedFalse(BatchIterator it) throws Exception {
        TestingBatchConsumer.moveToEnd(it).toCompletableFuture().get(10, TimeUnit.SECONDS);
        assertThat(it.moveNext(), is(false));
    }

    private void testIllegalNextBatchCall(BatchIterator it) throws Exception {
        while (!it.allLoaded()) {
            it.loadNextBatch().toCompletableFuture().get(10, TimeUnit.SECONDS);
        }
        CompletionStage<?> completionStage = it.loadNextBatch();
        assertThat(completionStage.toCompletableFuture().isCompletedExceptionally(), is(true));
    }

    private void testIteratorAccessFromDifferentThreads(BatchIterator it, List<Object[]> expectedResult) throws Exception {
        if (expectedResult.size() < 2) {
            return;
        }
        ExecutorService executor = Executors.newFixedThreadPool(3);
        final Columns inputs = it.rowData();
        try {
            CompletableFuture<Object[]> firstRow = CompletableFuture.supplyAsync(() -> {
                assertThat("it should have at least two rows, first missing", getBatchAwareMoveNext(it), is(true));
                return RowBridging.materialize(inputs);
            }, executor);
            CompletableFuture<Object[]> secondRow = firstRow.thenApplyAsync(row -> {
                assertThat("it should have at least two rows", getBatchAwareMoveNext(it), is(true));
                return RowBridging.materialize(inputs);
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

    private void testColumnsBehaviour(BatchIterator it) {
        Columns inputs = assertValidRowData(it);
        assertThat(it.rowData(), sameInstance(inputs));
        it.moveNext();
        assertThat(it.rowData(), sameInstance(inputs));
        it.close();
        assertThat(it.rowData(), sameInstance(inputs));
    }

    private void testBehaviourAfterClose(BatchIterator it) {
        Columns inputs = it.rowData();
        assertThat(inputs.size(), greaterThan(-1));
        it.close();
        // after close the rowData call is still valid
        assertThat(it.rowData().size(), greaterThan(-1));

        expectFailure(it::moveNext, IllegalStateException.class, "moveNext must fail after close");
        expectFailure(it::moveToStart, IllegalStateException.class, "moveToStart must fail after close");
        expectFailure(it::allLoaded, IllegalStateException.class, "allLoaded must fail after close");
    }

    private void testProperConsumption(BatchIterator it, List<Object[]> expectedResult) throws Exception {
        TestingBatchConsumer consumer = new TestingBatchConsumer();
        consumer.accept(it, null);

        List<Object[]> result = consumer.getResult();
        checkResult(expectedResult, result);
    }

    private static void checkResult(List<Object[]> expected, List<Object[]> actual) {
        if (expected.isEmpty()){
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
