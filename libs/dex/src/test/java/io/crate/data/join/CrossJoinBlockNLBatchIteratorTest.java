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

package io.crate.data.join;

import static io.crate.data.SentinelRow.SENTINEL;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.IntSupplier;
import java.util.function.Supplier;

import org.junit.Test;
import org.junit.runner.RunWith;

import com.carrotsearch.randomizedtesting.RandomizedRunner;
import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import io.crate.data.BatchIterator;
import io.crate.data.InMemoryBatchIterator;
import io.crate.data.Row;
import io.crate.data.breaker.RowAccounting;
import io.crate.data.testing.BatchIteratorTester;
import io.crate.data.testing.BatchSimulatingIterator;
import io.crate.data.testing.TestingBatchIterators;
import io.crate.data.testing.TestingRowConsumer;

@RunWith(RandomizedRunner.class)
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class CrossJoinBlockNLBatchIteratorTest {

    private final int id;
    private final Supplier<BatchIterator<Row>> left;
    private final Supplier<BatchIterator<Row>> right;
    private final IntSupplier blockSizeCalculator;
    private final TestingRowAccounting testingRowAccounting;
    private final List<Object[]> expectedResults;
    private int expectedRowsLeft;
    private int expectedRowsRight;

    public CrossJoinBlockNLBatchIteratorTest(@Name("id") int id,
                                             Supplier<BatchIterator<Row>> left,
                                             Supplier<BatchIterator<Row>> right,
                                             IntSupplier blockSizeCalculator) throws Exception {

        this.id = id;
        this.left = left;
        this.right = right;
        this.blockSizeCalculator = blockSizeCalculator;
        this.testingRowAccounting = new TestingRowAccounting();
        this.expectedResults = createExpectedResult(left.get(), right.get());
    }

    private List<Object[]> createExpectedResult(BatchIterator<Row> left, BatchIterator<Row> right) throws Exception {
        TestingRowConsumer leftConsumer = new TestingRowConsumer();
        leftConsumer.accept(left, null);
        TestingRowConsumer rightConsumer = new TestingRowConsumer();
        rightConsumer.accept(right, null);

        List<Object[]> expectedResults = new ArrayList<>();

        List<Object[]> leftResults = leftConsumer.getResult();
        List<Object[]> rightResults = rightConsumer.getResult();
        expectedRowsLeft = leftResults.size();
        expectedRowsRight = rightResults.size();

        for (Object[] leftRow : leftResults) {
            for (Object[] rightRow : rightResults) {
                Object[] combinedRow = Arrays.copyOf(leftRow, leftRow.length + rightRow.length);
                System.arraycopy(rightRow, 0, combinedRow, leftRow.length, rightRow.length);
                expectedResults.add(combinedRow);
            }
        }
        return expectedResults;
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        return Arrays.asList(
            $(0, () -> TestingBatchIterators.range(0, 3), () -> TestingBatchIterators.range(0, 3), () -> 1),
            $(1, () -> TestingBatchIterators.range(0, 1), () -> TestingBatchIterators.range(0, 5), () -> 2),
            $(2, () -> TestingBatchIterators.range(0, 3), () -> TestingBatchIterators.range(0, 3), () -> 3),
            $(3, () -> TestingBatchIterators.range(0, 3), () -> TestingBatchIterators.range(0, 3), () -> 10),
            $(4, () -> TestingBatchIterators.range(0, 1), () -> TestingBatchIterators.range(0, 5), () -> 100),
            $(5, () -> TestingBatchIterators.range(0, 3), () -> TestingBatchIterators.range(0, 3), () -> 10000),
            $(6, () -> TestingBatchIterators.range(0, 100), () -> TestingBatchIterators.range(0, 30), () -> 1),
            $(7, () -> TestingBatchIterators.range(0, 10), () -> TestingBatchIterators.range(0, 250), () -> 2),
            $(8, () -> TestingBatchIterators.range(0, 30), () -> TestingBatchIterators.range(0, 100), () -> 3),
            $(9, () -> TestingBatchIterators.range(0, 250), () -> TestingBatchIterators.range(0, 30), () -> 10),
            $(10, () -> TestingBatchIterators.range(0, 100), () -> TestingBatchIterators.range(0, 50), () -> 100),
            $(11, () -> TestingBatchIterators.range(0, 30), () -> TestingBatchIterators.range(0, 200), () -> 10000)
        );
    }

    private static Object[] $(int id,
                              Supplier<BatchIterator<Row>> left,
                              Supplier<BatchIterator<Row>> right,
                              IntSupplier blockSize) {
        return new Object[]{
            id, left, right, blockSize
        };
    }

    @Test
    public void testNestedLoopBatchIterator() throws Exception {
        var tester = BatchIteratorTester.forRows(
            () -> new CrossJoinBlockNLBatchIterator(
                left.get(),
                right.get(),
                new CombinedRow(1, 1),
                blockSizeCalculator,
                testingRowAccounting
            )
        );
        tester.verifyResultAndEdgeCaseBehaviour(expectedResults,
            it -> {
                assertThat(testingRowAccounting.numRows).isEqualTo(expectedRowsLeft);
                assertThat(testingRowAccounting.numReleaseCalled).isGreaterThan(
                    expectedRowsLeft / blockSizeCalculator.getAsInt());
            });
    }

    @Test
    public void testNestedLoopWithBatchedSource() throws Exception {
        int batchSize = 50;
        var tester = BatchIteratorTester.forRows(
            () -> new CrossJoinBlockNLBatchIterator(
                new BatchSimulatingIterator<>(left.get(), batchSize, expectedRowsLeft / batchSize + 1, null),
                new BatchSimulatingIterator<>(right.get(), batchSize, expectedRowsRight / batchSize + 1, null),
                new CombinedRow(1, 1),
                blockSizeCalculator,
                testingRowAccounting
            )
        );
        tester.verifyResultAndEdgeCaseBehaviour(expectedResults,
            it -> {
                assertThat(testingRowAccounting.numRows).isEqualTo(expectedRowsLeft);
                assertThat(testingRowAccounting.numReleaseCalled).isGreaterThan(
                    expectedRowsLeft / blockSizeCalculator.getAsInt());
            });
    }

    @Test
    public void testNestedLoopLeftAndRightEmpty() throws Exception {
        BatchIterator<Row> iterator = new CrossJoinBlockNLBatchIterator(
            InMemoryBatchIterator.empty(SENTINEL),
            InMemoryBatchIterator.empty(SENTINEL),
            new CombinedRow(0, 0),
            blockSizeCalculator,
            testingRowAccounting
        );
        TestingRowConsumer consumer = new TestingRowConsumer();
        consumer.accept(iterator, null);
        assertThat(consumer.getResult()).isEmpty();
    }

    @Test
    public void testNestedLoopLeftEmpty() throws Exception {
        BatchIterator<Row> iterator = new CrossJoinBlockNLBatchIterator(
            InMemoryBatchIterator.empty(SENTINEL),
            right.get(),
            new CombinedRow(0, 1),
            blockSizeCalculator,
            testingRowAccounting
        );
        TestingRowConsumer consumer = new TestingRowConsumer();
        consumer.accept(iterator, null);
        assertThat(consumer.getResult()).isEmpty();
    }

    @Test
    public void testNestedLoopRightEmpty() throws Exception {
        BatchIterator<Row> iterator = new CrossJoinBlockNLBatchIterator(
            left.get(),
            InMemoryBatchIterator.empty(SENTINEL),
            new CombinedRow(1, 0),
            blockSizeCalculator,
            testingRowAccounting
        );
        TestingRowConsumer consumer = new TestingRowConsumer();
        consumer.accept(iterator, null);
        assertThat(consumer.getResult()).isEmpty();
    }

    @Test
    public void testMoveToStartWhileRightSideIsActive() {
        BatchIterator<Row> batchIterator = new CrossJoinBlockNLBatchIterator(
            left.get(),
            right.get(),
            new CombinedRow(1, 1),
            blockSizeCalculator,
            testingRowAccounting
        );

        assertThat(batchIterator.moveNext()).isTrue();
        assertThat(expectedResults).contains(batchIterator.currentElement().materialize());

        batchIterator.moveToStart();

        assertThat(batchIterator.moveNext()).isTrue();
        assertThat(expectedResults).contains(batchIterator.currentElement().materialize());
    }

    private static class TestingRowAccounting implements RowAccounting<Object[]> {

        int numRows;
        int numReleaseCalled;

        @Override
        public void accountForAndMaybeBreak(Object[] row) {
            numRows++;
        }

        @Override
        public void release() {
            numReleaseCalled++;
        }
    }
}
