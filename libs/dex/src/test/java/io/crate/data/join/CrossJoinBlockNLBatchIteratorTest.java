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
import java.util.function.LongToIntFunction;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import io.crate.data.BatchIterator;
import io.crate.data.InMemoryBatchIterator;
import io.crate.data.Row;
import io.crate.data.breaker.RowAccounting;
import io.crate.data.testing.BatchIteratorTester;
import io.crate.data.testing.BatchSimulatingIterator;
import io.crate.data.testing.TestingBatchIterators;
import io.crate.data.testing.TestingRowConsumer;

public class CrossJoinBlockNLBatchIteratorTest {

    private static class Params {
        private final int id;
        private final Supplier<BatchIterator<Row>> left;
        private final Supplier<BatchIterator<Row>> right;
        private final LongToIntFunction blockSizeCalculator;
        private final TestingRowAccounting testingRowAccounting;
        private final List<Object[]> expectedResults;
        private final int expectedRowsLeft;
        private final int expectedRowsRight;

        @SuppressWarnings("unchecked")
        private Params(int id,
                       Supplier<BatchIterator<Row>> left,
                       Supplier<BatchIterator<Row>> right,
                       LongToIntFunction blockSizeCalculator) throws Exception {
            this.id = id;
            this.left = left;
            this.right = right;
            this.blockSizeCalculator = blockSizeCalculator;
            this.testingRowAccounting = new TestingRowAccounting();
            Object[] expected = createExpectedResult(left.get(), right.get());
            this.expectedRowsLeft = (int) expected[0];
            this.expectedRowsRight = (int) expected[1];
            this.expectedResults = (List<Object[]>) expected[2];
        }

        @Override
        public String toString() {
            return id + "";
        }
    }

    private static Object[] createExpectedResult(BatchIterator<Row> left,
                                                 BatchIterator<Row> right) throws Exception {
        TestingRowConsumer leftConsumer = new TestingRowConsumer();
        leftConsumer.accept(left, null);
        TestingRowConsumer rightConsumer = new TestingRowConsumer();
        rightConsumer.accept(right, null);

        List<Object[]> expectedResults = new ArrayList<>();

        List<Object[]> leftResults = leftConsumer.getResult();
        List<Object[]> rightResults = rightConsumer.getResult();
        int expectedRowsLeft = leftResults.size();
        int expectedRowsRight = rightResults.size();

        for (Object[] leftRow : leftResults) {
            for (Object[] rightRow : rightResults) {
                Object[] combinedRow = Arrays.copyOf(leftRow, leftRow.length + rightRow.length);
                System.arraycopy(rightRow, 0, combinedRow, leftRow.length, rightRow.length);
                expectedResults.add(combinedRow);
            }
        }
        return new Object[] {expectedRowsLeft, expectedRowsRight, expectedResults};
    }

    static Stream<Params> parameters() throws Exception {
        return Stream.of(
            new Params(0, () -> TestingBatchIterators.range(0, 3), () -> TestingBatchIterators.range(0, 3), ignored -> 1),
            new Params(1, () -> TestingBatchIterators.range(0, 1), () -> TestingBatchIterators.range(0, 5), ignored -> 2),
            new Params(2, () -> TestingBatchIterators.range(0, 3), () -> TestingBatchIterators.range(0, 3), ignored -> 3),
            new Params(3, () -> TestingBatchIterators.range(0, 3), () -> TestingBatchIterators.range(0, 3), ignored -> 10),
            new Params(4, () -> TestingBatchIterators.range(0, 1), () -> TestingBatchIterators.range(0, 5), ignored -> 100),
            new Params(5,
                () -> TestingBatchIterators.range(0, 3),
                () -> TestingBatchIterators.range(0, 3),
                ignored -> 10000),
            new Params(6, () -> TestingBatchIterators.range(0, 100), () -> TestingBatchIterators.range(0, 30), ignored -> 1),
            new Params(7, () -> TestingBatchIterators.range(0, 10), () -> TestingBatchIterators.range(0, 250), ignored -> 2),
            new Params(8, () -> TestingBatchIterators.range(0, 30), () -> TestingBatchIterators.range(0, 100), ignored -> 3),
            new Params(9,
                () -> TestingBatchIterators.range(0, 250),
                () -> TestingBatchIterators.range(0, 30),
                ignored -> 10),
            new Params(10,
                () -> TestingBatchIterators.range(0, 100),
                () -> TestingBatchIterators.range(0, 50),
                ignored -> 100),
            new Params(11,
                () -> TestingBatchIterators.range(0, 30),
                () -> TestingBatchIterators.range(0, 200),
                ignored -> 10000));
    }

    @ParameterizedTest(name = "{arguments}")
    @MethodSource("parameters")
    public void testNestedLoopBatchIterator(Params params) throws Exception {
        var tester = BatchIteratorTester.forRows(
            () -> new CrossJoinBlockNLBatchIterator(
                params.left.get(),
                params.right.get(),
                new CombinedRow(1, 1),
                params.blockSizeCalculator,
                params.testingRowAccounting
            )
        );
        tester.verifyResultAndEdgeCaseBehaviour(params.expectedResults,
            it -> {
                assertThat(params.testingRowAccounting.numRows)
                    .isEqualTo(params.expectedRowsLeft);
                assertThat(params.testingRowAccounting.numReleaseCalled)
                    .isGreaterThan(
                        params.expectedRowsLeft /
                            params.blockSizeCalculator.applyAsInt(-1));
            });
    }

    @ParameterizedTest
    @MethodSource("parameters")
    public void testNestedLoopWithBatchedSource(Params params) throws Exception {
        int batchSize = 50;
        var tester = BatchIteratorTester.forRows(
            () -> new CrossJoinBlockNLBatchIterator(
                new BatchSimulatingIterator<>(params.left.get(),
                    batchSize,
                    params.expectedRowsLeft / batchSize + 1,
                    null),
                new BatchSimulatingIterator<>(params.right.get(),
                    batchSize,
                    params.expectedRowsRight / batchSize + 1,
                    null),
                new CombinedRow(1, 1),
                params.blockSizeCalculator,
                params.testingRowAccounting
            )
        );
        tester.verifyResultAndEdgeCaseBehaviour(params.expectedResults,
            it -> {
                assertThat(params.testingRowAccounting.numRows)
                    .isEqualTo(params.expectedRowsLeft);
                assertThat(params.testingRowAccounting.numReleaseCalled)
                    .isGreaterThan(params.expectedRowsLeft /
                        params.blockSizeCalculator.applyAsInt(-1));
            });
    }

    @ParameterizedTest
    @MethodSource("parameters")
    public void testNestedLoopLeftAndRightEmpty(Params params) throws Exception {
        BatchIterator<Row> iterator = new CrossJoinBlockNLBatchIterator(
            InMemoryBatchIterator.empty(SENTINEL),
            InMemoryBatchIterator.empty(SENTINEL),
            new CombinedRow(0, 0),
            params.blockSizeCalculator,
            params.testingRowAccounting
        );
        TestingRowConsumer consumer = new TestingRowConsumer();
        consumer.accept(iterator, null);
        assertThat(consumer.getResult()).isEmpty();
    }

    @ParameterizedTest(name = "{argumentsWithNames}")
    @MethodSource("parameters")
    public void testNestedLoopLeftEmpty(Params params) throws Exception {
        BatchIterator<Row> iterator = new CrossJoinBlockNLBatchIterator(
            InMemoryBatchIterator.empty(SENTINEL),
            params.right.get(),
            new CombinedRow(0, 1),
            params.blockSizeCalculator,
            params.testingRowAccounting
        );
        TestingRowConsumer consumer = new TestingRowConsumer();
        consumer.accept(iterator, null);
        assertThat(consumer.getResult()).isEmpty();
    }

    @ParameterizedTest
    @MethodSource("parameters")
    public void testNestedLoopRightEmpty(Params params) throws Exception {
        BatchIterator<Row> iterator = new CrossJoinBlockNLBatchIterator(
            params.left.get(),
            InMemoryBatchIterator.empty(SENTINEL),
            new CombinedRow(1, 0),
            params.blockSizeCalculator,
            params.testingRowAccounting
        );
        TestingRowConsumer consumer = new TestingRowConsumer();
        consumer.accept(iterator, null);
        assertThat(consumer.getResult()).isEmpty();
    }

    @ParameterizedTest
    @MethodSource("parameters")
    public void testMoveToStartWhileRightSideIsActive(Params params) {
        BatchIterator<Row> batchIterator = new CrossJoinBlockNLBatchIterator(
            params.left.get(),
            params.right.get(),
            new CombinedRow(1, 1),
            params.blockSizeCalculator,
            params.testingRowAccounting
        );

        assertThat(batchIterator.moveNext()).isTrue();
        assertThat(params.expectedResults).contains(batchIterator.currentElement().materialize());

        batchIterator.moveToStart();

        assertThat(batchIterator.moveNext()).isTrue();
        assertThat(params.expectedResults).contains(batchIterator.currentElement().materialize());
    }

    private static class TestingRowAccounting implements RowAccounting<Object[]> {

        int numRows;
        int numReleaseCalled;

        @Override
        public long accountForAndMaybeBreak(Object[] row) {
            numRows++;
            return 42;
        }

        @Override
        public void release() {
            numReleaseCalled++;
        }
    }
}
