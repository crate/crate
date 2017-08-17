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

import io.crate.data.join.CombinedRow;
import io.crate.data.join.NestedLoopBatchIterator;
import io.crate.testing.BatchIteratorTester;
import io.crate.testing.BatchSimulatingIterator;
import io.crate.testing.TestingBatchIterators;
import io.crate.testing.TestingRowConsumer;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static io.crate.data.SentinelRow.SENTINEL;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class NestedLoopBatchIteratorTest {

    private ArrayList<Object[]> threeXThreeRows;
    private ArrayList<Object[]> leftJoinResult;
    private ArrayList<Object[]> rightJoinResult;
    private ArrayList<Object[]> fullJoinResult;

    private Predicate<Row> getCol0EqCol1JoinCondition() {
        return row -> Objects.equals(row.get(0), row.get(1));
    }

    @Before
    public void setUp() throws Exception {
        threeXThreeRows = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            for (int j = 0; j < 3; j++) {
                threeXThreeRows.add(new Object[] { i, j });
            }
        }

        leftJoinResult = new ArrayList<>();
        leftJoinResult.add(new Object[] { 0, null });
        leftJoinResult.add(new Object[] { 1, null });
        leftJoinResult.add(new Object[] { 2, 2, });
        leftJoinResult.add(new Object[] { 3, 3, });

        rightJoinResult = new ArrayList<>();
        rightJoinResult.add(new Object[] { 2, 2, });
        rightJoinResult.add(new Object[] { 3, 3, });
        rightJoinResult.add(new Object[] { null, 4 });
        rightJoinResult.add(new Object[] { null, 5 });


        fullJoinResult = new ArrayList<>();
        fullJoinResult.add(new Object[] { 0, null });
        fullJoinResult.add(new Object[] { 1, null });
        fullJoinResult.add(new Object[] { 2, 2, });
        fullJoinResult.add(new Object[] { 3, 3, });
        fullJoinResult.add(new Object[] { null, 4 });
        fullJoinResult.add(new Object[] { null, 5 });
    }

    @Test
    public void testNestedLoopBatchIterator() throws Exception {
        BatchIteratorTester tester = new BatchIteratorTester(
            () -> NestedLoopBatchIterator.crossJoin(
                TestingBatchIterators.range(0, 3),
                TestingBatchIterators.range(0, 3),
                new CombinedRow(1, 1)
            )
        );
        tester.verifyResultAndEdgeCaseBehaviour(threeXThreeRows);
    }

    @Test
    public void testNestedLoopWithBatchedSource() throws Exception {
        BatchIteratorTester tester = new BatchIteratorTester(
            () -> NestedLoopBatchIterator.crossJoin(
                new BatchSimulatingIterator<>(TestingBatchIterators.range(0, 3), 2, 2, null),
                new BatchSimulatingIterator<>(TestingBatchIterators.range(0, 3), 2, 2, null),
                new CombinedRow(1, 1)
            )
        );
        tester.verifyResultAndEdgeCaseBehaviour(threeXThreeRows);
    }

    @Test
    public void testNestedLoopLeftAndRightEmpty() throws Exception {
        BatchIterator<Row> iterator = NestedLoopBatchIterator.crossJoin(
            InMemoryBatchIterator.empty(SENTINEL),
            InMemoryBatchIterator.empty(SENTINEL),
            new CombinedRow(0, 0)
        );
        TestingRowConsumer consumer = new TestingRowConsumer();
        consumer.accept(iterator, null);
        assertThat(consumer.getResult(), Matchers.empty());
    }

    @Test
    public void testNestedLoopLeftEmpty() throws Exception {
        BatchIterator<Row> iterator = NestedLoopBatchIterator.crossJoin(
            InMemoryBatchIterator.empty(SENTINEL),
            TestingBatchIterators.range(0, 5),
            new CombinedRow(0, 1)
        );
        TestingRowConsumer consumer = new TestingRowConsumer();
        consumer.accept(iterator, null);
        assertThat(consumer.getResult(), Matchers.empty());
    }

    @Test
    public void testNestedLoopRightEmpty() throws Exception {
        BatchIterator<Row> iterator = NestedLoopBatchIterator.crossJoin(
            TestingBatchIterators.range(0, 5),
            InMemoryBatchIterator.empty(SENTINEL),
            new CombinedRow(1, 0)
        );
        TestingRowConsumer consumer = new TestingRowConsumer();
        consumer.accept(iterator, null);
        assertThat(consumer.getResult(), Matchers.empty());
    }


    @Test
    public void testLeftJoin() throws Exception {
        Supplier<BatchIterator<Row>> batchIteratorSupplier = () -> NestedLoopBatchIterator.leftJoin(
            TestingBatchIterators.range(0, 4),
            TestingBatchIterators.range(2, 6),
            new CombinedRow(1, 1),
            getCol0EqCol1JoinCondition()
        );
        BatchIteratorTester tester = new BatchIteratorTester(batchIteratorSupplier);
        tester.verifyResultAndEdgeCaseBehaviour(leftJoinResult);
    }

    @Test
    public void testLeftJoinBatchedSource() throws Exception {
        Supplier<BatchIterator<Row>> batchIteratorSupplier = () -> NestedLoopBatchIterator.leftJoin(
            new BatchSimulatingIterator<>(TestingBatchIterators.range(0, 4), 2, 2, null),
            new BatchSimulatingIterator<>(TestingBatchIterators.range(2, 6), 2, 2, null),
            new CombinedRow(1, 1),
            getCol0EqCol1JoinCondition()
        );
        BatchIteratorTester tester = new BatchIteratorTester(batchIteratorSupplier);
        tester.verifyResultAndEdgeCaseBehaviour(leftJoinResult);
    }

    @Test
    public void testRightJoin() throws Exception {
        Supplier<BatchIterator<Row>> batchIteratorSupplier = () -> NestedLoopBatchIterator.rightJoin(
            TestingBatchIterators.range(0, 4),
            TestingBatchIterators.range(2, 6),
            new CombinedRow(1, 1),
            getCol0EqCol1JoinCondition()
        );
        BatchIteratorTester tester = new BatchIteratorTester(batchIteratorSupplier);
        tester.verifyResultAndEdgeCaseBehaviour(rightJoinResult);
    }

    @Test
    public void testRightJoinBatchedSource() throws Exception {
        Supplier<BatchIterator<Row>> batchIteratorSupplier = () -> NestedLoopBatchIterator.rightJoin(
            new BatchSimulatingIterator<>(TestingBatchIterators.range(0, 4), 2, 2, null),
            new BatchSimulatingIterator<>(TestingBatchIterators.range(2, 6), 2, 2, null),
            new CombinedRow(1, 1),
            getCol0EqCol1JoinCondition()
        );
        BatchIteratorTester tester = new BatchIteratorTester(batchIteratorSupplier);
        tester.verifyResultAndEdgeCaseBehaviour(rightJoinResult);
    }

    @Test
    public void testFullOuterJoin() throws Exception {
        Supplier<BatchIterator<Row>> batchIteratorSupplier = () -> NestedLoopBatchIterator.fullOuterJoin(
            TestingBatchIterators.range(0, 4),
            TestingBatchIterators.range(2, 6),
            new CombinedRow(1, 1),
            getCol0EqCol1JoinCondition()
        );
        BatchIteratorTester tester = new BatchIteratorTester(batchIteratorSupplier);
        tester.verifyResultAndEdgeCaseBehaviour(fullJoinResult);
    }

    @Test
    public void testFullOuterJoinBatchedSource() throws Exception {
        Supplier<BatchIterator<Row>> batchIteratorSupplier = () -> NestedLoopBatchIterator.fullOuterJoin(
            new BatchSimulatingIterator<>(TestingBatchIterators.range(0, 4), 2, 2, null),
            new BatchSimulatingIterator<>(TestingBatchIterators.range(2, 6), 2, 2, null),
            new CombinedRow(1, 1),
            getCol0EqCol1JoinCondition()
        );
        BatchIteratorTester tester = new BatchIteratorTester(batchIteratorSupplier);
        tester.verifyResultAndEdgeCaseBehaviour(fullJoinResult);
    }

    @Test
    public void testMoveToStartWhileRightSideIsActive() throws Exception {
        BatchIterator<Row> batchIterator = NestedLoopBatchIterator.crossJoin(
            TestingBatchIterators.range(0, 3),
            TestingBatchIterators.range(10, 20),
            new CombinedRow(1, 1)
        );

        assertThat(batchIterator.moveNext(), is(true));
        assertThat(batchIterator.currentElement().get(0), is(0));
        assertThat(batchIterator.currentElement().get(1), is(10));

        batchIterator.moveToStart();

        assertThat(batchIterator.moveNext(), is(true));
        assertThat(batchIterator.currentElement().get(0), is(0));
        assertThat(batchIterator.currentElement().get(1), is(10));
    }
}
