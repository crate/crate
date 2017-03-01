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

import io.crate.testing.BatchIteratorTester;
import io.crate.testing.BatchSimulatingIterator;
import io.crate.testing.CollectingBatchConsumer;
import io.crate.testing.TestingBatchIterators;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Objects;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.junit.Assert.assertThat;

public class NestedLoopBatchIteratorTest {

    private ArrayList<Object[]> threeXThreeRows;
    private ArrayList<Object[]> leftJoinResult;

    private Function<Columns, BooleanSupplier> getCol0EqCol1JoinCondition() {
        return columns -> new BooleanSupplier() {

            Input<?> col1 = columns.get(0);
            Input<?> col2 = columns.get(1);

            @Override
            public boolean getAsBoolean() {
                return Objects.equals(col1.value(), col2.value());
            }
        };
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
        for (int i = 0; i < 4; i++) {
            boolean matched = false;
            for (int j = 2; j < 6 ; j++) {
                if (Integer.compare(i, j) == 0) {
                    matched = true;
                    leftJoinResult.add(new Object[] { i, j });
                }
            }
            if (!matched) {
                leftJoinResult.add(new Object[] { i, null });
            }
        }
    }

    @Test
    public void testNestedLoopBatchIterator() throws Exception {
        BatchIteratorTester tester = new BatchIteratorTester(
            () -> NestedLoopBatchIterator.crossJoin(
                TestingBatchIterators.range(0, 3),
                TestingBatchIterators.range(0, 3)
            ),
            threeXThreeRows
        );
        tester.run();
    }

    @Test
    public void testNestedLoopWithBatchedSource() throws Exception {
        BatchIteratorTester tester = new BatchIteratorTester(
            () -> NestedLoopBatchIterator.crossJoin(
                new BatchSimulatingIterator(TestingBatchIterators.range(0, 3), 2, 2, null),
                new BatchSimulatingIterator(TestingBatchIterators.range(0, 3), 2, 2, null)
            ),
            threeXThreeRows
        );
        tester.run();
    }

    @Test
    public void testNestedLoopLeftAndRightEmpty() throws Exception {
        BatchIterator iterator = NestedLoopBatchIterator.crossJoin(
            RowsBatchIterator.empty(),
            RowsBatchIterator.empty()
        );
        CollectingBatchConsumer consumer = new CollectingBatchConsumer();
        consumer.accept(iterator, null);
        assertThat(consumer.getResult(), Matchers.empty());
    }

    @Test
    public void testNestedLoopLeftEmpty() throws Exception {
        BatchIterator iterator = NestedLoopBatchIterator.crossJoin(
            RowsBatchIterator.empty(),
            TestingBatchIterators.range(0, 5)
        );
        CollectingBatchConsumer consumer = new CollectingBatchConsumer();
        consumer.accept(iterator, null);
        assertThat(consumer.getResult(), Matchers.empty());
    }

    @Test
    public void testNestedLoopRightEmpty() throws Exception {
        BatchIterator iterator = NestedLoopBatchIterator.crossJoin(
            TestingBatchIterators.range(0, 5),
            RowsBatchIterator.empty()
        );
        CollectingBatchConsumer consumer = new CollectingBatchConsumer();
        consumer.accept(iterator, null);
        assertThat(consumer.getResult(), Matchers.empty());
    }


    @Test
    public void testLeftJoin() throws Exception {
        Supplier<BatchIterator> batchIteratorSupplier = () -> NestedLoopBatchIterator.leftJoin(
            TestingBatchIterators.range(0, 4),
            TestingBatchIterators.range(2, 6),
            getCol0EqCol1JoinCondition()
        );
        BatchIteratorTester tester = new BatchIteratorTester(batchIteratorSupplier, leftJoinResult);
        tester.run();
    }

    @Test
    public void testLeftJoinBatchedSource() throws Exception {
        Supplier<BatchIterator> batchIteratorSupplier = () -> NestedLoopBatchIterator.leftJoin(
            new BatchSimulatingIterator(TestingBatchIterators.range(0, 4), 2, 2, null),
            new BatchSimulatingIterator(TestingBatchIterators.range(2, 6), 2, 2, null),
            getCol0EqCol1JoinCondition()
        );
        BatchIteratorTester tester = new BatchIteratorTester(batchIteratorSupplier, leftJoinResult);
        tester.run();
    }

}
