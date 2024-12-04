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

package io.crate.execution.engine.join;

import static com.carrotsearch.randomizedtesting.RandomizedTest.$;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.ToIntFunction;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.carrotsearch.randomizedtesting.RandomizedRunner;
import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import io.crate.data.BatchIterator;
import io.crate.data.Row;
import io.crate.data.breaker.RowAccounting;
import io.crate.data.join.CombinedRow;
import io.crate.data.testing.BatchIteratorTester;
import io.crate.data.testing.BatchIteratorTester.ResultOrder;
import io.crate.data.testing.BatchSimulatingIterator;
import io.crate.data.testing.TestingBatchIterators;

@RunWith(RandomizedRunner.class)
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class LeftOuterHashJoinBatchIteratorTest {

    private final CircuitBreaker circuitBreaker = mock(CircuitBreaker.class);
    private final List<Object[]> expectedResult;
    private final Supplier<BatchIterator<Row>> leftIterator;
    private final Supplier<BatchIterator<Row>> rightIterator;

    private static Predicate<Row> getCol0EqCol1JoinCondition() {
        return row -> Objects.equals(row.get(0), row.get(1));
    }

    private static ToIntFunction<Row> getHashForLeft() {
        return row -> Objects.hash(row.get(0));
    }

    private static ToIntFunction<Row> getHashForRight() {
        return row -> Objects.hash(row.get(0));
    }

    private static ToIntFunction<Row> getHashWithCollisions() {
        return row -> (Integer) row.get(0) % 3;
    }

    public LeftOuterHashJoinBatchIteratorTest(@SuppressWarnings("unused") @Name("dataSetName") String testName,
                                              @Name("dataForLeft") Supplier<BatchIterator<Row>> leftIterator,
                                              @Name("dataForRight") Supplier<BatchIterator<Row>> rightIterator,
                                              @Name("expectedResulWithoutNulls") List<Object[]> expectedResulWithoutNulls) {
        this.leftIterator = leftIterator;
        this.rightIterator = rightIterator;
        this.expectedResult = expectedResulWithoutNulls;
        when(circuitBreaker.getLimit()).thenReturn(110L);
        when(circuitBreaker.getUsed()).thenReturn(10L);
    }

    @ParametersFactory
    public static Iterable<Object[]> testParameters() {

        return List.of(
            $("UniqueValues-plain",
                (Supplier<BatchIterator<Row>>) () -> TestingBatchIterators.range(0, 5),
                (Supplier<BatchIterator<Row>>) () -> TestingBatchIterators.range(2, 6),
                List.of($(0, null), $(1, null), $(2, 2), $(3, 3), $(4, 4))),
            $("UniqueValues-batchedSource",
                (Supplier<BatchIterator<Row>>) () -> new BatchSimulatingIterator<>(TestingBatchIterators.range(0, 5), 2, 2, null),
                (Supplier<BatchIterator<Row>>) () -> new BatchSimulatingIterator<>(TestingBatchIterators.range(2, 6), 2, 2, null),
                List.of($(0, null), $(1, null), $(2, 2), $(3, 3), $(4, 4))),
            $("DuplicateValues-plain",
                (Supplier<BatchIterator<Row>>) () -> TestingBatchIterators.ofValues(List.of(0, 0, 1, 2, 2, 3, 4, 4)),
                (Supplier<BatchIterator<Row>>) () -> TestingBatchIterators.ofValues(List.of(1, 1, 2, 3, 4, 4, 5, 5, 6)),
                List.of($(0, null), $(0, null), $(1, 1), $(1, 1), $(2, 2), $(2, 2), $(3, 3), $(4, 4), $(4, 4), $(4, 4), $(4, 4))),
            $("DuplicateValues-batchedSource",
                (Supplier<BatchIterator<Row>>) () -> new BatchSimulatingIterator<>(TestingBatchIterators.ofValues(Arrays.asList(0, 0, 1, 2, 2, 3, 4, 4)), 2, 4, null),
                (Supplier<BatchIterator<Row>>) () -> new BatchSimulatingIterator<>(TestingBatchIterators.ofValues(Arrays.asList(1, 1, 2, 3, 4, 4, 5, 5, 6)), 2, 4, null),
                List.of($(0, null), $(0, null), $(1, 1), $(1, 1), $(2, 2), $(2, 2), $(3, 3), $(4, 4), $(4, 4), $(4, 4), $(4, 4))),
            $("DuplicateValues-leftLoadedRightBatched",
                (Supplier<BatchIterator<Row>>) () -> TestingBatchIterators.ofValues(Arrays.asList(0, 0, 1, 2, 2, 3, 4, 4)),
                (Supplier<BatchIterator<Row>>) () -> new BatchSimulatingIterator<>(TestingBatchIterators.ofValues(Arrays.asList(1, 1, 2, 3, 4, 4, 5, 5, 6)), 2, 4, null),
                List.of($(0, null), $(0, null), $(1, 1), $(1, 1), $(2, 2), $(2, 2), $(3, 3), $(4, 4), $(4, 4), $(4, 4), $(4, 4))));
    }

    @Test
    public void test_left_outer_join() throws Exception {
        Supplier<BatchIterator<Row>> batchIteratorSupplier = () -> new HashJoinBatchIterator(
            leftIterator.get(),
            rightIterator.get(),
            mock(RowAccounting.class),
            new CombinedRow(1, 1),
            getCol0EqCol1JoinCondition(),
            getHashForLeft(),
            getHashForRight(),
            ignored -> 5,
            true
        );
        var tester = BatchIteratorTester.forRows(batchIteratorSupplier, ResultOrder.ANY);
        tester.verifyResultAndEdgeCaseBehaviour(expectedResult);
    }

    @Test
    public void test_left_outer_join_with_blocksize_smaller_than_dataset() throws Exception {
        Supplier<BatchIterator<Row>> batchIteratorSupplier = () -> new HashJoinBatchIterator(
            leftIterator.get(),
            rightIterator.get(),
            mock(RowAccounting.class),
            new CombinedRow(1, 1),
            getCol0EqCol1JoinCondition(),
            getHashForLeft(),
            getHashForRight(),
            ignored -> 1,
            true
        );
        var tester = BatchIteratorTester.forRows(batchIteratorSupplier, ResultOrder.ANY);
        tester.verifyResultAndEdgeCaseBehaviour(expectedResult);
    }

    @Test
    public void test_left_outer_join_with_blocksize_bigger_than_tterator_batchsize() throws Exception {
        Supplier<BatchIterator<Row>> batchIteratorSupplier = () -> new HashJoinBatchIterator(
            leftIterator.get(),
            rightIterator.get(),
            mock(RowAccounting.class),
            new CombinedRow(1, 1),
            getCol0EqCol1JoinCondition(),
            getHashForLeft(),
            getHashForRight(),
            ignored -> 3,
            true
        );
        var tester = BatchIteratorTester.forRows(batchIteratorSupplier, ResultOrder.ANY);
        tester.verifyResultAndEdgeCaseBehaviour(expectedResult);
    }

    @Test
    public void test_left_outer_join_with_collisions() throws Exception {
        Supplier<BatchIterator<Row>> batchIteratorSupplier = () -> new HashJoinBatchIterator(
            leftIterator.get(),
            rightIterator.get(),
            mock(RowAccounting.class),
            new CombinedRow(1, 1),
            getCol0EqCol1JoinCondition(),
            getHashWithCollisions(),
            getHashWithCollisions(),
            ignored -> 3,
            true
        );
        var tester = BatchIteratorTester.forRows(batchIteratorSupplier, ResultOrder.ANY);
        tester.verifyResultAndEdgeCaseBehaviour(expectedResult);
    }
}
