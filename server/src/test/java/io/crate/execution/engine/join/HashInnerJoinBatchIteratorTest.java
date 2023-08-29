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
import io.crate.data.testing.BatchSimulatingIterator;
import io.crate.data.testing.TestingBatchIterators;

@RunWith(RandomizedRunner.class)
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class HashInnerJoinBatchIteratorTest {

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

    public HashInnerJoinBatchIteratorTest(@SuppressWarnings("unused") @Name("dataSetName") String testName,
                                          @Name("dataForLeft") Supplier<BatchIterator<Row>> leftIterator,
                                          @Name("dataForRight") Supplier<BatchIterator<Row>> rightIterator,
                                          @Name("expectedResult") List<Object[]> expectedResult) {
        this.leftIterator = leftIterator;
        this.rightIterator = rightIterator;
        this.expectedResult = expectedResult;
        when(circuitBreaker.getLimit()).thenReturn(110L);
        when(circuitBreaker.getUsed()).thenReturn(10L);
    }

    @ParametersFactory
    public static Iterable<Object[]> testParameters() {
        List<Object[]> resultForUniqueValues = Arrays.asList(
            new Object[] { 2, 2 }, new Object[] { 3, 3 }, new Object[] { 4, 4 });
        List<Object[]> resultForDuplicateValues = Arrays.asList(
            new Object[] { 1, 1 }, new Object[] { 1, 1 },
            new Object[] { 2, 2 }, new Object[] { 2, 2 },
            new Object[] { 3, 3 },
            new Object[] { 4, 4 }, new Object[] { 4, 4 }, new Object[] { 4, 4 }, new Object[] { 4, 4 }
        );

        return Arrays.asList(
            $("UniqueValues-plain",
              (Supplier<BatchIterator<Row>>) () -> TestingBatchIterators.range(0, 5),
              (Supplier<BatchIterator<Row>>) () -> TestingBatchIterators.range(2, 6),
              resultForUniqueValues),
            $("UniqueValues-batchedSource",
              (Supplier<BatchIterator<Row>>) () ->
                  new BatchSimulatingIterator<>(TestingBatchIterators.range(0, 5), 2, 2, null),
              (Supplier<BatchIterator<Row>>) () ->
                  new BatchSimulatingIterator<>(TestingBatchIterators.range(2, 6), 2, 2, null),
              resultForUniqueValues),
            $("DuplicateValues-plain",
              (Supplier<BatchIterator<Row>>) () -> TestingBatchIterators.ofValues(Arrays.asList(0, 0, 1, 2, 2, 3, 4, 4)),
              (Supplier<BatchIterator<Row>>) () ->
                  TestingBatchIterators.ofValues(Arrays.asList(1, 1, 2, 3, 4, 4, 5, 5, 6)),
              resultForDuplicateValues),
            $("DuplicateValues-batchedSource",
              (Supplier<BatchIterator<Row>>) () ->
                  new BatchSimulatingIterator<>(
                      TestingBatchIterators.ofValues(Arrays.asList(0, 0, 1, 2, 2, 3, 4, 4)), 2, 4, null),
              (Supplier<BatchIterator<Row>>) () -> new BatchSimulatingIterator<>(
                TestingBatchIterators.ofValues(Arrays.asList(1, 1, 2, 3, 4, 4, 5, 5, 6)), 2, 4, null),
              resultForDuplicateValues),
            $("DuplicateValues-leftLoadedRightBatched",
              (Supplier<BatchIterator<Row>>) () -> TestingBatchIterators.ofValues(Arrays.asList(0, 0, 1, 2, 2, 3, 4, 4)),
              (Supplier<BatchIterator<Row>>) () -> new BatchSimulatingIterator<>(
              TestingBatchIterators.ofValues(Arrays.asList(1, 1, 2, 3, 4, 4, 5, 5, 6)), 2, 4, null),
              resultForDuplicateValues));
    }

    @Test
    public void testInnerHashJoin() throws Exception {
        Supplier<BatchIterator<Row>> batchIteratorSupplier = () -> new HashInnerJoinBatchIterator(
            leftIterator.get(),
            rightIterator.get(),
            mock(RowAccounting.class),
            new CombinedRow(1, 1),
            getCol0EqCol1JoinCondition(),
            getHashForLeft(),
            getHashForRight(),
            () -> 5
        );
        var tester = BatchIteratorTester.forRows(batchIteratorSupplier);
        tester.verifyResultAndEdgeCaseBehaviour(expectedResult);
    }

    @Test
    public void testInnerHashJoinWithHashCollisions() throws Exception {
        Supplier<BatchIterator<Row>> batchIteratorSupplier = () -> new HashInnerJoinBatchIterator(
            leftIterator.get(),
            rightIterator.get(),
            mock(RowAccounting.class),
            new CombinedRow(1, 1),
            getCol0EqCol1JoinCondition(),
            getHashWithCollisions(),
            getHashWithCollisions(),
            () -> 5
        );
        var tester = BatchIteratorTester.forRows(batchIteratorSupplier);
        tester.verifyResultAndEdgeCaseBehaviour(expectedResult);
    }

    @Test
    public void testInnerHashJoinWithBlockSizeSmallerThanDataSet() throws Exception {
        Supplier<BatchIterator<Row>> batchIteratorSupplier = () -> new HashInnerJoinBatchIterator(
            leftIterator.get(),
            rightIterator.get(),
            mock(RowAccounting.class),
            new CombinedRow(1, 1),
            getCol0EqCol1JoinCondition(),
            getHashForLeft(),
            getHashForRight(),
            () -> 1
        );
        var tester = BatchIteratorTester.forRows(batchIteratorSupplier);
        tester.verifyResultAndEdgeCaseBehaviour(expectedResult);
    }

    @Test
    public void testInnerHashJoinWithBlockSizeBiggerThanIteratorBatchSize() throws Exception {
        Supplier<BatchIterator<Row>> batchIteratorSupplier = () -> new HashInnerJoinBatchIterator(
            leftIterator.get(),
            rightIterator.get(),
            mock(RowAccounting.class),
            new CombinedRow(1, 1),
            getCol0EqCol1JoinCondition(),
            getHashForLeft(),
            getHashForRight(),
            () -> 3
        );
        var tester = BatchIteratorTester.forRows(batchIteratorSupplier);
        tester.verifyResultAndEdgeCaseBehaviour(expectedResult);
    }
}
