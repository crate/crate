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

import com.carrotsearch.randomizedtesting.RandomizedRunner;
import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import io.crate.data.join.CombinedRow;
import io.crate.data.join.JoinBatchIterators;
import io.crate.testing.BatchIteratorTester;
import io.crate.testing.BatchSimulatingIterator;
import io.crate.testing.TestingBatchIterators;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static com.carrotsearch.randomizedtesting.RandomizedTest.$;

@RunWith(RandomizedRunner.class)
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class HashInnerJoinBatchIteratorTest {

    private final List<Object[]> expectedResult;
    private final Supplier<BatchIterator<Row>> leftIterator;
    private final Supplier<BatchIterator<Row>> rightIterator;

    private static Predicate<Row> getCol0EqCol1JoinCondition() {
        return row -> Objects.equals(row.get(0), row.get(1));
    }

    private static Function<Row, Integer> getHashForLeft() {
        return row -> Objects.hash(row.get(0));
    }

    private static Function<Row, Integer> getHashForRight() {
        return row -> Objects.hash(row.get(0));
    }

    private static Function<Row, Integer> getHashWithCollisions() {
        return row -> (Integer) row.get(0) % 3;
    }

    public HashInnerJoinBatchIteratorTest(@SuppressWarnings("unused") @Name("dataSetName") String testName,
                                          @Name("dataForLeft") Supplier<BatchIterator<Row>> leftIterator,
                                          @Name("dataForRight") Supplier<BatchIterator<Row>> rightIterator,
                                          @Name("expectedResult") List<Object[]> expectedResult) {
        this.leftIterator = leftIterator;
        this.rightIterator = rightIterator;
        this.expectedResult = expectedResult;
    }

    @ParametersFactory
    public static Iterable<Object[]> testParameters() {
        List<Object[]> resultForUniqueValues = Arrays.asList(
            new Object[]{2, 2}, new Object[]{3, 3}, new Object[]{4, 4});
        List<Object[]> resultForDuplicateValues = Arrays.asList(
            new Object[]{1, 1}, new Object[]{1, 1},
            new Object[]{2, 2}, new Object[]{2, 2},
            new Object[]{3, 3},
            new Object[]{4, 4}, new Object[]{4, 4}, new Object[]{4, 4}, new Object[]{4, 4}
        );

        // BatchSimulatingIterators are used also for "plain" cases because the
        // TestingBatchIterators.range() always return allLoaded() == true
        return Arrays.asList(
            $("UniqueValues-plain",
              (Supplier<BatchIterator<Row>>) () -> new BatchSimulatingIterator<>(
                    TestingBatchIterators.range(0, 5), 5, 1, null),
              (Supplier<BatchIterator<Row>>) () -> new BatchSimulatingIterator<>(
                    TestingBatchIterators.range(2, 6), 4, 1, null),
              resultForUniqueValues),
            $("UniqueValues-batchedSource",
              (Supplier<BatchIterator<Row>>) () ->
                    new BatchSimulatingIterator<>(TestingBatchIterators.range(0, 5), 2, 2, null),
              (Supplier<BatchIterator<Row>>) () ->
                    new BatchSimulatingIterator<>(TestingBatchIterators.range(2, 6), 2, 2, null),
              resultForUniqueValues),
            $("DuplicateValues-plain",
              (Supplier<BatchIterator<Row>>) () -> new BatchSimulatingIterator<>(
                    TestingBatchIterators.ofValues(Arrays.asList(0, 0, 1, 2, 2, 3, 4, 4)), 8, 1, null),
              (Supplier<BatchIterator<Row>>) () -> new BatchSimulatingIterator<>(
                    TestingBatchIterators.ofValues(Arrays.asList(1, 1, 2, 3, 4, 4, 5, 5, 6)), 9, 1, null),
              resultForDuplicateValues),
            $("DuplicateValues-batchedSource",
              (Supplier<BatchIterator<Row>>) () -> new BatchSimulatingIterator<>(
                    TestingBatchIterators.ofValues(Arrays.asList(0, 0, 1, 2, 2, 3, 4, 4)), 2, 4, null),
              (Supplier<BatchIterator<Row>>) () -> new BatchSimulatingIterator<>(
                    TestingBatchIterators.ofValues(Arrays.asList(1, 1, 2, 3, 4, 4, 5, 5, 6)), 2, 4, null),
              resultForDuplicateValues));
    }

    @Test
    public void testInnerHashJoin() throws Exception {
        Supplier<BatchIterator<Row>> batchIteratorSupplier = () -> JoinBatchIterators.hashInnerJoin(
            leftIterator.get(),
            rightIterator.get(),
            new CombinedRow(1, 1),
            getCol0EqCol1JoinCondition(),
            getHashForLeft(),
            getHashForRight(),
            5
        );
        BatchIteratorTester tester = new BatchIteratorTester(batchIteratorSupplier);
        tester.verifyResultAndEdgeCaseBehaviour(expectedResult);
    }

    @Test
    public void testInnerHashJoinWithHashCollisions() throws Exception {
        Supplier<BatchIterator<Row>> batchIteratorSupplier = () -> JoinBatchIterators.hashInnerJoin(
            leftIterator.get(),
            rightIterator.get(),
            new CombinedRow(1, 1),
            getCol0EqCol1JoinCondition(),
            getHashWithCollisions(),
            getHashWithCollisions(),
            5
        );
        BatchIteratorTester tester = new BatchIteratorTester(batchIteratorSupplier);
        tester.verifyResultAndEdgeCaseBehaviour(expectedResult);
    }

    @Test
    public void testInnerHashJoinWithBlockSizeSmallerThanDataSet() throws Exception {
        Supplier<BatchIterator<Row>> batchIteratorSupplier = () -> JoinBatchIterators.hashInnerJoin(
            leftIterator.get(),
            rightIterator.get(),
            new CombinedRow(1, 1),
            getCol0EqCol1JoinCondition(),
            getHashForLeft(),
            getHashForRight(),
            1
        );
        BatchIteratorTester tester = new BatchIteratorTester(batchIteratorSupplier);
        tester.verifyResultAndEdgeCaseBehaviour(expectedResult);
    }

    @Test
    public void testInnerHashJoinWithBlockSizeBiggerThanIteratorBatchSize() throws Exception {
        Supplier<BatchIterator<Row>> batchIteratorSupplier = () -> JoinBatchIterators.hashInnerJoin(
            leftIterator.get(),
            rightIterator.get(),
            new CombinedRow(1, 1),
            getCol0EqCol1JoinCondition(),
            getHashForLeft(),
            getHashForRight(),
            3
        );
        BatchIteratorTester tester = new BatchIteratorTester(batchIteratorSupplier);
        tester.verifyResultAndEdgeCaseBehaviour(expectedResult);
    }
}
