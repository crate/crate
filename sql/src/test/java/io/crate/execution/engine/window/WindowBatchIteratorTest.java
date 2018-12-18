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

package io.crate.execution.engine.window;

import io.crate.analyze.WindowDefinition;
import io.crate.breaker.RamAccountingContext;
import io.crate.testing.BatchIteratorTester;
import io.crate.testing.BatchSimulatingIterator;
import io.crate.testing.TestingBatchIterators;
import io.crate.testing.TestingRowConsumer;
import io.crate.types.DataTypes;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.common.collect.Tuple.tuple;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;

public class WindowBatchIteratorTest {

    private final RamAccountingContext RAM_ACCOUNTING_CONTEXT =
        new RamAccountingContext("dummy", new NoopCircuitBreaker("dummy"));

    private List<Object[]> expectedRowNumberResult = IntStream.range(0, 10)
        .mapToObj(l -> new Object[]{l + 1}).collect(Collectors.toList());

    @Test
    public void testWindowBatchIterator() throws Exception {
        BatchIteratorTester tester = new BatchIteratorTester(
            () -> new WindowBatchIterator(
                emptyWindow(),
                Collections.emptyList(),
                Collections.emptyList(),
                TestingBatchIterators.range(0, 10),
                Collections.singletonList(rowNumberWindowFunction()),
                Collections.singletonList(DataTypes.INTEGER),
                RAM_ACCOUNTING_CONTEXT,
                null)
        );

        tester.verifyResultAndEdgeCaseBehaviour(expectedRowNumberResult);
    }

    @Test
    public void testWindowBatchIteratorWithBatchSimulatingSource() throws Exception {
        BatchIteratorTester tester = new BatchIteratorTester(
            () -> new WindowBatchIterator(
                emptyWindow(),
                Collections.emptyList(),
                Collections.emptyList(),
                new BatchSimulatingIterator<>(
                    TestingBatchIterators.range(0, 10), 4, 2, null),
                Collections.singletonList(rowNumberWindowFunction()),
                Collections.singletonList(DataTypes.INTEGER),
                RAM_ACCOUNTING_CONTEXT,
                null)
        );

        tester.verifyResultAndEdgeCaseBehaviour(expectedRowNumberResult);
    }

    private WindowFunction rowNumberWindowFunction() {
        return (rowIdx, frame) -> rowIdx + 1; // sql row numbers are 1-indexed
    }

    @Test
    public void testFrameBounds() throws Exception {
        WindowFunction frameBoundsWindowFunction =
            (rowIdx, currentFrame) -> tuple(currentFrame.lowerBound(), currentFrame.upperBoundExclusive());

        TestingRowConsumer consumer = new TestingRowConsumer();
        consumer.accept(new WindowBatchIterator(
            emptyWindow(),
            Collections.emptyList(),
            Collections.emptyList(),
            TestingBatchIterators.range(0, 10),
            Collections.singletonList(frameBoundsWindowFunction),
            Collections.singletonList(DataTypes.INTEGER),
            RAM_ACCOUNTING_CONTEXT,
            null), null);

        Object[] expectedBounds = {tuple(0, 10)};
        List<Object[]> result = consumer.getResult();

        IntStream.range(0, 10).forEach(i -> assertThat(result.get(i), is(expectedBounds)));
    }

    @Test
    public void testWindowBatchIteratorAccountsUsedMemory() {
        WindowBatchIterator windowBatchIterator = new WindowBatchIterator(
            emptyWindow(),
            Collections.emptyList(),
            Collections.emptyList(),
            TestingBatchIterators.range(0, 10),
            Collections.singletonList(rowNumberWindowFunction()),
            Collections.singletonList(DataTypes.INTEGER),
            RAM_ACCOUNTING_CONTEXT,
            null);
        RAM_ACCOUNTING_CONTEXT.release();
        TestingRowConsumer consumer = new TestingRowConsumer();
        consumer.accept(windowBatchIterator, null);

        // should've accounted for 10 integers (with padding) and some overhead
        assertThat(RAM_ACCOUNTING_CONTEXT.totalBytes(), is(greaterThan(160L)));
    }

    private static WindowDefinition emptyWindow() {
        return new WindowDefinition(Collections.emptyList(), null, null);
    }

}
