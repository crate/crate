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

import io.crate.analyze.OrderBy;
import io.crate.analyze.WindowDefinition;
import io.crate.breaker.RamAccountingContext;
import io.crate.data.InMemoryBatchIterator;
import io.crate.data.Input;
import io.crate.data.Row;
import io.crate.data.Row1;
import io.crate.execution.engine.collect.CollectExpression;
import io.crate.execution.engine.collect.InputCollectExpression;
import io.crate.expression.symbol.Literal;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.testing.BatchIteratorTester;
import io.crate.testing.BatchSimulatingIterator;
import io.crate.testing.TestingBatchIterators;
import io.crate.testing.TestingRowConsumer;
import io.crate.types.DataTypes;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.crate.data.SentinelRow.SENTINEL;
import static java.util.Collections.singletonList;
import static org.elasticsearch.common.collect.Tuple.tuple;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;

public class WindowBatchIteratorTest {

    private RamAccountingContext ramAccountingContext;

    private List<Object[]> expectedRowNumberResult = IntStream.range(0, 10)
        .mapToObj(l -> new Object[]{l + 1}).collect(Collectors.toList());

    @Before
    public void setUp() {
        ramAccountingContext = new RamAccountingContext("dummy", new NoopCircuitBreaker("dummy"));
    }

    @Test
    public void testWindowBatchIterator() throws Exception {
        BatchIteratorTester tester = new BatchIteratorTester(
            () -> new WindowBatchIterator(
                emptyWindow(),
                Collections.emptyList(),
                Collections.emptyList(),
                TestingBatchIterators.range(0, 10),
                Collections.singletonList(rowNumberWindowFunction()),
                Collections.emptyList(),
                Collections.singletonList(DataTypes.INTEGER),
                ramAccountingContext,
                null,
                new Input[0])
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
                Collections.emptyList(),
                Collections.singletonList(DataTypes.INTEGER),
                ramAccountingContext,
                null,
                new Input[0])
        );

        tester.verifyResultAndEdgeCaseBehaviour(expectedRowNumberResult);
    }

    @Test
    public void testFrameBounds() throws Exception {
        TestingRowConsumer consumer = new TestingRowConsumer();
        consumer.accept(new WindowBatchIterator(
            emptyWindow(),
            Collections.emptyList(),
            Collections.emptyList(),
            TestingBatchIterators.range(0, 10),
            Collections.singletonList(frameBoundsWindowFunction()),
            Collections.emptyList(),
            Collections.singletonList(DataTypes.INTEGER),
            ramAccountingContext,
            null,
            new Input[0]), null);

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
            Collections.emptyList(),
            Collections.singletonList(DataTypes.INTEGER),
            ramAccountingContext,
            null,
            new Input[0]);
        TestingRowConsumer consumer = new TestingRowConsumer();
        consumer.accept(windowBatchIterator, null);

        // should've accounted for 10 integers of 48 bytes each (16 for the integer, 32 for the LinkedList element)
        assertThat(ramAccountingContext.totalBytes(), is(480L));
    }

    @Test
    public void testWindowBatchIteratorWithOrderedWindowOverNullValues() throws Exception {
        OrderBy orderBy = new OrderBy(singletonList(Literal.of(1L)), new boolean[]{true}, new Boolean[]{true});
        WindowDefinition windowDefinition = new WindowDefinition(Collections.emptyList(), orderBy, null);

        WindowBatchIterator windowBatchIterator = new WindowBatchIterator(
            windowDefinition,
            Collections.emptyList(),
            Collections.emptyList(),
            InMemoryBatchIterator.of(List.of(new Row1(null), new Row1(2)), SENTINEL),
            Collections.singletonList(firstCellValue()),
            Collections.emptyList(),
            Collections.singletonList(DataTypes.INTEGER),
            ramAccountingContext,
            new int[]{0},
            new Input[0]);
        TestingRowConsumer consumer = new TestingRowConsumer();
        consumer.accept(windowBatchIterator, null);

        List<Object[]> result = consumer.getResult();
        assertThat(result, contains(new Object[]{null}, new Object[]{null}));
    }

    @Test
    public void testWindowBatchIteratorWithPartitions() throws Exception {
        Supplier<WindowBatchIterator> batchIterator = () -> {
            InputCollectExpression partitionByCollectExpression = new InputCollectExpression(0);
            return new WindowBatchIterator(
                emptyWindow(),
                Collections.emptyList(),
                Collections.emptyList(),
                InMemoryBatchIterator.of(
                    List.of(
                        new Row1(-1), new Row1(1), new Row1(1), new Row1(2), new Row1(2),
                        new Row1(3), new Row1(4), new Row1(5), new Row1(null), new Row1(null)
                    ),
                    SENTINEL
                ),
                singletonList(rowNumberWindowFunction()),
                Collections.emptyList(),
                Collections.singletonList(partitionByCollectExpression),
                Collections.singletonList(partitionByCollectExpression),
                singletonList(DataTypes.INTEGER),
                ramAccountingContext,
                null,
                new Input[0]);
        };

        List<Object[]> expectedResult = List.of(new Object[]{1}, new Object[]{1}, new Object[]{2}, new Object[]{1}, new Object[]{2}, new Object[]{1},
            new Object[]{1}, new Object[]{1}, new Object[]{1}, new Object[]{2});

        BatchIteratorTester tester = new BatchIteratorTester(() -> batchIterator.get());
        tester.verifyResultAndEdgeCaseBehaviour(expectedResult);
    }

    private static WindowDefinition emptyWindow() {
        return new WindowDefinition(Collections.emptyList(), null, null);
    }

    private static WindowFunction firstCellValue() {
        return new WindowFunction() {
            @Override
            public Object execute(int rowIdx, WindowFrameState currentFrame, List<? extends CollectExpression<Row, ?>> expressions, Input... args) {
                return currentFrame.getRows().iterator().next()[0];
            }

            @Override
            public FunctionInfo info() {
                return new FunctionInfo(
                    new FunctionIdent("first_cell_value", Collections.emptyList()),
                    DataTypes.INTEGER);
            }
        };
    }

    private static WindowFunction rowNumberWindowFunction() {
        return new WindowFunction() {
            @Override
            public Object execute(int rowIdx, WindowFrameState currentFrame, List<? extends CollectExpression<Row, ?>> expressions, Input... args) {
                return rowIdx + 1; // sql row numbers are 1-indexed;
            }

            @Override
            public FunctionInfo info() {
                return new FunctionInfo(
                    new FunctionIdent("row_number", Collections.emptyList()),
                    DataTypes.INTEGER);
            }
        };
    }

    private static WindowFunction frameBoundsWindowFunction() {
        return new WindowFunction() {
            @Override
            public Object execute(int rowIdx, WindowFrameState currentFrame, List<? extends CollectExpression<Row, ?>> expressions, Input... args) {
                return tuple(currentFrame.lowerBound(), currentFrame.upperBoundExclusive());
            }

            @Override
            public FunctionInfo info() {
                return new FunctionInfo(
                    new FunctionIdent("a_frame_bounded_window_function", Collections.emptyList()),
                    DataTypes.INTEGER);
            }
        };
    }

}
