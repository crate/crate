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

import io.crate.breaker.RamAccountingContext;
import io.crate.data.Input;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.junit.Before;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class WindowBatchIteratorTest {

    private RamAccountingContext ramAccountingContext;
    private Input[][] args = {new Input[0]};

    private List<Object[]> expectedRowNumberResult = IntStream.range(0, 10)
        .mapToObj(l -> new Object[]{l, l + 1}).collect(Collectors.toList());

    @Before
    public void setUp() {
        ramAccountingContext = new RamAccountingContext("dummy", new NoopCircuitBreaker("dummy"));
    }

    /*
    @Test
    public void testWindowBatchIterator() throws Exception {
        BatchIteratorTester tester = new BatchIteratorTester(
            () -> WindowFunctionBatchIterator.of(
                TestingBatchIterators.range(0, 10),
                new IgnoreRowAccounting(),
                new WindowDefinition(List.of(), null, RANGE_UNBOUNDED_PRECEDING_CURRENT_ROW),
                null,
                null,
                null,
                OrderingByPosition.arrayOrdering(0, false, false),
                1,
                () -> 1,
                Runnable::run,
                Collections.singletonList(rowNumberWindowFunction()),
                Collections.emptyList(),
                new Input[0])
        );
        tester.verifyResultAndEdgeCaseBehaviour(expectedRowNumberResult);
    }

    @Test
    public void testWindowBatchIteratorWithBatchSimulatingSource() throws Exception {
        BatchIteratorTester tester = new BatchIteratorTester(
            () -> WindowFunctionBatchIterator.of(
                new BatchSimulatingIterator<>(TestingBatchIterators.range(0, 10), 4, 2, null),
                new IgnoreRowAccounting(),
                new WindowDefinition(List.of(), null, RANGE_UNBOUNDED_PRECEDING_CURRENT_ROW),
                null,
                null,
                null,
                OrderingByPosition.arrayOrdering(0, false, false),
                1,
                () -> 1,
                Runnable::run,
                Collections.singletonList(rowNumberWindowFunction()),
                Collections.emptyList(),
                new Input[0])
        );
        tester.verifyResultAndEdgeCaseBehaviour(expectedRowNumberResult);
    }

    @Test
    public void testFrameBoundsEmptyWindow() throws Exception {
        var rows = IntStream.range(0, 10).mapToObj(i -> new Object[]{i, null}).collect(Collectors.toList());
        var result = Lists.newArrayList(sortAndComputeWindowFunctions(
            new ArrayList<>(rows),
            new WindowDefinition(List.of(), null, RANGE_UNBOUNDED_PRECEDING_CURRENT_ROW),
            null,
            null,
            null,
            null,
            1,
            () -> 1,
            Runnable::run,
            List.of(frameBoundsWindowFunction()),
            List.of(),
            args
        ).get(5, TimeUnit.SECONDS));
        var expectedBounds = tuple(0, 10);
        IntStream.range(0, 10).forEach(i -> assertThat(result.get(i), is(new Object[] { i, expectedBounds})));
    }

    @Test
    public void testFrameBoundsForPartitionedWindow() throws Exception {
        var rows = Arrays.asList(-1, 1, 1, 2, 2, 3, 4, 5, null, null);
        var rowsWithSpare = Lists2.map(rows, i -> new Object[] { i, null });
        var result = sortAndComputeWindowFunctions(
            rowsWithSpare,
            new WindowDefinition(List.of(), null, RANGE_UNBOUNDED_PRECEDING_CURRENT_ROW),
            null,
            null,
            OrderingByPosition.arrayOrdering(0, false, false),
            null,
            1,
            () -> 1,
            Runnable::run,
            List.of(frameBoundsWindowFunction()),
            List.of(),
            args
        ).get(5, TimeUnit.SECONDS);
        assertThat(
            result,
            contains(
                $(-1, tuple(0, 1)),
                $(1, tuple(0, 2)),
                $(1, tuple(0, 2)),
                $(2, tuple(0, 2)),
                $(2, tuple(0, 2)),
                $(3, tuple(0, 1)),
                $(4, tuple(0, 1)),
                $(5, tuple(0, 1)),
                $(null, tuple(0, 2)),
                $(null, tuple(0, 2))
            )
        );
    }

    @Test
    public void testFrameBoundsForPartitionedOrderedWindow() throws Exception {
        // window: partition by IC0, order by IC1
        var rows = Arrays.asList(
            $(-1 , -1, null),
            $(1, 0, null),
            $(1, 1, null),
            $(2, 2, null),
            $(2, -1, null),
            $(3, 3, null),
            $(4, 4, null),
            $(5, 5, null),
            $(null, null, null),
            $(null, null, null)
        );
        var result = sortAndComputeWindowFunctions(
            rows,
            new WindowDefinition(List.of(), null, RANGE_UNBOUNDED_PRECEDING_CURRENT_ROW),
            null,
            null,
            OrderingByPosition.arrayOrdering(0, false, false),
            OrderingByPosition.arrayOrdering(1, false, false),
            2,
            () -> 1,
            Runnable::run,
            List.of(frameBoundsWindowFunction()),
            List.of(),
            args
        ).get(5, TimeUnit.SECONDS);
        assertThat(
            result,
            contains(
                $(-1, -1, tuple(0, 1)),
                $(1, 0, tuple(0, 1)),
                $(1, 1, tuple(0, 2)),
                $(2, -1, tuple(0, 1)),
                $(2, 2, tuple(0, 2)),
                $(3, 3, tuple(0, 1)),
                $(4, 4, tuple(0, 1)),
                $(5, 5, tuple(0, 1)),
                $(null, null, tuple(0, 2)),
                $(null, null, tuple(0, 2))
            )
        );
    }

    @Test
    public void testFrameBoundsForUnboundedFollowingWindow() throws Exception {
        // window: partition by IC0, order by IC1
        var rows = Arrays.asList(
            $(-1 , -1, null),
            $(1, 0, null),
            $(1, 1, null),
            $(1, 1, null),
            $(2, 2, null),
            $(2, -1, null),
            $(3, 3, null),
            $(4, 4, null),
            $(5, 5, null),
            $(null, null, null),
            $(null, null, null)
        );
        var result = sortAndComputeWindowFunctions(
            rows,
            new WindowDefinition(List.of(), null, RANGE_CURRENT_ROW_UNBOUNDED_FOLLOWING),
            null,
            null,
            OrderingByPosition.arrayOrdering(0, false, false),
            OrderingByPosition.arrayOrdering(1, false, false),
            2,
            () -> 1,
            Runnable::run,
            List.of(frameBoundsWindowFunction()),
            List.of(),
            args
        ).get(5, TimeUnit.SECONDS);
        assertThat(
            result,
            contains(
                $(-1, -1, tuple(0, 1)),
                $(1, 0, tuple(0, 3)),
                $(1, 1, tuple(1, 3)),
                $(1, 1, tuple(1, 3)),
                $(2, -1, tuple(0, 2)),
                $(2, 2, tuple(1, 2)),
                $(3, 3, tuple(0, 1)),
                $(4, 4, tuple(0, 1)),
                $(5, 5, tuple(0, 1)),
                $(null, null, tuple(0, 2)),
                $(null, null, tuple(0, 2))
            )
        );
    }

    @Test
    public void testFrameBoundsForUnboundedFollowingUnorderedWindow() throws Exception {
        // window: partition by IC0
        var rows = Arrays.asList(
            $(-1, null),
            $(1, null),
            $(1, null),
            $(2, null),
            $(2, null),
            $(3, null),
            $(4, null),
            $(5, null),
            $(null, null),
            $(null, null)
        );
        var result = sortAndComputeWindowFunctions(
            rows,
            new WindowDefinition(List.of(), null, RANGE_CURRENT_ROW_UNBOUNDED_FOLLOWING),
            null,
            null,
            OrderingByPosition.arrayOrdering(0, false, false),
            null,
            1,
            () -> 1,
            Runnable::run,
            List.of(frameBoundsWindowFunction()),
            List.of(),
            args
        ).get(5, TimeUnit.SECONDS);
        assertThat(
            result,
            contains(
                $(-1, tuple(0, 1)),
                $(1, tuple(0, 2)),
                $(1, tuple(1, 2)),
                $(2, tuple(0, 2)),
                $(2, tuple(1, 2)),
                $(3, tuple(0, 1)),
                $(4, tuple(0, 1)),
                $(5, tuple(0, 1)),
                $(null, tuple(0, 2)),
                $(null, tuple(1, 2))
            )
        );
    }

    @Test
    public void testWindowBatchIteratorAccountsUsedMemory() {
        BatchIterator<Row> iterator = WindowFunctionBatchIterator.of(
            TestingBatchIterators.range(0, 10),
            new RowAccountingWithEstimators(List.of(DataTypes.INTEGER), ramAccountingContext, 32),
            new WindowDefinition(List.of(), null, RANGE_UNBOUNDED_PRECEDING_CURRENT_ROW),
            null,
            null,
            null,
            null,
            1,
            () -> 1,
            Runnable::run,
            List.of(rowNumberWindowFunction()),
            List.of(),
            new Input[][]{new Input[0]}
        );
        TestingRowConsumer consumer = new TestingRowConsumer();
        consumer.accept(iterator, null);
        // should've accounted for 10 integers of 48 bytes each (16 for the integer, 32 for the ArrayList element)
        assertThat(ramAccountingContext.totalBytes(), is(480L));
    }

    @Test
    public void testWindowBatchIteratorWithOrderedWindowOverNullValues() throws Exception {
        var rows = Arrays.asList(
            $(null, null),
            $(2, null)
        );
        var result = sortAndComputeWindowFunctions(
            rows,
            new WindowDefinition(List.of(), null, RANGE_UNBOUNDED_PRECEDING_CURRENT_ROW),
            null,
            null,
            null,
            OrderingByPosition.arrayOrdering(0, true, true),
            1,
            () -> 1,
            Runnable::run,
            List.of(firstCellValue()),
            List.of(),
            args
        ).get(5, TimeUnit.SECONDS);
        assertThat(
            result,
            contains(
                $(null, null),
                $(2, null)
            )
        );
    }

    private static WindowFunction firstCellValue() {
        return new WindowFunction() {
            @Override
            public Object execute(int idxInPartition, WindowFrameState currentFrame, List<? extends CollectExpression<Row, ?>> expressions, Input... args) {
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

    private static WindowFunction frameBoundsWindowFunction() {
        return new WindowFunction() {
            @Override
            public Object execute(int idxInPartition, WindowFrameState currentFrame, List<? extends CollectExpression<Row, ?>> expressions, Input... args) {
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

    private static WindowFunction rowNumberWindowFunction() {
        return new WindowFunction() {
            @Override
            public Object execute(int idxInPartition, WindowFrameState currentFrame, List<? extends CollectExpression<Row, ?>> expressions, Input... args) {
                return idxInPartition + 1; // sql row numbers are 1-indexed;
            }

            @Override
            public FunctionInfo info() {
                return new FunctionInfo(
                    new FunctionIdent("row_number", Collections.emptyList()),
                    DataTypes.INTEGER);
            }
        };
    }

     */

}
