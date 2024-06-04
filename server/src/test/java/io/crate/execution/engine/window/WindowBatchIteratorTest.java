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

package io.crate.execution.engine.window;

import static com.carrotsearch.randomizedtesting.RandomizedTest.$;
import static io.crate.execution.engine.window.WindowFunctionBatchIterator.sortAndComputeWindowFunctions;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.LongConsumer;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.junit.Test;

import io.crate.breaker.ConcurrentRamAccounting;
import io.crate.breaker.TypedRowAccounting;
import io.crate.common.collections.Lists;
import io.crate.common.collections.Tuple;
import io.crate.data.BatchIterator;
import io.crate.data.Input;
import io.crate.data.Row;
import io.crate.data.breaker.RamAccounting;
import io.crate.data.testing.BatchIteratorTester;
import io.crate.data.testing.BatchSimulatingIterator;
import io.crate.data.testing.TestingBatchIterators;
import io.crate.data.testing.TestingRowConsumer;
import io.crate.execution.engine.collect.CollectExpression;
import io.crate.execution.engine.sort.OrderingByPosition;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.sql.tree.FrameBound;
import io.crate.sql.tree.WindowFrame;
import io.crate.types.DataTypes;

public class WindowBatchIteratorTest {

    private Input[][] args = {new Input[0]};

    private List<Object[]> expectedRowNumberResult = IntStream.range(0, 10)
        .mapToObj(l -> new Object[]{l, l + 1}).collect(toList());

    @Test
    public void testWindowBatchIterator() throws Exception {
        var tester = BatchIteratorTester.forRows(
            () -> {
                Comparator<Object[]> cmpOrderBy = OrderingByPosition.arrayOrdering(DataTypes.INTEGER, 0, false, false);
                return WindowFunctionBatchIterator.of(
                    TestingBatchIterators.range(0, 10),
                    ignored -> {},
                    new IgnoreRowAccounting(),
                    getComputeFrameStart(cmpOrderBy, FrameBound.Type.UNBOUNDED_PRECEDING),
                    getComputeFrameEnd(cmpOrderBy, FrameBound.Type.CURRENT_ROW),
                    null,
                    cmpOrderBy,
                    1,
                    () -> 1,
                    Runnable::run,
                    Collections.singletonList(rowNumberWindowFunction()),
                    Collections.emptyList(),
                    new Boolean[]{null},
                    new Input[0]);
            }
        );
        tester.verifyResultAndEdgeCaseBehaviour(expectedRowNumberResult);
    }

    @Test
    public void testWindowBatchIteratorWithBatchSimulatingSource() throws Exception {
        var tester = BatchIteratorTester.forRows(
            () -> {
                Comparator<Object[]> cmpOrderBy = OrderingByPosition.arrayOrdering(DataTypes.INTEGER, 0, false, false);
                return WindowFunctionBatchIterator.of(
                    new BatchSimulatingIterator<>(TestingBatchIterators.range(0, 10), 4, 2, null),
                    ignored -> {},
                    new IgnoreRowAccounting(),
                    getComputeFrameStart(cmpOrderBy, FrameBound.Type.UNBOUNDED_PRECEDING),
                    getComputeFrameEnd(cmpOrderBy, FrameBound.Type.CURRENT_ROW),
                    null,
                    cmpOrderBy,
                    1,
                    () -> 1,
                    Runnable::run,
                    Collections.singletonList(rowNumberWindowFunction()),
                    Collections.emptyList(),
                    new Boolean[]{null},
                    new Input[0]);
            }
        );
        tester.verifyResultAndEdgeCaseBehaviour(expectedRowNumberResult);
    }

    @Test
    public void testFrameBoundsEmptyWindow() throws Exception {
        var rows = IntStream.range(0, 10).mapToObj(i -> new Object[]{i, null}).collect(toList());
        var result = StreamSupport.stream(sortAndComputeWindowFunctions(
            new ArrayList<>(rows),
            ignored -> {},
            getComputeFrameStart(null, FrameBound.Type.UNBOUNDED_PRECEDING),
            getComputeFrameEnd(null, FrameBound.Type.CURRENT_ROW),
            null,
            null,
            1,
            () -> 1,
            Runnable::run,
            List.of(frameBoundsWindowFunction()),
            List.of(),
            new Boolean[]{null},
            args).get(5, TimeUnit.SECONDS).spliterator(), false)
            .collect(toList());
        var expectedBounds = new Tuple<>(0, 10);
        IntStream.range(0, 10).forEach(i -> assertThat(result.get(i), is(new Object[] { i, expectedBounds})));
    }

    @Test
    public void testFrameBoundsForPartitionedWindow() throws Exception {
        var rows = Arrays.asList(-1, 1, 1, 2, 2, 3, 4, 5, null, null);
        var rowsWithSpare = Lists.map(rows, i -> new Object[] { i, null });
        var result = sortAndComputeWindowFunctions(
            rowsWithSpare,
            ignored -> {},
            getComputeFrameStart(null, FrameBound.Type.UNBOUNDED_PRECEDING),
            getComputeFrameEnd(null, FrameBound.Type.CURRENT_ROW),
            OrderingByPosition.arrayOrdering(DataTypes.INTEGER, 0, false, false),
            null,
            1,
            () -> 1,
            Runnable::run,
            List.of(frameBoundsWindowFunction()),
            List.of(),
            new Boolean[]{null},
            args
        ).get(5, TimeUnit.SECONDS);
        assertThat(
            result,
            contains(
                $(-1, new Tuple<>(0, 1)),
                $(1, new Tuple<>(0, 2)),
                $(1, new Tuple<>(0, 2)),
                $(2, new Tuple<>(0, 2)),
                $(2, new Tuple<>(0, 2)),
                $(3, new Tuple<>(0, 1)),
                $(4, new Tuple<>(0, 1)),
                $(5, new Tuple<>(0, 1)),
                $(null, new Tuple<>(0, 2)),
                $(null, new Tuple<>(0, 2))
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
        Comparator<Object[]> cmpOrderBy = OrderingByPosition.arrayOrdering(DataTypes.INTEGER, 1, false, false);
        var result = sortAndComputeWindowFunctions(
            rows,
            ignored -> {},
            getComputeFrameStart(cmpOrderBy, FrameBound.Type.UNBOUNDED_PRECEDING),
            getComputeFrameEnd(cmpOrderBy, FrameBound.Type.CURRENT_ROW),
            OrderingByPosition.arrayOrdering(DataTypes.INTEGER, 0, false, false),
            cmpOrderBy,
            2,
            () -> 1,
            Runnable::run,
            List.of(frameBoundsWindowFunction()),
            List.of(),
            new Boolean[]{null},
            args
        ).get(5, TimeUnit.SECONDS);
        assertThat(
            result,
            contains(
                $(-1, -1, new Tuple<>(0, 1)),
                $(1, 0, new Tuple<>(0, 1)),
                $(1, 1, new Tuple<>(0, 2)),
                $(2, -1, new Tuple<>(0, 1)),
                $(2, 2, new Tuple<>(0, 2)),
                $(3, 3, new Tuple<>(0, 1)),
                $(4, 4, new Tuple<>(0, 1)),
                $(5, 5, new Tuple<>(0, 1)),
                $(null, null, new Tuple<>(0, 2)),
                $(null, null, new Tuple<>(0, 2))
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
        Comparator<Object[]> cmpOrderBy = OrderingByPosition.arrayOrdering(DataTypes.INTEGER, 1, false, false);
        var result = sortAndComputeWindowFunctions(
            rows,
            ignored -> {},
            getComputeFrameStart(cmpOrderBy, FrameBound.Type.CURRENT_ROW),
            getComputeFrameEnd(cmpOrderBy, FrameBound.Type.UNBOUNDED_FOLLOWING),
            OrderingByPosition.arrayOrdering(DataTypes.INTEGER, 0, false, false),
            cmpOrderBy,
            2,
            () -> 1,
            Runnable::run,
            List.of(frameBoundsWindowFunction()),
            List.of(),
            new Boolean[]{null},
            args
        ).get(5, TimeUnit.SECONDS);
        assertThat(
            result,
            contains(
                $(-1, -1, new Tuple<>(0, 1)),
                $(1, 0, new Tuple<>(0, 3)),
                $(1, 1, new Tuple<>(1, 3)),
                $(1, 1, new Tuple<>(1, 3)),
                $(2, -1, new Tuple<>(0, 2)),
                $(2, 2, new Tuple<>(1, 2)),
                $(3, 3, new Tuple<>(0, 1)),
                $(4, 4, new Tuple<>(0, 1)),
                $(5, 5, new Tuple<>(0, 1)),
                $(null, null, new Tuple<>(0, 2)),
                $(null, null, new Tuple<>(0, 2))
            )
        );
    }

    private ComputeFrameBoundary<Object[]> getComputeFrameEnd(Comparator<Object[]> cmpOrderBy, FrameBound.Type frameBoundType) {
        return (partitionStart, partitionEnd, currentIndex, sortedRows) -> frameBoundType.getEnd(
            WindowFrame.Mode.RANGE,
            partitionStart,
            partitionEnd,
            currentIndex,
            null,
            null,
            cmpOrderBy,
            sortedRows
        );
    }

    private ComputeFrameBoundary<Object[]> getComputeFrameStart(Comparator<Object[]> cmpOrderBy, FrameBound.Type frameBoundType) {
        return (partitionStart, partitionEnd, currentIndex, sortedRows) -> frameBoundType.getStart(
            WindowFrame.Mode.RANGE,
            partitionStart,
            partitionEnd,
            currentIndex,
            null,
            null,
            cmpOrderBy,
            sortedRows
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
            ignored -> {},
            getComputeFrameStart(null, FrameBound.Type.CURRENT_ROW),
            getComputeFrameEnd(null, FrameBound.Type.UNBOUNDED_FOLLOWING),
            OrderingByPosition.arrayOrdering(DataTypes.INTEGER, 0, false, false),
            null,
            1,
            () -> 1,
            Runnable::run,
            List.of(frameBoundsWindowFunction()),
            List.of(),
            new Boolean[]{null},
            args
        ).get(5, TimeUnit.SECONDS);
        assertThat(
            result,
            contains(
                $(-1, new Tuple<>(0, 1)),
                $(1, new Tuple<>(0, 2)),
                $(1, new Tuple<>(1, 2)),
                $(2, new Tuple<>(0, 2)),
                $(2, new Tuple<>(1, 2)),
                $(3, new Tuple<>(0, 1)),
                $(4, new Tuple<>(0, 1)),
                $(5, new Tuple<>(0, 1)),
                $(null, new Tuple<>(0, 2)),
                $(null, new Tuple<>(1, 2))
            )
        );
    }

    @Test
    public void testWindowBatchIteratorAccountsUsedMemory() {
        RamAccounting ramAccounting = ConcurrentRamAccounting.forCircuitBreaker("test", new NoopCircuitBreaker("dummy"), 0);
        BatchIterator<Row> iterator = WindowFunctionBatchIterator.of(
            TestingBatchIterators.range(0, 10),
            ignored -> {},
            new TypedRowAccounting(List.of(DataTypes.INTEGER), ramAccounting, 32),
            (partitionStart, partitionEnd, currentIndex, sortedRows) -> 0,
            (partitionStart, partitionEnd, currentIndex, sortedRows) -> currentIndex,
            null,
            null,
            1,
            () -> 1,
            Runnable::run,
            List.of(rowNumberWindowFunction()),
            List.of(),
            new Boolean[]{null},
            new Input[][]{new Input[0]}
        );
        TestingRowConsumer consumer = new TestingRowConsumer();
        consumer.accept(iterator, null);
        // should've accounted for 10 integers of 48 bytes each (16 for the integer, 32 for the ArrayList element)
        assertThat(ramAccounting.totalBytes(), is(480L));
    }

    @Test
    public void testWindowBatchIteratorWithOrderedWindowOverNullValues() throws Exception {
        var rows = Arrays.asList(
            $(null, null),
            $(2, null)
        );
        var result = sortAndComputeWindowFunctions(
            rows,
            ignored -> {},
            (partitionStart, partitionEnd, currentIndex, sortedRows) -> 0,
            (partitionStart, partitionEnd, currentIndex, sortedRows) -> currentIndex,
            null,
            OrderingByPosition.arrayOrdering(DataTypes.INTEGER, 0, true, true),
            1,
            () -> 1,
            Runnable::run,
            List.of(firstCellValue()),
            List.of(),
            new Boolean[]{null},
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
            public Object execute(LongConsumer allocateBytes,
                                  int idxInPartition,
                                  WindowFrameState currentFrame,
                                  List<? extends CollectExpression<Row, ?>> expressions,
                                  Boolean ignoreNulls,
                                  Input<?> ... args) {
                return currentFrame.getRows().iterator().next()[0];
            }

            @Override
            public Signature signature() {
                return Signature.window("first_cell_value", DataTypes.INTEGER.getTypeSignature());
            }

            @Override
            public BoundSignature boundSignature() {
                return BoundSignature.sameAsUnbound(signature());
            }
        };
    }

    private static WindowFunction frameBoundsWindowFunction() {
        return new WindowFunction() {
            @Override
            public Object execute(LongConsumer allocateBytes,
                                  int idxInPartition,
                                  WindowFrameState currentFrame,
                                  List<? extends CollectExpression<Row, ?>> expressions,
                                  Boolean ignoreNulls,
                                  Input<?> ... args) {
                return new Tuple<>(currentFrame.lowerBound(), currentFrame.upperBoundExclusive());
            }

            @Override
            public Signature signature() {
                return Signature.window("a_frame_bounded_window_function", DataTypes.INTEGER.getTypeSignature());
            }

            @Override
            public BoundSignature boundSignature() {
                return BoundSignature.sameAsUnbound(signature());
            }
        };
    }

    private static WindowFunction rowNumberWindowFunction() {
        return new WindowFunction() {
            @Override
            public Object execute(LongConsumer allocateBytes,
                                  int idxInPartition,
                                  WindowFrameState currentFrame,
                                  List<? extends CollectExpression<Row, ?>> expressions,
                                  Boolean ignoreNulls,
                                  Input<?> ... args) {
                return idxInPartition + 1; // sql row numbers are 1-indexed;
            }

            @Override
            public Signature signature() {
                return Signature.window("row_number", DataTypes.INTEGER.getTypeSignature());
            }

            @Override
            public BoundSignature boundSignature() {
                return BoundSignature.sameAsUnbound(signature());
            }
        };
    }
}
