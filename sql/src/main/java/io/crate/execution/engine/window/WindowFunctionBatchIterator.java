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

import com.google.common.collect.Iterables;
import io.crate.analyze.WindowDefinition;
import io.crate.analyze.WindowFrameDefinition;
import io.crate.breaker.RowAccounting;
import io.crate.data.BatchIterator;
import io.crate.data.BatchIterators;
import io.crate.data.Buckets;
import io.crate.data.CollectingBatchIterator;
import io.crate.data.Input;
import io.crate.data.Row;
import io.crate.execution.engine.collect.CollectExpression;
import io.crate.execution.engine.sort.Sort;
import io.crate.expression.symbol.InputColumn;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.SymbolType;
import io.crate.types.DataType;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.logging.Loggers;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.IntSupplier;
import java.util.stream.Collectors;

import static io.crate.common.collections.Lists2.findFirstNonPeer;
import static io.crate.sql.tree.WindowFrame.Mode.RANGE;

/**
 * BatchIterator which computes window functions (incl. partitioning + ordering)
 *
 * <p>
 * This BatchIterator is implemented as a pipeline breaker because the ORDER BY requires to go through ALL rows.
 * In the case there is no PARTITION BY/ORDER BY we still need to process all rows because in that case all rows
 * belong to the same frame and window functions can have access to all rows within a frame.
 *
 * Doing the ORDER BY within the BatchIterator (instead of demanding the source to be pre-sorted) allows us to operate
 * on a in-memory list with RandomAccess instead of using the BatchIterator interface for the source.
 * </p>
 *
 * <pre>
 *     Partition
 *      |
 *     Order
 *      |
 *     Framing  - Window function computation
 *                      - Determine partition boundaries
 *                      - Determine window frame
 *                      - Compute window function over the frame and output tuple
 * </pre>
 */
public final class WindowFunctionBatchIterator {

    private static final Logger LOGGER = Loggers.getLogger(WindowFunctionBatchIterator.class);

    public static BatchIterator<Row> of(BatchIterator<Row> source,
                                        RowAccounting<Row> rowAccounting,
                                        WindowDefinition windowDefinition,
                                        @Nullable Object startFrameOffset,
                                        @Nullable Object endFrameOffset,
                                        Comparator<Object[]> cmpPartitionBy,
                                        Comparator<Object[]> cmpOrderBy,
                                        int numCellsInSourceRow,
                                        IntSupplier numAvailableThreads,
                                        Executor executor,
                                        List<WindowFunction> windowFunctions,
                                        List<? extends CollectExpression<Row, ?>> argsExpressions,
                                        Input[]... args) {
        // As optimization we use 1 list that acts both as inputs(source) and as outputs.
        // The window function results are injected during the computation into spare cells that are eagerly created
        Function<Row, Object[]> materialize = row -> {
            rowAccounting.accountForAndMaybeBreak(row);
            return materializeWithSpare(row, windowFunctions.size());
        };
        return CollectingBatchIterator.newInstance(
            source,
            src -> BatchIterators
                .collect(src, Collectors.mapping(materialize, Collectors.toList()))
                .thenCompose(rows -> sortAndComputeWindowFunctions(
                    rows,
                    windowDefinition,
                    startFrameOffset,
                    endFrameOffset,
                    cmpPartitionBy,
                    cmpOrderBy,
                    numCellsInSourceRow,
                    numAvailableThreads,
                    executor,
                    windowFunctions,
                    argsExpressions,
                    args
                ))
                .thenApply(rows -> Iterables.transform(rows, Buckets.arrayToSharedRow()::apply)),
            source.involvesIO()
        );
    }

    private static Object[] materializeWithSpare(Row row, int numWindowFunctions) {
        Object[] cells = new Object[row.numColumns() + numWindowFunctions];
        for (int i = 0; i < row.numColumns(); i++) {
            cells[i] = row.get(i);
        }
        return cells;
    }

    static CompletableFuture<Iterable<Object[]>> sortAndComputeWindowFunctions(
        List<Object[]> rows,
        WindowDefinition windowDefinition,
        @Nullable Object startFrameOffset,
        @Nullable Object endFrameOffset,
        @Nullable Comparator<Object[]> cmpPartitionBy,
        @Nullable Comparator<Object[]> cmpOrderBy,
        int numCellsInSourceRow,
        IntSupplier numAvailableThreads,
        Executor executor,
        List<WindowFunction> windowFunctions,
        List<? extends CollectExpression<Row, ?>> argsExpressions,
        Input[]... args) {

        Function<List<Object[]>, Iterable<Object[]>> computeWindowsFn = sortedRows -> computeWindowFunctions(
            sortedRows,
            windowDefinition,
            startFrameOffset,
            endFrameOffset,
            cmpPartitionBy,
            cmpOrderBy,
            numCellsInSourceRow,
            windowFunctions,
            argsExpressions,
            args);
        Comparator<Object[]> cmpPartitionThenOrderBy = joinCmp(cmpPartitionBy, cmpOrderBy);
        if (cmpPartitionThenOrderBy == null) {
            return CompletableFuture.completedFuture(computeWindowsFn.apply(rows));
        } else {
            int minItemsPerThread = 1 << 13; // Same as Arrays.MIN_ARRAY_SORT_GRAN
            return Sort
                .parallelSort(rows, cmpPartitionThenOrderBy, minItemsPerThread, numAvailableThreads.getAsInt(), executor)
                .thenApply(computeWindowsFn);
        }
    }

    private static Iterable<Object[]> computeWindowFunctions(List<Object[]> sortedRows,
                                                             WindowDefinition windowDefinition,
                                                             @Nullable Object startFrameOffset,
                                                             @Nullable Object endFrameOffset,
                                                             @Nullable Comparator<Object[]> cmpPartitionBy,
                                                             @Nullable Comparator<Object[]> cmpOrderBy,
                                                             int numCellsInSourceRow,
                                                             List<WindowFunction> windowFunctions,
                                                             List<? extends CollectExpression<Row, ?>> argsExpressions,
                                                             Input[]... args) {
        return () -> new Iterator<>() {

            private boolean isTraceEnabled = LOGGER.isTraceEnabled();

            private final int start = 0;
            private final int end = sortedRows.size();
            private final WindowFrameState frame = new WindowFrameState(start, end, sortedRows);
            private final WindowFrameDefinition frameDefinition = windowDefinition.windowFrameDefinition();
            private final Tuple<Object[], int[]> startOffsetCellsAndPositions =
                getOffsetCellsAndPositions(windowDefinition, startFrameOffset, numCellsInSourceRow);

            private final Tuple<Object[], int[]> endOffsetCellsAndPositions =
                getOffsetCellsAndPositions(windowDefinition, endFrameOffset, numCellsInSourceRow);

            @Nullable
            private final BinaryOperator<Object> substractFunction =
                getFunctionForRangeCustomOffset(windowDefinition, startFrameOffset, ArithmeticOperatorsFactory::getSubtractFunction);
            @Nullable
            private final BinaryOperator<Object> addFunction =
                getFunctionForRangeCustomOffset(windowDefinition, endFrameOffset, ArithmeticOperatorsFactory::getAddFunction);

            private final Object[] startProbeValue = new Object[numCellsInSourceRow];
            private final Object[] endProbeValue = new Object[numCellsInSourceRow];

            private int pStart = start;
            private int pEnd = findFirstNonPeer(sortedRows, pStart, end, cmpPartitionBy);
            private int i = 0;

            private int idxInPartition = 0;

            @Override
            public boolean hasNext() {
                return i < end;
            }

            @Override
            public Object[] next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                if (i == pEnd) {
                    pStart = i;
                    idxInPartition = 0;
                    pEnd = findFirstNonPeer(sortedRows, pStart, end, cmpPartitionBy);
                }

                if (windowDefinition.windowFrameDefinition().type() == RANGE) {
                    // if the offsetCell position is -1 the window is ordered by a Literal so we leave the
                    // probe value to null so it doesn't impact ordering (ie. all values will be consistently GT or LT
                    // `null`)
                    for (int offsetCellPos : startOffsetCellsAndPositions.v2()) {
                        if (offsetCellPos != -1) {
                            startProbeValue[offsetCellPos] = substractFunction
                                .apply(sortedRows.get(i)[offsetCellPos], startOffsetCellsAndPositions.v1()[offsetCellPos]);
                        }
                    }

                    for (int offsetCellPos : endOffsetCellsAndPositions.v2()) {
                        if (offsetCellPos != -1) {
                            endProbeValue[offsetCellPos] = addFunction
                                .apply(sortedRows.get(i)[offsetCellPos], endOffsetCellsAndPositions.v1()[offsetCellPos]);
                        }
                    }
                }

                int wBegin = frameDefinition.start().type().getStart(
                    frameDefinition.type(),
                    pStart,
                    pEnd,
                    i,
                    startFrameOffset,
                    startProbeValue,
                    cmpOrderBy,
                    sortedRows
                );

                int wEnd = frameDefinition.end().type().getEnd(
                    frameDefinition.type(),
                    pStart,
                    pEnd,
                    i,
                    endFrameOffset,
                    endProbeValue,
                    cmpOrderBy,
                    sortedRows
                );

                frame.updateBounds(pStart, pEnd, wBegin, wEnd);
                final Object[] row = computeAndInjectResults(
                    sortedRows, numCellsInSourceRow, windowFunctions, frame, i, idxInPartition, argsExpressions, args);

                if (isTraceEnabled) {
                    LOGGER.trace(
                        "idx={} idxInPartition={} pStart={} pEnd={} wBegin={} wEnd={} row={}",
                        i, idxInPartition, pStart, pEnd, wBegin, wEnd, Arrays.toString(sortedRows.get(i)));
                }
                i++;
                idxInPartition++;
                return row;
            }
        };
    }

    @Nullable
    private static BinaryOperator<Object> getFunctionForRangeCustomOffset(WindowDefinition windowDefinition,
                                                                          @Nullable Object frameOffset,
                                                                          Function<DataType, BinaryOperator<Object>> functionSupplier) {
        if (windowDefinition.windowFrameDefinition().type() == RANGE && frameOffset != null) {
            assert windowDefinition.orderBy() !=
                   null : "The window definition must be ordered if custom offsets are specified";
            DataType orderByDataType = windowDefinition.orderBy().orderBySymbols().get(0).valueType();
            return functionSupplier.apply(orderByDataType);
        }
        return null;
    }

    private static Tuple<Object[], int[]> getOffsetCellsAndPositions(WindowDefinition windowDefinition,
                                                                     @Nullable Object frameOffset,
                                                                     int numCellsInSourceRow) {
        if (frameOffset != null) {
            assert windowDefinition.orderBy() !=
                   null : "The window definition must be ordered if custom offsets are specified";
            List<Symbol> orderBySymbols = windowDefinition.orderBy().orderBySymbols();
            Object[] offsetCells = new Object[numCellsInSourceRow];
            int[] offsetColumnsPositions = new int[orderBySymbols.size()];
            for (int i = 0; i < orderBySymbols.size(); i++) {
                var orderSymbol = orderBySymbols.get(i);
                if (orderSymbol.symbolType() == SymbolType.LITERAL) {
                    offsetColumnsPositions[i] = -1;
                    continue;
                }
                assert orderSymbol instanceof InputColumn;
                int orderByColumnPosition = ((InputColumn) orderSymbol).index();
                offsetCells[orderByColumnPosition] = orderSymbol.valueType().value(frameOffset);
                offsetColumnsPositions[i] = orderByColumnPosition;
            }
            return new Tuple<>(offsetCells, offsetColumnsPositions);
        } else {
            return new Tuple<>(new Object[0], new int[0]);
        }
    }

    @Nullable
    private static Comparator<Object[]> joinCmp(@Nullable Comparator<Object[]> cmpPartitionBy,
                                                @Nullable Comparator<Object[]> cmpOrderBy) {
        if (cmpPartitionBy == null) {
            return cmpOrderBy;
        }
        if (cmpOrderBy == null) {
            return cmpPartitionBy;
        }
        return cmpPartitionBy.thenComparing(cmpOrderBy);
    }

    private static Object[] computeAndInjectResults(List<Object[]> rows,
                                                    int numCellsInSourceRow,
                                                    List<WindowFunction> windowFunctions,
                                                    WindowFrameState frame,
                                                    int idx,
                                                    int idxInPartition,
                                                    List<? extends CollectExpression<Row, ?>> argsExpressions,
                                                    Input[]... args) {
        Object[] row = rows.get(idx);
        for (int c = 0; c < windowFunctions.size(); c++) {
            WindowFunction windowFunction = windowFunctions.get(c);
            Object result = windowFunction.execute(idxInPartition, frame, argsExpressions, args[c]);
            row[numCellsInSourceRow + c] = result;
        }
        return row;
    }
}
