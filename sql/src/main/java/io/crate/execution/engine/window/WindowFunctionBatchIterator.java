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
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.Loggers;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Function;
import java.util.function.IntSupplier;
import java.util.stream.Collectors;

import static io.crate.sql.tree.FrameBound.Type.CURRENT_ROW;
import static io.crate.sql.tree.FrameBound.Type.UNBOUNDED_PRECEDING;

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
                                        WindowFrameDefinition frameDefinition,
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
                    frameDefinition,
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
        WindowFrameDefinition frameDefinition,
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
            frameDefinition,
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
                                                             WindowFrameDefinition frameDefinition,
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

            private int pStart = start;
            private int pEnd = findFirstNonPeer(sortedRows, pStart, end, cmpPartitionBy);
            private int i = 0;
            private int idxInPartition = 0;

            private boolean isUnboundedPrecedingCurrentRow = frameDefinition.start().type() == UNBOUNDED_PRECEDING &&
                                                             frameDefinition.end().type() == CURRENT_ROW;

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

                int wBegin;
                int wEnd;

                if (isUnboundedPrecedingCurrentRow) {
                    // UNBOUNDED PRECEDING -> CURRENT ROW - Frame always starts at the start of the partition
                    wBegin = pStart;
                    wEnd = findFirstNonPeer(sortedRows, i, pEnd, cmpOrderBy);
                } else {
                    // CURRENT ROW -> UNBOUNDED FOLLOWING - Frame start position changes with each window row
                    if (pStart == i) {
                        // if we just changed partition, make the beginning of the window be the beginning of the partition
                        wBegin = pStart;
                    } else {
                        if (cmpOrderBy == null) {
                            wBegin = i;
                        } else {
                            // within a partition, we will push the window beginning index forward only when encountering
                            // non-peers elements
                            wBegin = arePeers(sortedRows, frame.lowerBound() + frame.partitionStart(), i, cmpOrderBy) ?
                                (frame.lowerBound() + frame.partitionStart()) : i;
                        }
                    }
                    wEnd = pEnd;
                }

                frame.updateBounds(pStart, wBegin, wEnd);
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

    private static <T> boolean arePeers(List<T> rows, int pos1, int pos2, Comparator<T> cmp) {
        T fst = rows.get(pos1);
        return cmp.compare(fst, rows.get(pos2)) == 0;
    }

    static <T> int findFirstNonPeer(List<T> rows, int begin, int end, @Nullable Comparator<T> cmp) {
        if (cmp == null || (begin + 1) >= end) {
            return end;
        }
        T fst = rows.get(begin);
        if (cmp.compare(fst, rows.get(begin + 1)) != 0) {
            return begin + 1;
        }
        /*
         * Adapted binarySearch algorithm to find the first non peer (instead of the first match)
         * This depends on there being at least some EQ values;
         * Whenever we find a EQ pair we check if the following element isn't EQ anymore.
         *
         * E.g.
         *
         * i:     0  1  2  3  4  5  6  7
         * rows: [1, 1, 1, 1, 4, 4, 5, 6]
         *        ^ [1  1  1  4  4  5  6]
         *        +-----------^
         *           cmp: -1
         *        1 [1  1  1  4] 4  5  6
         *        ^     ^
         *        +-----+
         *           cmp: 0 --> cmp (mid +1) != 0 --> false
         *        1  1  1 [1  4] 4  5  6
         *        ^        ^
         *        +--------+
         *           cmp: 0 --> cmp (mid +1) != 0 --> true
         */
        int low = begin + 1;
        int high = end;
        while (low <= high) {
            int mid = (low + high) >>> 1;
            T t = rows.get(mid);
            int cmpResult = cmp.compare(fst, t);
            if (cmpResult == 0) {
                int next = mid + 1;
                if (next == high || cmp.compare(fst, rows.get(next)) != 0) {
                    return next;
                } else {
                    low = next;
                }
            } else if (cmpResult < 0) {
                high = mid;
            } else {
                low = mid;
            }
        }
        return end;
    }
}
