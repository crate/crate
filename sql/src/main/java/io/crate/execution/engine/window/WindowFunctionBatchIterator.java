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

import com.google.common.collect.Lists;
import io.crate.breaker.RowAccounting;
import io.crate.data.BatchIterator;
import io.crate.data.BatchIterators;
import io.crate.data.Buckets;
import io.crate.data.CollectingBatchIterator;
import io.crate.data.Input;
import io.crate.data.Row;
import io.crate.execution.engine.collect.CollectExpression;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.Loggers;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

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
                                        RowAccounting rowAccounting,
                                        Comparator<Object[]> cmpPartitionBy,
                                        Comparator<Object[]> cmpOrderBy,
                                        int numCellsInSourceRow,
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
                .thenApply(rows -> computeWindowFunctions(
                    rows,
                    cmpPartitionBy,
                    cmpOrderBy,
                    numCellsInSourceRow,
                    windowFunctions,
                    argsExpressions,
                    args
                ))
                .thenApply(rows -> Lists.transform(rows, Buckets.arrayToSharedRow()::apply)),
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

    static List<Object[]> computeWindowFunctions(List<Object[]> rows,
                                                 @Nullable Comparator<Object[]> cmpPartitionBy,
                                                 @Nullable Comparator<Object[]> cmpOrderBy,
                                                 int numCellsInSourceRow,
                                                 List<WindowFunction> windowFunctions,
                                                 List<? extends CollectExpression<Row, ?>> argsExpressions,
                                                 Input[]... args) {
        if (rows.isEmpty()) {
            return rows;
        }
        Comparator<Object[]> cmpPartitionThenOrderBy = joinCmp(cmpPartitionBy, cmpOrderBy);
        if (cmpPartitionThenOrderBy != null) {
            rows.sort(cmpPartitionThenOrderBy);
        }
        int start = 0;
        int end = rows.size();
        int pStart = start;
        int pEnd = findFirstNonPeer(rows, pStart, end, cmpPartitionBy);
        var frame = new WindowFrameState(pStart, pEnd, rows);
        boolean isTraceEnabled = LOGGER.isTraceEnabled();
        for (int i = 0, idxInPartition = 0; i < end; i++, idxInPartition++) {
            if (i == pEnd) {
                pStart = i;
                idxInPartition = 0;
                pEnd = findFirstNonPeer(rows, pStart, end, cmpPartitionBy);
            }
            int wBegin = pStart; // UNBOUNDED PRECEDING -> Frame always starts at the start of the partition
            int wEnd = findFirstNonPeer(rows, i, pEnd, cmpOrderBy);
            frame.updateBounds(pStart, wBegin, wEnd);
            computeAndInjectResults(
                rows, numCellsInSourceRow, windowFunctions, frame, i, idxInPartition, argsExpressions, args);

            if (isTraceEnabled) {
                LOGGER.trace(
                    "idx={} idxInPartition={} pStart={} pEnd={} wBegin={} wEnd={} row={}",
                    i, idxInPartition, pStart, pEnd, wBegin, wEnd, Arrays.toString(rows.get(i)));
            }
        }
        return rows;
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

    private static void computeAndInjectResults(List<Object[]> rows,
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
    }

    private static <T> int findFirstNonPeer(List<T> rows, int begin, int end, @Nullable Comparator<T> cmp) {
        if (cmp == null) {
            return end;
        }
        T fst = rows.get(begin);
        for (int i = begin + 1; i < end; i++) {
            if (cmp.compare(fst, rows.get(i)) != 0) {
                return i;
            }
        }
        return end;
    }
}
