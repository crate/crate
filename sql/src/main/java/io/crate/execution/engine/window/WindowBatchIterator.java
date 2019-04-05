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

import com.google.common.annotations.VisibleForTesting;
import io.crate.analyze.OrderBy;
import io.crate.analyze.WindowDefinition;
import io.crate.breaker.RamAccountingContext;
import io.crate.breaker.RowAccountingWithEstimators;
import io.crate.data.BatchIterator;
import io.crate.data.Input;
import io.crate.data.MappedForwardingBatchIterator;
import io.crate.data.Row;
import io.crate.data.RowN;
import io.crate.execution.engine.collect.CollectExpression;
import io.crate.execution.engine.join.RamAccountingBatchIterator;
import io.crate.types.DataType;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.function.BiPredicate;

/**
 * BatchIterator that computes an aggregate or window function against a window over the source batch iterator.
 * Executing a function over a window means having to range over a set of rows from the source iterator. By default (ie.
 * empty window) the range is between unbounded preceding and current row. This means the function will need to be
 * computed against all the rows from the beginning of the partition and through the current row's _last peer_. The
 * function result will be emitted for every row in the window.
 * <p>
 *      For eg.
 * <p>
 *      source : [ 1, 2, 3, 4 ]
 * <p>
 *      function : sum
 * <p>
 *      window definition : empty (equivalent to an empty OVER() clause)
 * <p>
 *      window range is all the rows in the source as all rows are peers if there is no ORDER BY in the OVER clause
 * <p>
 *      Execution steps :
 * <p>
 *          1. compute the function while the rows are peers
 * <p>
 *                  1 + 2 + 3 + 4 = 10
 * <p>
 *          2. for each row in the window emit the function result
 * <p>
 *                  [ 10, 10, 10, 10 ]
 */
public class WindowBatchIterator extends MappedForwardingBatchIterator<Row, Row> {

    private final BatchIterator<Row> source;
    private final List<WindowFunction> functions;
    private final Object[] outgoingCells;
    private final LinkedList<Object[]> standaloneOutgoingCells;
    private final LinkedList<Object[]> outstandingResults;
    private final List<CollectExpression<Row, ?>> standaloneExpressions;
    private final BiPredicate<Object[], Object[]> arePeerCellsPredicate;
    private final List<? extends CollectExpression<Row, ?>> windowFuncArgsExpressions;
    private final Input[][] windowFuncArgsInputs;

    private Row currentWindowRow;
    /**
     * Represents the "window row", the row for which we are computing the window, and once that's complete, execute the
     * window function for.
     */
    private Object[] currentRowCells = null;

    private int sourceRowsConsumed;
<<<<<<< HEAD
    private int windowRowPosition;
=======
    private int emittedRows;
    private int zeroIndexedRowNumber;
>>>>>>> 1a64bd31c2... This fixes a few bugs on processing window functions over partitioned windows.
    private final List<Object[]> windowForCurrentRow = new ArrayList<>();
    /**
     * Represents the start index of the new rows that were added in the current frame (compared to the previous frame)
     * in the list of rows that define the window {@link #windowForCurrentRow}
     */
    private int newRowsInCurrentFrameStartIdx = -1;
    private boolean foundCurrentRowsLastPeer = false;
    private int windowFunctionsCount;
    private final OrderBy order;

    @VisibleForTesting
    public WindowBatchIterator(WindowDefinition windowDefinition,
                        List<Input<?>> standaloneInputs,
                        List<CollectExpression<Row, ?>> standaloneExpressions,
                        BatchIterator<Row> source,
                        List<WindowFunction> functions,
                        List<? extends CollectExpression<Row, ?>> windowFuncArgsExpressions,
                        List<DataType> standaloneInputTypes,
                        RamAccountingContext ramAccountingContext,
                        int[] orderByIndexes,
                        Input[]... windowFuncArgsInputs) {
        assert windowDefinition.partitions().size() == 0 : "Window partitions are not supported.";
        assert windowDefinition.windowFrameDefinition().equals(WindowDefinition.DEFAULT_WINDOW_FRAME) : "Custom window frame definitions are not supported";
        assert windowDefinition.orderBy() == null || orderByIndexes.length > 0 : "Window is ordered but the IC indexes are not specified";

        this.order = windowDefinition.orderBy();

        // adding 32 extra bytes as some cells of each row will be part of a LinkedList which adds the overhead of
        // 24 bytes for each element (32 bytes each with compressed oops)
        int extraRowOverhead = 32;
        if (standaloneInputs.size() > 0) {
            // another LinkedList is used for standalone inputs
            extraRowOverhead += 32;
        }
        RowAccountingWithEstimators rowAccounting =
            new RowAccountingWithEstimators(standaloneInputTypes, ramAccountingContext, extraRowOverhead);
        this.source = new RamAccountingBatchIterator<>(source, rowAccounting);
        this.standaloneExpressions = standaloneExpressions;
        this.windowFunctionsCount = functions.size();
        this.outgoingCells = new Object[windowFunctionsCount + standaloneInputs.size()];
        this.standaloneOutgoingCells = new LinkedList<>();
        this.outstandingResults = new LinkedList<>();
        this.functions = functions;
        this.windowFuncArgsExpressions = windowFuncArgsExpressions;
        this.windowFuncArgsInputs = windowFuncArgsInputs;

        arePeerCellsPredicate = (prevRowCells, currentRowCells) -> {
            for (int i = 0; i < orderByIndexes.length; i++) {
                int samplingIndex = orderByIndexes[i];
                if (!Objects.equals(prevRowCells[samplingIndex], currentRowCells[samplingIndex])) {
                    return false;
                }
            }
            return true;
        };
    }

    private boolean arePeers(Object[] prevRowCells, Object[] currentRowCells) {
        if (order == null) {
            // all rows are peers when orderBy is missing
            return true;
        }

        return prevRowCells == currentRowCells || arePeerCellsPredicate.test(prevRowCells, currentRowCells);
    }

    @Override
    protected BatchIterator<Row> delegate() {
        return source;
    }

    @Override
    public Row currentElement() {
        return currentWindowRow;
    }

    @Override
    public void moveToStart() {
        super.moveToStart();
        sourceRowsConsumed = 0;
<<<<<<< HEAD
        windowRowPosition = 0;
=======
        emittedRows = 0;
        zeroIndexedRowNumber = 0;
>>>>>>> 1a64bd31c2... This fixes a few bugs on processing window functions over partitioned windows.
        windowForCurrentRow.clear();
        newRowsInCurrentFrameStartIdx = -1;
        currentRowCells = null;
        currentWindowRow = null;
    }

    @Override
    public boolean moveNext() {
        if (foundCurrentRowsLastPeer && windowRowPosition < sourceRowsConsumed - 1) {
            // emit the result of the window function as we computed the result and not yet emitted it for every row
            // in the window
            computeCurrentElement();
            return true;
        }

        while (source.moveNext()) {
            sourceRowsConsumed++;
            Row currentSourceRow = source.currentElement();
            Object[] sourceRowCells = currentSourceRow.materialize();
            computeAndFillStandaloneOutgoingCellsFor(currentSourceRow);
            if (sourceRowsConsumed == 1) {
                // first row in the source is the "current window row" we start with
                currentRowCells = sourceRowCells;
                newRowsInCurrentFrameStartIdx = 0;
            }

<<<<<<< HEAD
=======
            if (!Objects.equals(previousPartitionKey, currentPartitionKey)) {
                foundCurrentRowsLastPeer = false;
                executeWindowFunctions();
                currentRowCells = sourceRowCells;
                windowForCurrentRow.clear();
                // the "current row" that detected the partition change will be part of the new partition which will
                // be processed as a new window
                windowForCurrentRow.add(currentRowCells);
                newRowsInCurrentFrameStartIdx = 0;
                previousPartitionKey = currentPartitionKey;
                zeroIndexedRowNumber = 0;
                computeCurrentElement();
                return true;
            }

>>>>>>> 1a64bd31c2... This fixes a few bugs on processing window functions over partitioned windows.
            if (arePeers(currentRowCells, sourceRowCells)) {
                windowForCurrentRow.add(sourceRowCells);
                foundCurrentRowsLastPeer = false;
            } else {
                foundCurrentRowsLastPeer = true;

                executeWindowFunctions();
                // on the next source iteration, we'll start building the window for the next window row
                currentRowCells = sourceRowCells;
                windowForCurrentRow.add(currentRowCells);
                newRowsInCurrentFrameStartIdx = windowForCurrentRow.size() - 1;
                computeCurrentElement();
                return true;
            }
        }

        if (source.allLoaded()) {
            if (newRowsInCurrentFrameStartIdx != -1) {
                // we're done with consuming the source iterator, but were still in the process of building up the
                // window for the current window row. As there are no more rows to process, execute the function against
                // what we currently accumulated in the window and emit the result.
                executeWindowFunctions();
            }

            if (windowRowPosition < sourceRowsConsumed) {
                // we still need to emit rows
                computeCurrentElement();
                return true;
            }
        }
        return false;
    }

    private void computeCurrentElement() {
        if (outstandingResults.size() > 0) {
            Object[] windowFunctionsResult = outstandingResults.removeFirst();
            System.arraycopy(windowFunctionsResult, 0, outgoingCells, 0, windowFunctionsResult.length);
        }
        if (standaloneOutgoingCells.size() > 0) {
            Object[] inputRowCells = standaloneOutgoingCells.removeFirst();
            System.arraycopy(inputRowCells, 0, outgoingCells, windowFunctionsCount, inputRowCells.length);
        }
        currentWindowRow = new RowN(outgoingCells);
    }

    private void computeAndFillStandaloneOutgoingCellsFor(Row sourceRow) {
        if (standaloneExpressions.size() > 0) {
            Object[] standaloneInputValues = new Object[standaloneExpressions.size()];
            for (int i = 0; i < standaloneExpressions.size(); i++) {
                CollectExpression<Row, ?> expression = standaloneExpressions.get(i);
                expression.setNextRow(sourceRow);
                standaloneInputValues[i] = expression.value();
            }
            standaloneOutgoingCells.add(standaloneInputValues);
        }
    }

    private void executeWindowFunctions() {
        int newRowsInCurrentFrameCount = windowForCurrentRow.size() - newRowsInCurrentFrameStartIdx;
        WindowFrameState currentFrame = new WindowFrameState(
            // lower bound is always 0 as we currently only support the UNBOUNDED_PRECEDING -> CURRENT_ROW frame definition.
            0,
            windowForCurrentRow.size(),
            windowForCurrentRow
        );

        Object[][] newRowsInCurrentFrameCells = new Object[newRowsInCurrentFrameCount][windowFunctionsCount];

        int processedRowIndex = 0;
        for (int i = newRowsInCurrentFrameStartIdx; i < windowForCurrentRow.size(); i++) {
            for (int funcIdx = 0; funcIdx < functions.size(); funcIdx++) {
                WindowFunction function = functions.get(funcIdx);
<<<<<<< HEAD
                Object result = function.execute(windowRowPosition + i, currentFrame, windowFuncArgsExpressions, windowFuncArgsInputs[funcIdx]);
                newRowsInCurrentFrameCells[i][funcIdx] = result;
=======
                Object result = function.execute(
                    zeroIndexedRowNumber++, currentFrame, windowFuncArgsExpressions, windowFuncArgsInputs[funcIdx]);
                newRowsInCurrentFrameCells[processedRowIndex][funcIdx] = result;
>>>>>>> 1a64bd31c2... This fixes a few bugs on processing window functions over partitioned windows.
            }
            processedRowIndex++;
        }

        for (Object[] outgoingCells : newRowsInCurrentFrameCells) {
            outstandingResults.add(outgoingCells);
        }

        newRowsInCurrentFrameStartIdx = -1;
    }
}
