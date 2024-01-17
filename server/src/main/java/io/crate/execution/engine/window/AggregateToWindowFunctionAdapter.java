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

import static io.crate.execution.engine.window.WindowFrameState.isLowerBoundIncreasing;

import java.util.List;

import org.elasticsearch.Version;
import org.jetbrains.annotations.Nullable;

import io.crate.data.ArrayRow;
import io.crate.data.Input;
import io.crate.data.Row;
import io.crate.data.breaker.RamAccounting;
import io.crate.execution.engine.aggregation.AggregationFunction;
import io.crate.execution.engine.collect.CollectExpression;
import io.crate.expression.ExpressionsInput;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Symbol;
import io.crate.memory.MemoryManager;
import io.crate.metadata.NodeContext;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;

public class AggregateToWindowFunctionAdapter implements WindowFunction {

    private final AggregationFunction aggregationFunction;
    private final ExpressionsInput<Row, Boolean> filter;
    private final RamAccounting ramAccounting;
    private final Version indexVersionCreated;
    private final MemoryManager memoryManager;
    private final Version minNodeVersion;
    private Object accumulatedState;

    private int seenFrameLowerBound = -1;
    private int seenFrameUpperBound = -1;
    private Object resultForCurrentFrame;

    AggregateToWindowFunctionAdapter(AggregationFunction aggregationFunction,
                                     ExpressionsInput<Row, Boolean> filter,
                                     Version indexVersionCreated,
                                     RamAccounting ramAccounting,
                                     MemoryManager memoryManager,
                                     Version minNodeVersion) {
        this.aggregationFunction = aggregationFunction.optimizeForExecutionAsWindowFunction();
        this.filter = filter;
        this.ramAccounting = ramAccounting;
        this.indexVersionCreated = indexVersionCreated;
        this.memoryManager = memoryManager;
        this.minNodeVersion = minNodeVersion;
        this.accumulatedState = this.aggregationFunction.newState(
            this.ramAccounting,
            indexVersionCreated,
            minNodeVersion,
            memoryManager
        );
    }

    @Override
    public Signature signature() {
        return aggregationFunction.signature();
    }

    @Override
    public BoundSignature boundSignature() {
        return aggregationFunction.boundSignature();
    }

    @Override
    public Symbol normalizeSymbol(Function function, @Nullable TransactionContext txnCtx, NodeContext nodeCtx) {
        return aggregationFunction.normalizeSymbol(function, txnCtx, nodeCtx);
    }

    @Override
    public Object execute(int idxInPartition,
                          WindowFrameState frame,
                          List<? extends CollectExpression<Row, ?>> expressions,
                          @Nullable Boolean ignoreNulls,
                          Input<?> ... args) {
        assert ignoreNulls == null;
        if (idxInPartition == 0) {
            recomputeFunction(frame, expressions, args);
        } else if (isLowerBoundIncreasing(frame, seenFrameLowerBound)) {
            if (aggregationFunction.isRemovableCumulative()) {
                removeSeenRowsFromAccumulatedState(frame, expressions, args);
                resultForCurrentFrame = aggregationFunction.terminatePartial(ramAccounting, accumulatedState);
                if (frame.upperBoundExclusive() > seenFrameUpperBound) {
                    // if the frame is growing on the upper side, we need to accumulate the new rows
                    accumulateAndExecuteStartingWithIndex(seenFrameUpperBound, frame, expressions, args);
                }
                seenFrameLowerBound = frame.lowerBound();
                seenFrameUpperBound = frame.upperBoundExclusive();
            } else {
                recomputeFunction(frame, expressions, args);
            }
        } else if (frame.upperBoundExclusive() > seenFrameUpperBound) {
            executeAggregateForFrame(frame, expressions, args);
        }
        return resultForCurrentFrame;
    }

    private void removeSeenRowsFromAccumulatedState(WindowFrameState frame,
                                                    List<? extends CollectExpression<Row, ?>> expressions,
                                                    Input<?> ... args) {
        var row = new ArrayRow();
        for (int i = seenFrameLowerBound; i < frame.lowerBound(); i++) {
            Object[] cells = frame.getRowInPartitionAtIndexOrNull(i);
            assert cells != null : "No row at idx=" + i + " in current partition=" + frame;
            row.cells(cells);
            for (int j = 0, expressionsSize = expressions.size(); j < expressionsSize; j++) {
                expressions.get(j).setNextRow(row);
            }
            if (filter.value(row)) {
                //noinspection unchecked
                accumulatedState = aggregationFunction.removeFromAggregatedState(
                    ramAccounting,
                    accumulatedState,
                    args);
            }
        }
    }

    private void recomputeFunction(WindowFrameState frame,
                                   List<? extends CollectExpression<Row, ?>> expressions,
                                   Input<?> ... args) {
        accumulatedState = aggregationFunction.newState(
            ramAccounting,
            indexVersionCreated,
            minNodeVersion,
            memoryManager
        );
        seenFrameUpperBound = -1;
        seenFrameLowerBound = -1;
        executeAggregateForFrame(frame, expressions, args);
    }

    private void executeAggregateForFrame(WindowFrameState frame,
                                          List<? extends CollectExpression<Row, ?>> expressions,
                                          Input<?> ... inputs) {
        /*
         * If the window is not shrinking (eg. UNBOUNDED PRECEDING - CURRENT_ROW) the successive frames will have
         * overlapping rows. In this case we want to accumulate the rows we haven't processed yet (the difference
         * between the rows in the current frame to the rows in the previous frame, if any).
         */
        int unseenRowsInCurrentFrameStart =
            !isLowerBoundIncreasing(frame, seenFrameLowerBound) && seenFrameUpperBound > 0
                ? seenFrameUpperBound
                : frame.lowerBound();
        accumulateAndExecuteStartingWithIndex(unseenRowsInCurrentFrameStart, frame, expressions, inputs);
    }

    private void accumulateAndExecuteStartingWithIndex(int indexInFrame,
                                                       WindowFrameState frame,
                                                       List<? extends CollectExpression<Row, ?>> expressions,
                                                       Input<?> ... inputs) {
        var row = new ArrayRow();
        for (int i = indexInFrame; i < frame.upperBoundExclusive(); i++) {
            Object[] cells = frame.getRowInFrameAtIndexOrNull(i);
            assert cells != null : "No row at idx=" + i + " in current frame=" + frame;
            row.cells(cells);
            for (int j = 0, expressionsSize = expressions.size(); j < expressionsSize; j++) {
                expressions.get(j).setNextRow(row);
            }
            if (filter.value(row)) {
                //noinspection unchecked
                accumulatedState = aggregationFunction.iterate(
                    ramAccounting,
                    memoryManager,
                    accumulatedState,
                    inputs
                );
            }
        }

        resultForCurrentFrame = aggregationFunction.terminatePartial(ramAccounting, accumulatedState);
        seenFrameLowerBound = frame.lowerBound();
        seenFrameUpperBound = frame.upperBoundExclusive();
    }
}
