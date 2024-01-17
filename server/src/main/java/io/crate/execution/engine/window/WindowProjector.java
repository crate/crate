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

import static io.crate.analyze.SymbolEvaluator.evaluateWithoutParams;
import static io.crate.execution.engine.sort.Comparators.createComparator;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;
import java.util.function.IntSupplier;
import java.util.function.Supplier;

import org.elasticsearch.Version;
import org.jetbrains.annotations.Nullable;

import io.crate.analyze.OrderBy;
import io.crate.analyze.WindowDefinition;
import io.crate.breaker.RowAccountingWithEstimators;
import io.crate.data.Input;
import io.crate.data.Projector;
import io.crate.data.Row;
import io.crate.data.breaker.RamAccounting;
import io.crate.execution.dsl.projection.WindowAggProjection;
import io.crate.execution.engine.aggregation.AggregationFunction;
import io.crate.execution.engine.collect.CollectExpression;
import io.crate.expression.ExpressionsInput;
import io.crate.expression.InputFactory;
import io.crate.expression.symbol.InputColumn;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.SymbolType;
import io.crate.expression.symbol.Symbols;
import io.crate.memory.MemoryManager;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.NodeContext;
import io.crate.metadata.TransactionContext;
import io.crate.sql.tree.WindowFrame;
import io.crate.types.DataType;
import io.crate.types.IntervalType;

public class WindowProjector {

    public static Projector fromProjection(WindowAggProjection projection,
                                           NodeContext nodeCtx,
                                           InputFactory inputFactory,
                                           TransactionContext txnCtx,
                                           RamAccounting ramAccounting,
                                           MemoryManager memoryManager,
                                           Version minNodeVersion,
                                           Version indexVersionCreated,
                                           IntSupplier numThreads,
                                           Executor executor) {
        var windowFunctionSymbols = projection.windowFunctions();
        var numWindowFunctions = windowFunctionSymbols.size();
        assert numWindowFunctions > 0 : "WindowAggProjection must have at least 1 window function.";

        ArrayList<WindowFunction> windowFunctions = new ArrayList<>(numWindowFunctions);
        ArrayList<CollectExpression<Row, ?>> windowFuncArgsExpressions = new ArrayList<>(numWindowFunctions);
        Input[][] windowFuncArgsInputs = new Input[numWindowFunctions][];
        Boolean[] ignoreNulls = new Boolean[numWindowFunctions];

        for (int idx = 0; idx < numWindowFunctions; idx++) {
            var windowFunctionSymbol = windowFunctionSymbols.get(idx);

            InputFactory.Context<CollectExpression<Row, ?>> ctx = inputFactory.ctxForInputColumns(txnCtx);
            ctx.add(windowFunctionSymbol.arguments());

            FunctionImplementation impl = nodeCtx.functions().getQualified(windowFunctionSymbol);
            assert impl != null : "Function implementation not found using full qualified lookup";
            if (impl instanceof AggregationFunction) {
                var filterInputFactoryCtx = inputFactory.ctxForInputColumns(txnCtx);
                var filterSymbol = windowFunctionSymbol.filter();

                //noinspection unchecked
                Input<Boolean> filterInput = filterSymbol == null
                    ? Literal.BOOLEAN_TRUE
                    : (Input<Boolean>) filterInputFactoryCtx.add(filterSymbol);

                ExpressionsInput<Row, Boolean> filter = new ExpressionsInput<>(
                    filterInput,
                    filterInputFactoryCtx.expressions());

                windowFunctions.add(
                    new AggregateToWindowFunctionAdapter(
                        (AggregationFunction) impl,
                        filter,
                        indexVersionCreated,
                        ramAccounting,
                        memoryManager,
                        minNodeVersion
                    )
                );
            } else if (impl instanceof WindowFunction) {
                windowFunctions.add((WindowFunction) impl);
            } else {
                throw new AssertionError("Function needs to be either a window or an aggregate function");
            }
            windowFuncArgsExpressions.addAll(ctx.expressions());
            windowFuncArgsInputs[idx] = ctx.topLevelInputs().toArray(new Input[0]);
            ignoreNulls[idx] = windowFunctionSymbol.ignoreNulls();
        }
        var windowDefinition = projection.windowDefinition();
        var partitions = windowDefinition.partitions();
        Supplier<InputFactory.Context<CollectExpression<Row, ?>>> createInputFactoryContext =
            () -> inputFactory.ctxForInputColumns(txnCtx);
        int arrayListElementOverHead = 32;
        List<DataType<?>> rowTypes = Symbols.typeView(projection.standalone());
        RowAccountingWithEstimators accounting = new RowAccountingWithEstimators(
            rowTypes, ramAccounting, arrayListElementOverHead);
        Comparator<Object[]> cmpPartitionBy = partitions.isEmpty()
            ? null
            : createComparator(createInputFactoryContext, rowTypes, new OrderBy(windowDefinition.partitions()));
        Comparator<Object[]> cmpOrderBy = createComparator(
            createInputFactoryContext,
            rowTypes,
            windowDefinition.orderBy()
        );
        int numCellsInSourceRow = projection.standalone().size();
        ComputeFrameBoundary<Object[]> computeFrameStart = createComputeStartFrameBoundary(
            numCellsInSourceRow,
            txnCtx,
            nodeCtx,
            windowDefinition,
            cmpOrderBy
        );
        ComputeFrameBoundary<Object[]> computeFrameEnd = createComputeEndFrameBoundary(
            numCellsInSourceRow,
            txnCtx,
            nodeCtx,
            windowDefinition,
            cmpOrderBy
        );
        return sourceRows -> WindowFunctionBatchIterator.of(
            sourceRows,
            accounting,
            computeFrameStart,
            computeFrameEnd,
            cmpPartitionBy,
            cmpOrderBy,
            numCellsInSourceRow,
            numThreads,
            executor,
            windowFunctions,
            windowFuncArgsExpressions,
            ignoreNulls,
            windowFuncArgsInputs
        );
    }

    static ComputeFrameBoundary<Object[]> createComputeEndFrameBoundary(int numCellsInSourceRow,
                                                                        TransactionContext txnCtx,
                                                                        NodeContext nodeCtx,
                                                                        WindowDefinition windowDefinition,
                                                                        Comparator<Object[]> cmpOrderBy) {
        var frameDefinition = windowDefinition.windowFrameDefinition();
        var frameBoundEnd = frameDefinition.end();
        var framingMode = frameDefinition.mode();
        DataType offsetType = frameBoundEnd.value().valueType();
        Object offsetValue = evaluateWithoutParams(txnCtx, nodeCtx, frameBoundEnd.value());
        Object[] endProbeValues = new Object[numCellsInSourceRow];
        BiFunction<Object[], Object[], Object[]> updateProbeValues;
        if (offsetValue != null && framingMode == WindowFrame.Mode.RANGE) {
            updateProbeValues = createUpdateProbeValueFunction(
                windowDefinition, ArithmeticOperatorsFactory::getAddFunction, offsetValue, offsetType);
        } else {
            updateProbeValues = (currentRow, x) -> x;
        }
        return (partitionStart, partitionEnd, currentIndex, sortedRows) -> frameBoundEnd.type().getEnd(
            framingMode,
            partitionStart,
            partitionEnd,
            currentIndex,
            offsetValue,
            updateProbeValues.apply(sortedRows.get(currentIndex), endProbeValues),
            cmpOrderBy,
            sortedRows
        );
    }

    static ComputeFrameBoundary<Object[]> createComputeStartFrameBoundary(int numCellsInSourceRow,
                                                                          TransactionContext txnCtx,
                                                                          NodeContext nodeCtx,
                                                                          WindowDefinition windowDefinition,
                                                                          @Nullable Comparator<Object[]> cmpOrderBy) {
        var frameDefinition = windowDefinition.windowFrameDefinition();
        var frameBoundStart = frameDefinition.start();
        var framingMode = frameDefinition.mode();
        DataType offsetType = frameBoundStart.value().valueType();
        Object offsetValue = evaluateWithoutParams(txnCtx, nodeCtx, frameBoundStart.value());
        Object[] startProbeValues = new Object[numCellsInSourceRow];
        BiFunction<Object[], Object[], Object[]> updateStartProbeValue;
        if (offsetValue != null && framingMode == WindowFrame.Mode.RANGE) {
            updateStartProbeValue = createUpdateProbeValueFunction(
                windowDefinition, ArithmeticOperatorsFactory::getSubtractFunction, offsetValue, offsetType);
        } else {
            updateStartProbeValue = (currentRow, x) -> x;
        }
        return (partitionStart, partitionEnd, currentIndex, sortedRows) -> frameBoundStart.type().getStart(
            framingMode,
            partitionStart,
            partitionEnd,
            currentIndex,
            offsetValue,
            updateStartProbeValue.apply(sortedRows.get(currentIndex), startProbeValues),
            cmpOrderBy,
            sortedRows
        );
    }

    private static BiFunction<Object[], Object[], Object[]> createUpdateProbeValueFunction(WindowDefinition windowDefinition,
                                                                                           BiFunction<DataType<?>, DataType<?>, BiFunction> getOffsetApplicationFunction,
                                                                                           Object offsetValue,
                                                                                           DataType<?> offsetType) {
        OrderBy windowOrdering = windowDefinition.orderBy();
        assert windowOrdering != null : "The window definition must be ordered if custom offsets are specified";
        List<Symbol> orderBySymbols = windowOrdering.orderBySymbols();
        if (orderBySymbols.size() != 1) {
            throw new IllegalArgumentException("Must have exactly 1 ORDER BY expression if using <offset> FOLLOWING/PRECEDING");
        }
        int offsetColumnPosition;
        Symbol orderSymbol = orderBySymbols.get(0);
        BiFunction applyOffsetOnOrderingValue = getOffsetApplicationFunction.apply(orderSymbol.valueType(), offsetType);
        if (orderSymbol.symbolType() == SymbolType.LITERAL) {
            offsetColumnPosition = -1;
        } else {
            assert orderSymbol instanceof InputColumn
                : "ORDER BY expression must resolve to an InputColumn, but got: " + orderSymbol;
            offsetColumnPosition = ((InputColumn) orderSymbol).index();
        }
        var finalOffsetValue =
            offsetType.id() == IntervalType.ID ? offsetType.sanitizeValue(offsetValue) : orderSymbol.valueType().sanitizeValue(offsetValue);
        return (currentRow, x) -> {
            // if the offsetCell position is -1 the window is ordered by a Literal so we leave the
            // probe value to null so it doesn't impact ordering (ie. all values will be consistently GT or LT
            // `null`)
            if (offsetColumnPosition != -1) {
                x[offsetColumnPosition] = applyOffsetOnOrderingValue.apply(currentRow[offsetColumnPosition], finalOffsetValue);
            }
            return x;
        };
    }
}
