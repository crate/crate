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
import io.crate.breaker.RamAccounting;
import io.crate.breaker.RowAccountingWithEstimators;
import io.crate.data.Input;
import io.crate.data.Projector;
import io.crate.data.Row;
import io.crate.execution.dsl.projection.WindowAggProjection;
import io.crate.execution.engine.aggregation.AggregationFunction;
import io.crate.execution.engine.collect.CollectExpression;
import io.crate.expression.ExpressionsInput;
import io.crate.expression.InputFactory;
import io.crate.expression.symbol.InputColumn;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.SymbolType;
import io.crate.expression.symbol.Symbols;
import io.crate.memory.MemoryManager;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.Functions;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.Signature;
import io.crate.sql.tree.WindowFrame;
import io.crate.types.DataType;
import io.crate.types.IntervalType;
import org.elasticsearch.Version;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;
import java.util.function.IntSupplier;
import java.util.function.Supplier;

import static io.crate.analyze.SymbolEvaluator.evaluateWithoutParams;
import static io.crate.execution.engine.sort.Comparators.createComparator;

public class WindowProjector {

    public static Projector fromProjection(WindowAggProjection projection,
                                           Functions functions,
                                           InputFactory inputFactory,
                                           TransactionContext txnCtx,
                                           RamAccounting ramAccounting,
                                           MemoryManager memoryManager,
                                           Version minNodeVersion,
                                           Version indexVersionCreated,
                                           IntSupplier numThreads,
                                           Executor executor) {
        var windowFunctionContexts = projection.windowFunctionContexts();
        var numWindowFunctions = windowFunctionContexts.size();

        ArrayList<WindowFunction> windowFunctions = new ArrayList<>(numWindowFunctions);
        ArrayList<CollectExpression<Row, ?>> windowFuncArgsExpressions = new ArrayList<>(numWindowFunctions);
        Input[][] windowFuncArgsInputs = new Input[numWindowFunctions][];

        for (int idx = 0; idx < numWindowFunctions; idx++) {
            var windowFunctionContext = windowFunctionContexts.get(idx);

            InputFactory.Context<CollectExpression<Row, ?>> ctx = inputFactory.ctxForInputColumns(txnCtx);
            ctx.add(windowFunctionContext.inputs());

            FunctionIdent ident = windowFunctionContext.function().info().ident();
            Signature signature = windowFunctionContext.function().signature();
            FunctionImplementation impl;
            if (signature == null) {
                impl = functions.getQualified(ident);
            } else {
                impl = functions.getQualified(signature, ident.argumentTypes());
            }
            assert impl != null : "Function implementation not found using full qualified lookup";
            if (impl instanceof AggregationFunction) {
                var filterInputFactoryCtx = inputFactory.ctxForInputColumns(txnCtx);
                //noinspection unchecked
                Input<Boolean> filterInput =
                    (Input<Boolean>) filterInputFactoryCtx.add(windowFunctionContext.filter());

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
        }
        var windowDefinition = projection.windowDefinition();
        var partitions = windowDefinition.partitions();
        Supplier<InputFactory.Context<CollectExpression<Row, ?>>> createInputFactoryContext =
            () -> inputFactory.ctxForInputColumns(txnCtx);
        int arrayListElementOverHead = 32;
        RowAccountingWithEstimators accounting = new RowAccountingWithEstimators(
            Symbols.typeView(projection.standalone()), ramAccounting, arrayListElementOverHead);
        Comparator<Object[]> cmpPartitionBy = partitions.isEmpty()
            ? null
            : createComparator(createInputFactoryContext, new OrderBy(windowDefinition.partitions()));
        Comparator<Object[]> cmpOrderBy = createComparator(createInputFactoryContext, windowDefinition.orderBy());
        int numCellsInSourceRow = projection.standalone().size();
        ComputeFrameBoundary<Object[]> computeFrameStart = createComputeStartFrameBoundary(
            numCellsInSourceRow,
            functions,
            txnCtx,
            windowDefinition,
            cmpOrderBy
        );
        ComputeFrameBoundary<Object[]> computeFrameEnd = createComputeEndFrameBoundary(
            numCellsInSourceRow,
            functions,
            txnCtx,
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
            windowFuncArgsInputs
        );
    }

    static ComputeFrameBoundary<Object[]> createComputeEndFrameBoundary(int numCellsInSourceRow,
                                                                        Functions functions,
                                                                        TransactionContext txnCtx,
                                                                        WindowDefinition windowDefinition,
                                                                        Comparator<Object[]> cmpOrderBy) {
        var frameDefinition = windowDefinition.windowFrameDefinition();
        var frameBoundEnd = frameDefinition.end();
        var framingMode = frameDefinition.mode();
        DataType offsetType = frameBoundEnd.value().valueType();
        Object offsetValue = evaluateWithoutParams(txnCtx, functions, frameBoundEnd.value());
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
                                                                          Functions functions,
                                                                          TransactionContext txnCtx,
                                                                          WindowDefinition windowDefinition,
                                                                          @Nullable Comparator<Object[]> cmpOrderBy) {
        var frameDefinition = windowDefinition.windowFrameDefinition();
        var frameBoundStart = frameDefinition.start();
        var framingMode = frameDefinition.mode();
        DataType offsetType = frameBoundStart.value().valueType();
        Object offsetValue = evaluateWithoutParams(txnCtx, functions, frameBoundStart.value());
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
                                                                                           DataType offsetType) {
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
            offsetType.id() == IntervalType.ID ? offsetType.value(offsetValue) : orderSymbol.valueType().value(offsetValue);
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
