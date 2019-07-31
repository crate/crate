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

import io.crate.analyze.FrameBoundDefinition;
import io.crate.analyze.OrderBy;
import io.crate.analyze.WindowFrameDefinition;
import io.crate.breaker.RamAccountingContext;
import io.crate.breaker.RowAccounting;
import io.crate.breaker.RowAccountingWithEstimators;
import io.crate.common.collections.Lists2;
import io.crate.data.BatchIterator;
import io.crate.data.Input;
import io.crate.data.Projector;
import io.crate.data.Row;
import io.crate.data.RowN;
import io.crate.execution.dsl.projection.WindowAggProjection;
import io.crate.execution.engine.aggregation.AggregationFunction;
import io.crate.execution.engine.collect.CollectExpression;
import io.crate.execution.engine.sort.NullAwareComparator;
import io.crate.expression.ExpressionsInput;
import io.crate.expression.InputFactory;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.Symbols;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.Functions;
import io.crate.metadata.TransactionContext;
import io.crate.sql.tree.FrameBound;
import io.crate.sql.tree.WindowFrame;
import io.crate.types.DataType;
import org.elasticsearch.Version;
import org.elasticsearch.common.util.BigArrays;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.IntSupplier;
import java.util.function.Supplier;

import static io.crate.execution.engine.sort.Comparators.createComparator;

public class WindowProjector implements Projector {

    private final ComputeFrameBoundary<Object[]> computeFrameStart;
    private final ComputeFrameBoundary<Object[]> computeFrameEnd;
    private final Comparator<Object[]> cmpPartitionBy;
    private final Comparator<Object[]> cmpOrderBy;
    private final int cellOffset;
    private final ArrayList<WindowFunction> windowFunctions;
    private final List<CollectExpression<Row, ?>> argsExpressions;
    private final Input[][] args;
    private final IntSupplier numThreads;
    private final Executor executor;
    private final RowAccounting<Row> rowAccounting;

    public static WindowProjector fromProjection(WindowAggProjection projection,
                                                 Functions functions,
                                                 InputFactory inputFactory,
                                                 TransactionContext txnCtx,
                                                 RamAccountingContext ramAccountingContext,
                                                 BigArrays bigArrays,
                                                 Version indexVersionCreated,
                                                 IntSupplier numThreads,
                                                 Executor executor) {
        LinkedHashMap<io.crate.expression.symbol.WindowFunction, List<Symbol>> functionsWithInputs = projection.functionsWithInputs();
        ArrayList<WindowFunction> windowFunctions = new ArrayList<>(functionsWithInputs.size());
        List<CollectExpression<Row, ?>> windowFuncArgsExpressions = new ArrayList<>(functionsWithInputs.size());
        Input[][] windowFuncArgsInputs = new Input[functionsWithInputs.size()][];
        int inputsIndex = 0;
        for (Map.Entry<io.crate.expression.symbol.WindowFunction, List<Symbol>> functionAndInputsEntry : functionsWithInputs.entrySet()) {
            InputFactory.Context<CollectExpression<Row, ?>> ctx = inputFactory.ctxForInputColumns(txnCtx);
            ctx.add(functionAndInputsEntry.getValue());
            windowFuncArgsInputs[inputsIndex] = ctx.topLevelInputs().toArray(new Input[0]);
            inputsIndex++;
            windowFuncArgsExpressions.addAll(ctx.expressions());

            FunctionImplementation impl = functions.getQualified(functionAndInputsEntry.getKey().info().ident());
            if (impl instanceof AggregationFunction) {
                windowFunctions.add(
                    new AggregateToWindowFunctionAdapter(
                        (AggregationFunction) impl,
                        indexVersionCreated,
                        bigArrays,
                        ramAccountingContext)
                );
            } else if (impl instanceof WindowFunction) {
                windowFunctions.add((WindowFunction) impl);
            } else {
                throw new AssertionError("Function needs to be either a window or an aggregate function");
            }
        }
        var windowDefinition = projection.windowDefinition();
        var partitions = windowDefinition.partitions();
        Supplier<InputFactory.Context<CollectExpression<Row, ?>>> createInputFactoryContext =
            () -> inputFactory.ctxForInputColumns(txnCtx);
        int arrayListElementOverHead = 32;
        RowAccountingWithEstimators accounting = new RowAccountingWithEstimators(
            Symbols.typeView(projection.standalone()), ramAccountingContext, arrayListElementOverHead);

        OrderBy orderBy = windowDefinition.orderBy();
        Comparator<Object[]> cmpRow = createComparator(createInputFactoryContext, orderBy);
        WindowFrameDefinition windowFrameDefinition = windowDefinition.windowFrameDefinition();
        // TODO: de-duplicate logic in createComputeFrameStart and createComputeFrameEnd
        ComputeFrameBoundary<Object[]> computeFrameStart = createComputeFrameStart(
            inputFactory,
            txnCtx,
            windowFrameDefinition.mode(),
            windowFrameDefinition.start(),
            cmpRow,
            orderBy
        );
        ComputeFrameBoundary<Object[]> computeFrameEnd = createComputeFrameEnd(
            inputFactory,
            txnCtx,
            windowFrameDefinition.mode(),
            windowFrameDefinition.end(),
            cmpRow,
            orderBy
        );
        return new WindowProjector(
            accounting,
            computeFrameStart,
            computeFrameEnd,
            partitions.isEmpty() ? null : createComparator(createInputFactoryContext, new OrderBy(windowDefinition.partitions())),
            cmpRow,
            projection.standalone().size(),
            windowFunctions,
            windowFuncArgsExpressions,
            windowFuncArgsInputs,
            numThreads,
            executor
        );
    }

    public static ComputeFrameBoundary<Object[]> createComputeFrameEnd(InputFactory inputFactory,
                                                                        TransactionContext txnCtx,
                                                                        WindowFrame.Mode mode,
                                                                        FrameBoundDefinition end,
                                                                        Comparator<Object[]> cmpRow,
                                                                        OrderBy orderBy) {
        BinaryOperator<Object> plus;
        Function<Object[], Object> getOffset;
        Function<Object[], Object> getOrderingValue;
        Comparator<Object> cmpOrderingValue;
        if (end.type() == FrameBound.Type.FOLLOWING) {
            Symbol orderSymbol = Lists2.getOnlyElement(orderBy.orderBySymbols());
            DataType<Object> dataType = (DataType<Object>) orderSymbol.valueType();

            InputFactory.Context<CollectExpression<Row, ?>> ctxGetOffset = inputFactory.ctxForInputColumns(txnCtx);
            Input<?> offsetInput = ctxGetOffset.add(end.value());
            ExpressionsInput<Row, ?> offset = new ExpressionsInput<>(offsetInput, ctxGetOffset.expressions());
            // TODO: Avoid new RowN by using a shared Row; Maybe make a ExpressionsInput that works with Object[]
            getOffset = cells -> dataType.value(offset.value(new RowN(cells)));

            InputFactory.Context<CollectExpression<Row, ?>> ctxOrderingValue = inputFactory.ctxForInputColumns(txnCtx);
            Input<?> orderingInput = ctxOrderingValue.add(orderSymbol);
            ExpressionsInput<Row, ?> orderingValue = new ExpressionsInput<>(orderingInput, ctxOrderingValue.expressions());
            getOrderingValue = cells -> orderingValue.value(new RowN(cells));

            // TODO: need to handle reverseFlags/nullsFirst -> utilize NullAwareComparator
            plus = ArithmeticOperatorsFactory.getAddFunction(dataType);
            cmpOrderingValue = new NullAwareComparator<>(
                x -> (Comparable<Object>) x,
                orderBy.reverseFlags()[0],
                orderBy.nullsFirst()[0]
            );
        } else {
            plus = null;
            getOffset = row -> null;
            getOrderingValue = row -> null;
            cmpOrderingValue = (val1, val2) -> 0;
        }
        return (partitionStart, partitionEnd, currentIndex, sortedRows) -> end.type().getEnd(
            mode,
            partitionStart,
            partitionEnd,
            currentIndex,
            getOffset,
            getOrderingValue,
            plus,
            cmpOrderingValue,
            cmpRow,
            sortedRows
        );
    }

    public static ComputeFrameBoundary<Object[]> createComputeFrameStart(InputFactory inputFactory,
                                                                          TransactionContext txnCtx,
                                                                          WindowFrame.Mode mode,
                                                                          FrameBoundDefinition start,
                                                                          Comparator<Object[]> cmpRow,
                                                                          OrderBy orderBy) {
        BinaryOperator<Object> minus;
        Function<Object[], Object> getOffset;
        Function<Object[], Object> getOrderingValue;
        Comparator<Object> cmpOrderingValue;
        if (start.type() == FrameBound.Type.PRECEDING) {
            Symbol orderSymbol = Lists2.getOnlyElement(orderBy.orderBySymbols());
            DataType<Object> dataType = (DataType<Object>) orderSymbol.valueType();
            minus = ArithmeticOperatorsFactory.getSubtractFunction(dataType);
            InputFactory.Context<CollectExpression<Row, ?>> ctxGetOffset = inputFactory.ctxForInputColumns(txnCtx);
            Input<?> offsetInput = ctxGetOffset.add(start.value());
            ExpressionsInput<Row, ?> offset = new ExpressionsInput<>(offsetInput, ctxGetOffset.expressions());
            // TODO: Avoid new RowN by using a shared Row; Maybe make a ExpressionsInput that works with Object[]
            getOffset = cells -> dataType.value(offset.value(new RowN(cells)));

            InputFactory.Context<CollectExpression<Row, ?>> ctxOrderingValue = inputFactory.ctxForInputColumns(txnCtx);
            Input<?> orderingInput = ctxOrderingValue.add(orderSymbol);
            ExpressionsInput<Row, ?> orderingValue = new ExpressionsInput<>(orderingInput, ctxOrderingValue.expressions());
            getOrderingValue = cells -> orderingValue.value(new RowN(cells));
            cmpOrderingValue = new NullAwareComparator<>(
                x -> (Comparable<Object>) x,
                orderBy.reverseFlags()[0],
                orderBy.nullsFirst()[0]
            );
        } else {
            minus = null;
            getOffset = row -> null;
            getOrderingValue = row -> null;
            cmpOrderingValue = (val1, val2) -> 0;
        }
        return (partitionStart, partitionEnd, currentIndex, sortedRows) -> start.type().getStart(
            mode,
            partitionStart,
            partitionEnd,
            currentIndex,
            getOffset,
            getOrderingValue,
            minus,
            cmpOrderingValue,
            cmpRow,
            sortedRows
        );
    }

    private WindowProjector(RowAccounting<Row> rowAccounting,
                            ComputeFrameBoundary<Object[]> computeFrameStart,
                            ComputeFrameBoundary<Object[]> computeFrameEnd,
                            Comparator<Object[]> cmpPartitionBy,
                            Comparator<Object[]> cmpOrderBy,
                            int cellOffset,
                            ArrayList<WindowFunction> windowFunctions,
                            List<CollectExpression<Row, ?>> argsExpressions,
                            Input[][] args,
                            IntSupplier numThreads,
                            Executor executor) {
        this.rowAccounting = rowAccounting;
        this.computeFrameStart = computeFrameStart;
        this.computeFrameEnd = computeFrameEnd;
        this.cmpPartitionBy = cmpPartitionBy;
        this.cmpOrderBy = cmpOrderBy;
        this.cellOffset = cellOffset;
        this.windowFunctions = windowFunctions;
        this.argsExpressions = argsExpressions;
        this.args = args;
        this.numThreads = numThreads;
        this.executor = executor;
    }

    @Override
    public BatchIterator<Row> apply(BatchIterator<Row> source) {
        return WindowFunctionBatchIterator.of(
            source,
            rowAccounting,
            computeFrameStart,
            computeFrameEnd,
            cmpPartitionBy,
            cmpOrderBy,
            cellOffset,
            numThreads,
            executor,
            windowFunctions,
            argsExpressions,
            args
        );
    }
}
