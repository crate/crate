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

package io.crate.execution.dsl.projection.builder;

import static io.crate.planner.operators.LogicalPlanner.extractColumns;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;

import io.crate.analyze.OrderBy;
import io.crate.analyze.QueriedSelectRelation;
import io.crate.expression.symbol.Aggregation;
import io.crate.expression.symbol.DefaultTraversalSymbolVisitor;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.OuterColumn;
import io.crate.expression.symbol.SelectSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.WindowFunction;
import io.crate.metadata.FunctionType;

public final class SplitPointsBuilder extends DefaultTraversalSymbolVisitor<SplitPointsBuilder.Context, Void> {

    private static final SplitPointsBuilder INSTANCE = new SplitPointsBuilder();

    static class Context {
        private final ArrayList<Function> aggregates = new ArrayList<>();
        private final ArrayList<Function> tableFunctions = new ArrayList<>();
        private final ArrayList<Symbol> standalone = new ArrayList<>();
        private final ArrayList<WindowFunction> windowFunctions = new ArrayList<>();
        private final ArrayList<SelectSymbol> correlatedQueries = new ArrayList<>();

        /**
         * OuterColumns found within a relation
         * For example in
         *
         * <pre>
         *  SELECT (SELECT t.mountain) FROM sys.summits t
         * </pre>
         *
         * This would contain `t.mountain` when processing the <b>inner</b> relation.
         */
        private final ArrayList<OuterColumn> outerColumns = new ArrayList<>();
        private boolean insideAggregate = false;

        private int tableFunctionLevel = 0;

        boolean foundAggregateOrTableFunction = false;

        Context() {
        }

        void allocateTableFunction(Function tableFunction) {
            if (tableFunctions.contains(tableFunction) == false) {
                tableFunctions.add(tableFunction);
            }
        }

        void allocateAggregate(Function aggregate) {
            if (aggregates.contains(aggregate) == false) {
                aggregates.add(aggregate);
            }
        }

        void allocateWindowFunction(WindowFunction windowFunction) {
            if (windowFunctions.contains(windowFunction) == false) {
                windowFunctions.add(windowFunction);
            }
        }
    }

    private void process(Collection<Symbol> symbols, Context context) {
        for (Symbol symbol : symbols) {
            process(symbol, context);
        }
    }

    private void process(Symbol symbol, Context context) {
        context.foundAggregateOrTableFunction = false;
        symbol.accept(this, context);
        if (context.foundAggregateOrTableFunction == false) {
            context.standalone.add(symbol);
        }
    }

    public static SplitPoints create(QueriedSelectRelation relation) {
        Context context = new Context();
        INSTANCE.process(relation.outputs(), context);
        OrderBy orderBy = relation.orderBy();
        if (orderBy != null) {
            INSTANCE.process(orderBy.orderBySymbols(), context);
        }
        Symbol having = relation.having();
        if (having != null) {
            having.accept(INSTANCE, context);
        }
        for (var joinPair : relation.joinPairs()) {
            Symbol condition = joinPair.condition();
            if (condition != null) {
                INSTANCE.process(condition, context);
            }
        }
        Symbol where = relation.where();
        where.accept(INSTANCE, context);
        LinkedHashSet<Symbol> toCollect = new LinkedHashSet<>();
        for (Function tableFunction : context.tableFunctions) {
            toCollect.addAll(extractColumns(tableFunction.arguments()));
        }
        for (Function aggregate : context.aggregates) {
            toCollect.addAll(aggregate.arguments());
            if (aggregate.filter() != null) {
                toCollect.add(aggregate.filter());
            }
        }
        for (WindowFunction windowFunction : context.windowFunctions) {
            toCollect.addAll(extractColumns(windowFunction.arguments()));
            if (windowFunction.filter() != null) {
                toCollect.add(windowFunction.filter());
            }
            INSTANCE.process(windowFunction.windowDefinition().partitions(), context);
            OrderBy windowOrderBy = windowFunction.windowDefinition().orderBy();
            if (windowOrderBy != null) {
                INSTANCE.process(windowOrderBy.orderBySymbols(), context);
            }
        }

        // group-by symbols must be processed on a dedicated context to be able extract table functions which must
        // be processed *below* a grouping operator
        var groupByContext = new Context();
        if (relation.groupBy().isEmpty() == false) {
            INSTANCE.process(relation.groupBy(), groupByContext);
            for (Function tableFunction : groupByContext.tableFunctions) {
                toCollect.addAll(extractColumns(tableFunction.arguments()));
            }
            toCollect.addAll(groupByContext.standalone);
            context.tableFunctions.removeAll(groupByContext.tableFunctions);
        } else if (context.aggregates.isEmpty() && relation.groupBy().isEmpty()) {
            toCollect.addAll(context.standalone);
        }
        var collectOuterColumns = new DefaultTraversalSymbolVisitor<Void, Void>() {

            public Void visitOuterColumn(OuterColumn outerColumn, Void ignored) {
                toCollect.add(outerColumn.symbol());
                return null;
            }
        };
        for (var selectSymbol : context.correlatedQueries) {
            selectSymbol.relation().visitSymbols(symbol -> symbol.accept(collectOuterColumns, null));
        }

        where.visit(Symbol.IS_COLUMN, toCollect::add);
        ArrayList<Symbol> outputs = new ArrayList<>();
        for (var output : toCollect) {
            if (output.any(Symbol.IS_CORRELATED_SUBQUERY)) {
                outputs.addAll(extractColumns(output));
            } else {
                outputs.add(output);
            }
        }
        return new SplitPoints(
            outputs,
            context.outerColumns,
            context.aggregates,
            context.tableFunctions,
            groupByContext.tableFunctions,
            context.windowFunctions
        );
    }

    @Override
    public Void visitFunction(Function function, Context context) {
        FunctionType type = function.signature().getKind();
        switch (type) {
            case SCALAR:
                super.visitFunction(function, context);
                return null;

            case AGGREGATE:
                context.foundAggregateOrTableFunction = true;
                context.allocateAggregate(function);
                context.insideAggregate = true;
                super.visitFunction(function, context);
                context.insideAggregate = false;
                return null;

            case TABLE:
                if (context.insideAggregate) {
                    throw new UnsupportedOperationException("Cannot use table functions inside aggregates");
                }
                context.foundAggregateOrTableFunction = true;
                if (context.tableFunctionLevel == 0) {
                    context.allocateTableFunction(function);
                }
                context.tableFunctionLevel++;
                super.visitFunction(function, context);
                context.tableFunctionLevel--;
                return null;
            default:
                throw new UnsupportedOperationException("Invalid function type: " + type);
        }
    }

    @Override
    public Void visitWindowFunction(WindowFunction windowFunction, Context context) {
        context.foundAggregateOrTableFunction = true;
        context.allocateWindowFunction(windowFunction);
        return super.visitFunction(windowFunction, context);
    }

    @Override
    public Void visitSelectSymbol(SelectSymbol selectSymbol, Context context) {
        if (selectSymbol.isCorrelated()) {
            context.correlatedQueries.add(selectSymbol);
            return null;
        }
        return super.visitSelectSymbol(selectSymbol, context);
    }

    @Override
    public Void visitOuterColumn(OuterColumn outerColumn, Context context) {
        context.outerColumns.add(outerColumn);
        return null;
    }

    @Override
    public Void visitAggregation(Aggregation symbol, Context context) {
        throw new AssertionError("Aggregation Symbols must not be visited with " +
                                 getClass().getCanonicalName());
    }

}
