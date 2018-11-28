/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

import io.crate.analyze.HavingClause;
import io.crate.analyze.OrderBy;
import io.crate.analyze.relations.QueriedRelation;
import io.crate.expression.symbol.Aggregation;
import io.crate.expression.symbol.DefaultTraversalSymbolVisitor;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.WindowFunction;
import io.crate.metadata.FunctionInfo;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;

import static io.crate.planner.operators.LogicalPlanner.extractColumns;

public final class SplitPointsBuilder extends DefaultTraversalSymbolVisitor<SplitPointsBuilder.Context, Void> {

    private static final SplitPointsBuilder INSTANCE = new SplitPointsBuilder();

    static class Context {
        private final ArrayList<Function> aggregates = new ArrayList<>();
        private final ArrayList<Function> tableFunctions = new ArrayList<>();
        private final ArrayList<Symbol> standalone = new ArrayList<>();
        private final ArrayList<WindowFunction> windowFunctions = new ArrayList<>();
        private boolean insideAggregate = false;

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
            context.foundAggregateOrTableFunction = false;
            process(symbol, context);
            if (context.foundAggregateOrTableFunction == false) {
                context.standalone.add(symbol);
            }
        }
    }

    public static SplitPoints create(QueriedRelation relation) {
        Context context = new Context();
        INSTANCE.process(relation.outputs(), context);
        OrderBy orderBy = relation.orderBy();
        if (orderBy != null) {
            INSTANCE.process(orderBy.orderBySymbols(), context);
        }
        HavingClause having = relation.having();
        if (having != null && having.hasQuery()) {
            INSTANCE.process(having.query(), context);
        }
        LinkedHashSet<Symbol> toCollect = new LinkedHashSet<>();
        for (Function tableFunction : context.tableFunctions) {
            toCollect.addAll(extractColumns(tableFunction.arguments()));
        }
        for (Function aggregate : context.aggregates) {
            toCollect.addAll(aggregate.arguments());
        }
        for (WindowFunction windowFunction : context.windowFunctions) {
            toCollect.addAll(windowFunction.arguments());
        }

        toCollect.addAll(relation.groupBy());
        if (context.aggregates.isEmpty() && relation.groupBy().isEmpty()) {
            toCollect.addAll(context.standalone);
        }
        return new SplitPoints(new ArrayList<>(toCollect), context.aggregates, context.tableFunctions, context.windowFunctions);
    }

    @Override
    public Void visitFunction(Function function, Context context) {
        FunctionInfo.Type type = function.info().type();
        switch (type) {
            case SCALAR:
                return super.visitFunction(function, context);

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
                context.allocateTableFunction(function);
                return super.visitFunction(function, context);

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
    public Void visitAggregation(Aggregation symbol, Context context) {
        throw new AssertionError("Aggregation Symbols must not be visited with " +
                                 getClass().getCanonicalName());
    }

}
