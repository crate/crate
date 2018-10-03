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
import io.crate.metadata.FunctionInfo;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;

public final class SplitPointsBuilder extends DefaultTraversalSymbolVisitor<SplitPointsBuilder.Context, Void> {

    private static final SplitPointsBuilder INSTANCE = new SplitPointsBuilder();

    static class Context {
        private final LinkedHashSet<Symbol> toCollect = new LinkedHashSet<>();
        private final ArrayList<Function> aggregatesOrTableFunctions = new ArrayList<>();
        private final ArrayList<Symbol> standalone = new ArrayList<>();

        boolean foundFunction = false;

        @Nullable
        FunctionInfo.Type functionsType;

        Context() {
        }

        void allocateFunction(Function aggregate) {
            if (aggregatesOrTableFunctions.contains(aggregate) == false) {
                aggregatesOrTableFunctions.add(aggregate);
            }
        }
    }

    private void process(Collection<Symbol> symbols, Context context) {
        for (Symbol symbol : symbols) {
            context.foundFunction = false;
            process(symbol, context);
            if (context.foundFunction == false) {
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
        for (Function aggregatesOrTableFunction : context.aggregatesOrTableFunctions) {
            context.toCollect.addAll(aggregatesOrTableFunction.arguments());
        }
        context.toCollect.addAll(relation.groupBy());
        if (!FunctionInfo.Type.AGGREGATE.equals(context.functionsType) && relation.groupBy().isEmpty()) {
            context.toCollect.addAll(context.standalone);
        }
        return new SplitPoints(
            new ArrayList<>(context.toCollect),
            context.aggregatesOrTableFunctions,
            context.functionsType
        );
    }

    @Override
    public Void visitFunction(Function function, Context context) {
        FunctionInfo.Type type = function.info().type();
        switch (type) {
            case SCALAR:
                return super.visitFunction(function, context);

            case AGGREGATE:
            case TABLE:
                context.foundFunction = true;
                if (context.functionsType == null) {
                    context.functionsType = type;
                } else if (!context.functionsType.equals(type)) {
                    throw new UnsupportedOperationException("Cannot mix aggregates and table functions");
                }
                context.allocateFunction(function);
                return super.visitFunction(function, context);

            default:
                throw new UnsupportedOperationException("Invalid function type: " + type);
        }
    }

    @Override
    public Void visitAggregation(Aggregation symbol, Context context) {
        throw new AssertionError("Aggregation Symbols must not be visited with " +
                                 getClass().getCanonicalName());
    }

}
