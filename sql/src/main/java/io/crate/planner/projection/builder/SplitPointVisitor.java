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

package io.crate.planner.projection.builder;

import io.crate.analyze.HavingClause;
import io.crate.analyze.OrderBy;
import io.crate.analyze.QuerySpec;
import io.crate.analyze.symbol.Aggregation;
import io.crate.analyze.symbol.DefaultTraversalSymbolVisitor;
import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.Symbol;
import io.crate.metadata.FunctionInfo;

import java.util.ArrayList;
import java.util.Collection;

final class SplitPointVisitor extends DefaultTraversalSymbolVisitor<SplitPointVisitor.Context, Void> {

    private static final SplitPointVisitor INSTANCE = new SplitPointVisitor();

    static class Context {
        final ArrayList<Symbol> toCollect;
        final ArrayList<Function> aggregates;
        boolean aggregateSeen;
        boolean collectingOutputs = true;

        Context(ArrayList<Symbol> toCollect, ArrayList<Function> aggregates) {
            this.toCollect = toCollect;
            this.aggregates = aggregates;
        }

        void allocateCollectSymbol(Symbol symbol) {
            if (!toCollect.contains(symbol)) {
                toCollect.add(symbol);
            }
        }

        void allocateAggregate(Function aggregate) {
            // while processing outputs aggregates must be added always, otherwise outputs and aggregates differs
            if (collectingOutputs || aggregates.contains(aggregate) == false) {
                aggregates.add(aggregate);
            }
        }
    }

    private void process(Collection<Symbol> symbols, Context context) {
        for (Symbol symbol : symbols) {
            context.aggregateSeen = false;
            process(symbol, context);
            if (!context.aggregateSeen) {
                // add directly since it must be an entry without aggregate
                context.allocateCollectSymbol(symbol);
            }
        }
    }

    static void addAggregatesAndToCollectSymbols(QuerySpec querySpec, SplitPoints splitContext) {
        Context context = new Context(splitContext.toCollect(), splitContext.aggregates());
        INSTANCE.process(querySpec.outputs(), context);
        context.collectingOutputs = false;

        OrderBy orderBy = querySpec.orderBy();
        if (orderBy != null) {
            INSTANCE.process(orderBy.orderBySymbols(), context);
        }
        HavingClause having = querySpec.having();
        if (having != null && having.hasQuery()) {
            INSTANCE.process(having.query(), context);
        }
        if (!querySpec.groupBy().isEmpty()) {
            INSTANCE.process(querySpec.groupBy(), context);
        }
    }

    @Override
    public Void visitFunction(Function symbol, Context context) {
        if (symbol.info().type() == FunctionInfo.Type.AGGREGATE) {
            context.allocateAggregate(symbol);
            context.aggregateSeen = true;
            for (Symbol arg : symbol.arguments()) {
                context.allocateCollectSymbol(arg);
            }
            return null;
        }
        return super.visitFunction(symbol, context);
    }

    @Override
    public Void visitAggregation(Aggregation symbol, Context context) {
        throw new AssertionError("Aggregation Symbols must not be visited with " +
                                 getClass().getCanonicalName());
    }

}
