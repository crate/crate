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
import io.crate.metadata.FunctionInfo;
import io.crate.planner.symbol.Aggregation;
import io.crate.planner.symbol.DefaultTraversalSymbolVisitor;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.Symbol;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

class SplitPointVisitor extends DefaultTraversalSymbolVisitor<
        SplitPointVisitor.Context, Void> {

    public static final SplitPointVisitor INSTANCE = new SplitPointVisitor();

    static class Context {
        final ArrayList<Symbol> toCollect;
        final ArrayList<Function> aggregates;
        boolean aggregateSeen;

        Context(ArrayList<Symbol> toCollect, ArrayList<Function> aggregates) {
            this.toCollect = toCollect;
            this.aggregates = aggregates;
        }

        void allocateCollectSymbol(Symbol symbol){
            if (!toCollect.contains(symbol)) {
                toCollect.add(symbol);
            }
        }

        void allocateAggregate(Function aggregate){
            if (!aggregates.contains(aggregate)) {
                aggregates.add(aggregate);
            }
        }
    }

    public void process(Collection<Symbol> symbols, Context context){
        for (Symbol symbol : symbols) {
            context.aggregateSeen = false;
            process(symbol, context);
            if (!context.aggregateSeen){
                // add directly since it must be an entry without aggregate
                context.allocateCollectSymbol(symbol);
            }
        }
    }

    public void process(SplitPoints splitContext){
        Context context = new Context(splitContext.toCollect(), splitContext.aggregates());
        process(splitContext.querySpec().outputs(), context);
        OrderBy orderBy = splitContext.querySpec().orderBy();
        if (orderBy != null){
            process(orderBy.orderBySymbols(), context);
        }
        HavingClause havingClause = splitContext.querySpec().having();
        if (havingClause != null && havingClause.query() != null){
            process(havingClause.query(), context);
        }
        List<Symbol> groupBy = splitContext.querySpec().groupBy();
        if (groupBy != null){
            process(groupBy, context);
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
