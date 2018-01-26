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

import java.util.ArrayList;
import java.util.Collection;

final class SplitPointVisitor extends DefaultTraversalSymbolVisitor<SplitPointVisitor.Context, Void> {

    private static final SplitPointVisitor INSTANCE = new SplitPointVisitor();

    static class Context {
        final ArrayList<Symbol> toCollect;
        final ArrayList<Function> aggregates;

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
            if (aggregates.contains(aggregate) == false) {
                aggregates.add(aggregate);
            }
        }
    }

    private void process(Collection<Symbol> symbols, Context context) {
        for (Symbol symbol : symbols) {
            process(symbol, context);
        }
    }

    static void addAggregatesAndToCollectSymbols(QueriedRelation relation, SplitPoints splitContext) {
        Context context = new Context(splitContext.toCollect(), splitContext.aggregates());
        INSTANCE.process(relation.outputs(), context);
        OrderBy orderBy = relation.orderBy();
        if (orderBy != null) {
            INSTANCE.process(orderBy.orderBySymbols(), context);
        }
        HavingClause having = relation.having();
        if (having != null && having.hasQuery()) {
            INSTANCE.process(having.query(), context);
        }
        for (Symbol groupKey : relation.groupBy()) {
            context.allocateCollectSymbol(groupKey);
        }
    }

    @Override
    public Void visitFunction(Function symbol, Context context) {
        if (symbol.info().type() == FunctionInfo.Type.AGGREGATE) {
            context.allocateAggregate(symbol);
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
