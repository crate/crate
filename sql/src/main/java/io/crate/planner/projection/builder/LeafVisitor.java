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

import io.crate.analyze.symbol.Aggregation;
import io.crate.analyze.symbol.DefaultTraversalSymbolVisitor;
import io.crate.analyze.symbol.Symbol;

import java.util.ArrayList;
import java.util.Collection;

class LeafVisitor extends DefaultTraversalSymbolVisitor<
        LeafVisitor.Context, Void> {

    public static final LeafVisitor INSTANCE = new LeafVisitor();

    static class Context {
        final ArrayList<Symbol> leaves;

        Context(ArrayList<Symbol> leaves) {
            this.leaves = leaves;
        }

        void allocateLeafSymbol(Symbol symbol){
            if (!leaves.contains(symbol)) {
                leaves.add(symbol);
            }
        }
    }

    public void process(Collection<Symbol> symbols, Context context){
        for (Symbol symbol : symbols) {
            process(symbol, context);
        }
    }

    public void process(SplitPoints splitContext){
        Context context = new Context(splitContext.leaves());
        process(splitContext.toCollect(), context);
    }

    @Override
    protected Void visitSymbol(Symbol symbol, Context context) {
        context.allocateLeafSymbol(symbol);
        return null;
    }

    @Override
    public Void visitAggregation(Aggregation symbol, Context context) {
        throw new AssertionError("Aggregation Symbols must not be visited with " +
                getClass().getCanonicalName());
    }

}
