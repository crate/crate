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

import java.util.Collection;
import java.util.List;

final class LeafVisitor extends DefaultTraversalSymbolVisitor<List<Symbol>, Void> {

    private static final LeafVisitor INSTANCE = new LeafVisitor();

    private LeafVisitor() {
    }

    /**
     * Finds the leaves of {@link SplitPoints#toCollect()} and adds them to {@link SplitPoints#leaves()}
     */
    static void addLeafs(SplitPoints splitContext) {
        INSTANCE.process(splitContext.toCollect(), splitContext.leaves());
    }

    private void process(Collection<Symbol> symbols, List<Symbol> splitPointLeaves) {
        for (Symbol symbol : symbols) {
            process(symbol, splitPointLeaves);
        }
    }

    @Override
    protected Void visitSymbol(Symbol symbol, List<Symbol> splitPointLeaves) {
        allocateLeafSymbol(splitPointLeaves, symbol);
        return null;
    }

    @Override
    public Void visitAggregation(Aggregation symbol, List<Symbol> leafSymbols) {
        throw new AssertionError("Aggregation Symbols must not be visited with " +
                                 getClass().getCanonicalName());
    }

    private static void allocateLeafSymbol(List<Symbol> splitPointLeaves, Symbol symbol) {
        if (!splitPointLeaves.contains(symbol)) {
            splitPointLeaves.add(symbol);
        }
    }
}
