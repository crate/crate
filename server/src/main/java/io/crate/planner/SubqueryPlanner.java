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

package io.crate.planner;

import io.crate.analyze.AnalyzedStatement;
import io.crate.expression.symbol.DefaultTraversalSymbolVisitor;
import io.crate.expression.symbol.SelectSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.planner.operators.LogicalPlan;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

public class SubqueryPlanner {

    private final Function<SelectSymbol, LogicalPlan> planSubSelects;

    public SubqueryPlanner(Function<SelectSymbol, LogicalPlan> planSubSelects) {
        this.planSubSelects = planSubSelects;
    }

    public Map<LogicalPlan, SelectSymbol> planSubQueries(AnalyzedStatement statement) {
        Visitor visitor = new Visitor();
        statement.visitSymbols(visitor);
        return visitor.subQueries;
    }

    private void planSubquery(SelectSymbol selectSymbol, Map<LogicalPlan, SelectSymbol> subQueries) {
        LogicalPlan subPlan = planSubSelects.apply(selectSymbol);
        subQueries.put(subPlan, selectSymbol);
    }

    private class Visitor extends DefaultTraversalSymbolVisitor<Symbol, Void> implements Consumer<Symbol> {

        private final Map<LogicalPlan, SelectSymbol> subQueries = new HashMap<>();

        @Override
        public Void visitSelectSymbol(SelectSymbol selectSymbol, Symbol parent) {
            planSubquery(selectSymbol, subQueries);
            return null;
        }

        @Override
        public void accept(Symbol symbol) {
            symbol.accept(this, null);
        }
    }
}
