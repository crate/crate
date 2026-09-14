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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;

import io.crate.analyze.AnalyzedStatement;
import io.crate.analyze.QueriedSelectRelation;
import io.crate.expression.symbol.SelectSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.planner.operators.CorrelatedJoin;
import io.crate.planner.operators.LogicalPlan;

public class SubqueryPlanner {

    private final Function<SelectSymbol, LogicalPlan> planSubSelects;

    public SubqueryPlanner(Function<SelectSymbol, LogicalPlan> planSubSelects) {
        this.planSubSelects = planSubSelects;
    }

    public record Correlated(Map<SelectSymbol, LogicalPlan> toApplyBeforeGrouping, Map<SelectSymbol, LogicalPlan> toApplyAfterGrouping) {}

    public record SubQueries(Map<LogicalPlan, SelectSymbol> uncorrelated, Correlated correlated) {

        public LogicalPlan applyPreGroupingSubQueries(LogicalPlan source) {
            return applyCorrelatedJoin(source, correlated.toApplyBeforeGrouping);
        }

        public LogicalPlan applyPostGroupingSubQueries(LogicalPlan source) {
            return applyCorrelatedJoin(source, correlated.toApplyAfterGrouping);
        }

        private LogicalPlan applyCorrelatedJoin(LogicalPlan source, Map<SelectSymbol, LogicalPlan> correlatedSubqueries) {
            for (Map.Entry<SelectSymbol, LogicalPlan> entry : correlatedSubqueries.entrySet()) {
                SelectSymbol selectSymbol = entry.getKey();
                LogicalPlan plan = entry.getValue();
                source = new CorrelatedJoin(source, selectSymbol, plan);
            }
            return source;
        }
    }

    public SubQueries planSubQueries(AnalyzedStatement statement) {
        SubQueries subQueries = new SubQueries(new LinkedHashMap<>(), new Correlated(new LinkedHashMap<>(), new LinkedHashMap<>()));
        statement.visitSymbols(tree ->
            tree.visit(SelectSymbol.class, selectSymbol -> planSubquery(selectSymbol, subQueries))
        );
        return subQueries;
    }

    public SubQueries planSubQueries(QueriedSelectRelation statement) {
        SubQueries subQueries = new SubQueries(new LinkedHashMap<>(), new Correlated(new LinkedHashMap<>(), new LinkedHashMap<>()));
        statement.visitSymbols(tree ->
            tree.visit(SelectSymbol.class, selectSymbol -> planSubquery(selectSymbol, subQueries))
        );
        if (!statement.groupBy().isEmpty()) {
            for (Symbol output: statement.outputs()) {
                output.visit(SelectSymbol.class, selectSymbol -> {
                    if (selectSymbol.isCorrelated() && subQueries.correlated.toApplyBeforeGrouping.containsKey(selectSymbol)) {
                        subQueries.correlated.toApplyAfterGrouping.put(selectSymbol, subQueries.correlated.toApplyBeforeGrouping.get(selectSymbol));
                        subQueries.correlated.toApplyBeforeGrouping.remove(selectSymbol);
                    }
                });
            }
        }
        return subQueries;
    }

    public SubQueries planSubQueries(Symbol symbol) {
        SubQueries subQueries = new SubQueries(new LinkedHashMap<>(), new Correlated(new LinkedHashMap<>(), new LinkedHashMap<>()));
        symbol.visit(SelectSymbol.class, selectSymbol -> planSubquery(selectSymbol, subQueries));
        return subQueries;
    }

    private void planSubquery(SelectSymbol selectSymbol, SubQueries subQueries) {
        if (selectSymbol.isCorrelated()) {
            if (!subQueries.correlated.toApplyBeforeGrouping.containsKey(selectSymbol)) {
                LogicalPlan subPlan = planSubSelects.apply(selectSymbol);
                subQueries.correlated.toApplyBeforeGrouping.put(selectSymbol, subPlan);
            }
        } else {
            LogicalPlan subPlan = planSubSelects.apply(selectSymbol);
            subQueries.uncorrelated.put(subPlan, selectSymbol);
        }
    }
}
