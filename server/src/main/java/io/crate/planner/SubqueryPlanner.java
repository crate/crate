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
import io.crate.expression.symbol.SelectSymbol;
import io.crate.planner.operators.CorrelatedJoin;
import io.crate.planner.operators.LogicalPlan;

public class SubqueryPlanner {

    private final Function<SelectSymbol, LogicalPlan> planSubSelects;

    public SubqueryPlanner(Function<SelectSymbol, LogicalPlan> planSubSelects) {
        this.planSubSelects = planSubSelects;
    }

    public record SubQueries(Map<LogicalPlan, SelectSymbol> uncorrelated, Map<SelectSymbol, LogicalPlan> correlated) {

        public LogicalPlan applyCorrelatedJoin(LogicalPlan source) {
            for (var entry : correlated.entrySet()) {
                LogicalPlan plannedSubQuery = entry.getValue();
                SelectSymbol subQuery = entry.getKey();
                source = new CorrelatedJoin(
                    source,
                    subQuery,
                    plannedSubQuery
                );
            }
            return source;
        }
    }

    public SubQueries planSubQueries(AnalyzedStatement statement) {
        SubQueries subQueries = new SubQueries(new LinkedHashMap<>(), new LinkedHashMap<>());
        statement.visitSymbols(tree ->
            tree.visit(SelectSymbol.class, selectSymbol -> planSubquery(selectSymbol, subQueries))
        );
        return subQueries;
    }

    private void planSubquery(SelectSymbol selectSymbol, SubQueries subQueries) {
        if (selectSymbol.isCorrelated()) {
            if (!subQueries.correlated.containsKey(selectSymbol)) {
                LogicalPlan subPlan = planSubSelects.apply(selectSymbol);
                subQueries.correlated.put(selectSymbol, subPlan);
            }
        } else {
            LogicalPlan subPlan = planSubSelects.apply(selectSymbol);
            subQueries.uncorrelated.put(subPlan, selectSymbol);
        }
    }
}
