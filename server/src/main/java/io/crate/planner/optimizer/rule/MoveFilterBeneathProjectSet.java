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

package io.crate.planner.optimizer.rule;

import static io.crate.planner.operators.LogicalPlanner.extractColumns;
import static io.crate.planner.optimizer.matcher.Pattern.typeOf;
import static io.crate.planner.optimizer.matcher.Patterns.source;
import static io.crate.planner.optimizer.rule.Util.transpose;

import java.util.ArrayList;
import java.util.List;
import java.util.function.UnaryOperator;

import io.crate.expression.operator.AndOperator;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.SymbolVisitors;
import io.crate.metadata.FunctionType;
import io.crate.metadata.NodeContext;
import io.crate.metadata.TransactionContext;
import io.crate.planner.PlannerContext;
import io.crate.planner.operators.Filter;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.operators.ProjectSet;
import io.crate.planner.optimizer.Rule;
import io.crate.planner.optimizer.costs.PlanStats;
import io.crate.planner.optimizer.matcher.Capture;
import io.crate.planner.optimizer.matcher.Captures;
import io.crate.planner.optimizer.matcher.Pattern;

public final class MoveFilterBeneathProjectSet implements Rule<Filter> {

    private final Capture<ProjectSet> projectSetCapture;
    private final Pattern<Filter> pattern;

    public MoveFilterBeneathProjectSet() {
        this.projectSetCapture = new Capture<>();
        this.pattern = typeOf(Filter.class)
            .with(source(), typeOf(ProjectSet.class).capturedAs(projectSetCapture));
    }

    @Override
    public Pattern<Filter> pattern() {
        return pattern;
    }

    @Override
    public LogicalPlan apply(Filter filter,
                             Captures captures,
                             PlanStats planStats,
                             TransactionContext txnCtx,
                             NodeContext nodeCtx,
                             UnaryOperator<LogicalPlan> resolvePlan,
                             PlannerContext plannerContext) {
        var projectSet = captures.get(projectSetCapture);

        var queryParts = AndOperator.split(filter.query());
        ArrayList<Symbol> toPushDown = new ArrayList<>();
        ArrayList<Symbol> toKeep = new ArrayList<>();
        for (var part : queryParts) {
            if (!SymbolVisitors.any(MoveFilterBeneathProjectSet::isTableFunction, part)
                && projectSet.standaloneOutputs().containsAll(extractColumns(part))) {
                toPushDown.add(part);
            } else {
                toKeep.add(part);
            }
        }

        if (toPushDown.isEmpty()) {
            return null;
        } else if (toKeep.isEmpty()) {
            return transpose(filter, projectSet);
        } else {
            var newProjectSet = projectSet.replaceSources(
                List.of(new Filter(projectSet.source(), AndOperator.join(toPushDown))));
            return new Filter(newProjectSet, AndOperator.join(toKeep));
        }
    }

    private static boolean isTableFunction(Symbol s) {
        return s instanceof io.crate.expression.symbol.Function &&
               ((io.crate.expression.symbol.Function) s).signature().getKind() == FunctionType.TABLE;
    }
}
