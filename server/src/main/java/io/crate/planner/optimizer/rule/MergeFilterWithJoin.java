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

import static io.crate.planner.optimizer.matcher.Pattern.typeOf;
import static io.crate.planner.optimizer.matcher.Patterns.source;
import static io.crate.planner.optimizer.rule.ExtractConstantJoinConditionsIntoFilter.numberOfRelationsUsed;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.function.Function;
import java.util.function.UnaryOperator;

import io.crate.analyze.relations.QuerySplitter;
import io.crate.expression.operator.AndOperator;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.NodeContext;
import io.crate.metadata.TransactionContext;
import io.crate.planner.operators.Filter;
import io.crate.planner.operators.JoinPlan;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.optimizer.Rule;
import io.crate.planner.optimizer.costs.PlanStats;
import io.crate.planner.optimizer.matcher.Capture;
import io.crate.planner.optimizer.matcher.Captures;
import io.crate.planner.optimizer.matcher.Pattern;
import io.crate.sql.tree.JoinType;

public class MergeFilterWithJoin implements Rule<Filter> {

    private final Capture<JoinPlan> joinCapture;
    private final Pattern<Filter> pattern;

    public MergeFilterWithJoin() {
        this.joinCapture = new Capture<>();
        this.pattern = typeOf(Filter.class)
            .with(source(),
                typeOf(JoinPlan.class)
                    .capturedAs(joinCapture)
            );
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
                             UnaryOperator<LogicalPlan> resolvePlan) {
        var join = captures.get(joinCapture);
        var allConditions = QuerySplitter.split(filter.query());
        if (allConditions.size() == 1) {
            var relationNames = new HashSet<>(join.getRelationNames());
            var conditions = new HashSet<Symbol>(allConditions.size());
            for (var entry : allConditions.entrySet()) {
                if (numberOfRelationsUsed(entry.getValue()) == 2 && relationNames.containsAll(entry.getKey())) {
                    conditions.add(entry.getValue());
                }
            }
            if (conditions.isEmpty()) {
                return null;
            }
            if (join.joinType() == JoinType.CROSS) {
                return new JoinPlan(join.lhs(), join.rhs(), JoinType.INNER, AndOperator.join(conditions));
            } else {
                var newConditions = new ArrayList<Symbol>();
                newConditions.add(join.joinCondition());
                newConditions.addAll(conditions);
                return new JoinPlan(join.lhs(), join.rhs(), join.joinType(), AndOperator.join(newConditions));
            }
        }
        return null;
    }
}
