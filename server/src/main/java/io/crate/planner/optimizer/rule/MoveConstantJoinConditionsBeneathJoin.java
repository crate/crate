/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.planner.optimizer.rule;

import static io.crate.planner.optimizer.matcher.Pattern.typeOf;
import static io.crate.planner.optimizer.rule.MoveFilterBeneathJoin.getNewSource;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.function.UnaryOperator;

import io.crate.analyze.relations.QuerySplitter;
import io.crate.expression.operator.AndOperator;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.NodeContext;
import io.crate.metadata.RelationName;
import io.crate.metadata.TransactionContext;
import io.crate.planner.consumer.RelationNameCollector;
import io.crate.planner.operators.JoinPlan;
import io.crate.planner.optimizer.costs.PlanStats;
import io.crate.sql.tree.JoinType;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.optimizer.Rule;
import io.crate.planner.optimizer.matcher.Captures;
import io.crate.planner.optimizer.matcher.Pattern;

public class MoveConstantJoinConditionsBeneathJoin implements Rule<JoinPlan> {

    private final Pattern<JoinPlan> pattern;

    public MoveConstantJoinConditionsBeneathJoin() {
        this.pattern = typeOf(JoinPlan.class)
            .with(nl -> nl.joinType() == JoinType.INNER &&
                        !nl.isJoinConditionOptimized() &&
                        nl.joinCondition() != null);
    }

    @Override
    public Pattern<JoinPlan> pattern() {
        return pattern;
    }

    @Override
    public LogicalPlan apply(JoinPlan join,
                             Captures captures,
                             PlanStats planStats,
                             TransactionContext txnCtx,
                             NodeContext nodeCtx,
                             UnaryOperator<LogicalPlan> resolvePlan) {
        var conditions = join.joinCondition();
        var allConditions = QuerySplitter.split(conditions);
        var constantConditions = new HashMap<Set<RelationName>, Symbol>(allConditions.size());
        var nonConstantConditions = new HashSet<Symbol>(allConditions.size());
        for (var condition : allConditions.entrySet()) {
            if (numberOfRelationsUsed(condition.getValue()) <= 1) {
                constantConditions.put(condition.getKey(), condition.getValue());
            } else {
                nonConstantConditions.add(condition.getValue());
            }
        }
        if (constantConditions.isEmpty() || nonConstantConditions.isEmpty()) {
            // Nothing to optimize, just mark it to skip the rule the next time
            return new JoinPlan(
                join.lhs(),
                join.rhs(),
                join.joinType(),
                join.joinCondition(),
                join.isFiltered(),
                join.isRewriteFilterOnOuterJoinToInnerJoinDone(),
                join.isOrderPushedThrough(),
                true
            );
        } else {
            // Push constant join condition down to source
            var lhs = resolvePlan.apply(join.lhs());
            var rhs = resolvePlan.apply(join.rhs());
            var queryForLhs = constantConditions.remove(new HashSet<>(lhs.getRelationNames()));
            var queryForRhs = constantConditions.remove(new HashSet<>(rhs.getRelationNames()));
            var newLhs = getNewSource(queryForLhs, lhs);
            var newRhs = getNewSource(queryForRhs, rhs);
            return new JoinPlan(
                newLhs,
                newRhs,
                join.joinType(),
                AndOperator.join(nonConstantConditions),
                join.isFiltered(),
                join.isRewriteFilterOnOuterJoinToInnerJoinDone(),
                join.isOrderPushedThrough(),
                true
            );
        }
    }

    private static int numberOfRelationsUsed(Symbol joinCondition) {
        return RelationNameCollector.collect(joinCondition).size();
    }
}
