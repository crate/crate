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

import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
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
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.optimizer.Rule;
import io.crate.planner.optimizer.costs.PlanStats;
import io.crate.planner.optimizer.matcher.Captures;
import io.crate.planner.optimizer.matcher.Pattern;
import io.crate.sql.tree.JoinType;

public class MoveConstantJoinConditionsBeneathJoin implements Rule<JoinPlan> {

    private final Pattern<JoinPlan> pattern;
    private final EnumSet<JoinType> SUPPORTED_JOIN_TYPE = EnumSet.of(
        JoinType.INNER,
        JoinType.LEFT,
        JoinType.RIGHT,
        JoinType.CROSS
    );

    public MoveConstantJoinConditionsBeneathJoin() {
        this.pattern = typeOf(JoinPlan.class)
            .with(join -> SUPPORTED_JOIN_TYPE.contains(join.joinType()) &&
                !join.moveConstantJoinConditionRuleApplied() &&
                hasConstantJoinConditions(join.joinCondition()));
    }

    private static boolean hasConstantJoinConditions(Symbol joinCondition) {
        if (joinCondition == null) {
            return false;
        }
        for (var condition : QuerySplitter.split(joinCondition).values()) {
            if (numberOfRelationsUsed(condition) <= 1) {
                return true;
            }
        }
        return false;
    }

    @Override
    public Pattern<JoinPlan> pattern() {
        return pattern;
    }

    @Override
    public LogicalPlan apply(JoinPlan joinPlan,
                             Captures captures,
                             PlanStats planStats,
                             TransactionContext txnCtx,
                             NodeContext nodeCtx,
                             UnaryOperator<LogicalPlan> resolvePlan) {
        var conditions = joinPlan.joinCondition();
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
        if (!constantConditions.isEmpty() && !nonConstantConditions.isEmpty()) {
            var lhsRelations = new HashSet<>(joinPlan.lhs().relationNames());
            var rhsRelations = new HashSet<>(joinPlan.rhs().relationNames());
            final LogicalPlan newLhs;
            final LogicalPlan newRhs;
            if (joinPlan.joinType() == JoinType.INNER || joinPlan.joinType() == JoinType.CROSS) {
                // Cross/Inner-join will filter on both sides
                // therefore constant filters can be pushed down to both sides
                var lhs = resolvePlan.apply(joinPlan.lhs());
                var rhs = resolvePlan.apply(joinPlan.rhs());
                var queryForLhs = constantConditions.remove(lhsRelations);
                var queryForRhs = constantConditions.remove(rhsRelations);
                newLhs = getNewSource(queryForLhs, lhs);
                newRhs = getNewSource(queryForRhs, rhs);
            } else if (joinPlan.joinType() == JoinType.RIGHT) {
                // Right-join will only filter on the lhs
                // therefore constant filters can be pushed down to the lhs
                var lhs = resolvePlan.apply(joinPlan.lhs());
                var queryForLhs = constantConditions.remove(lhsRelations);
                newLhs = getNewSource(queryForLhs, lhs);
                newRhs = joinPlan.rhs();
            } else if (joinPlan.joinType() == JoinType.LEFT) {
                // Left-join will only filter on the rhs
                // therefore constant filters can be pushed down to the rhs
                var rhs = resolvePlan.apply(joinPlan.rhs());
                var queryForRhs = constantConditions.remove(rhsRelations);
                newRhs = getNewSource(queryForRhs, rhs);
                newLhs = joinPlan.lhs();
            } else {
                return joinPlan.withMoveConstantJoinConditionRuleApplied(true);
            }

            var newJoinConditions = new LinkedHashSet<>(nonConstantConditions);
            newJoinConditions.addAll(constantConditions.values());

            return new JoinPlan(
                newLhs,
                newRhs,
                joinPlan.joinType(),
                AndOperator.join(newJoinConditions),
                joinPlan.isFiltered(),
                joinPlan.isRewriteFilterOnOuterJoinToInnerJoinDone(),
                joinPlan.isLookUpJoinRuleApplied(),
                true,
                joinPlan.lookUpJoin()
            );
        } else {
            return joinPlan.withMoveConstantJoinConditionRuleApplied(true);
        }
    }

    private static int numberOfRelationsUsed(Symbol joinCondition) {
        return RelationNameCollector.collect(joinCondition).size();
    }
}
