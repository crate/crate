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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import io.crate.analyze.RelationNames;
import io.crate.expression.operator.AndOperator;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.RelationName;
import io.crate.planner.operators.JoinPlan;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.optimizer.Rule;
import io.crate.planner.optimizer.matcher.Captures;
import io.crate.planner.optimizer.matcher.Pattern;
import io.crate.sql.tree.JoinType;

/// Moves conditions of an inner join, which reference relations of only one side of the join,
/// beneath the join into that side.
public class PushdownInnerJoinConditions implements Rule<JoinPlan> {

    private final Pattern<JoinPlan> pattern = typeOf(JoinPlan.class)
        .with(j -> j.joinType() == JoinType.INNER &&
            j.joinCondition() != null &&
            hasSameSideConditions(j));

    private static boolean hasSameSideConditions(JoinPlan join) {
        Set<RelationName> lhsRelations = new HashSet<>(join.lhs().relationNames());
        Set<RelationName> rhsRelations = new HashSet<>(join.rhs().relationNames());
        for (Symbol condition : AndOperator.split(join.joinCondition())) {
            if (!condition.isDeterministic()) {
                continue;
            }
            Set<RelationName> relations = RelationNames.getShallow(condition);
            if (lhsRelations.containsAll(relations) || rhsRelations.containsAll(relations)) {
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
    public LogicalPlan apply(JoinPlan join,
                             Captures captures,
                             Rule.Context context) {
        Set<RelationName> lhsRelations = new HashSet<>(join.lhs().relationNames());
        Set<RelationName> rhsRelations = new HashSet<>(join.rhs().relationNames());
        List<Symbol> lhsConditions = new ArrayList<>();
        List<Symbol> rhsConditions = new ArrayList<>();
        List<Symbol> joinConditions = new ArrayList<>();
        for (Symbol condition : AndOperator.split(join.joinCondition())) {
            if (condition.isDeterministic()) {
                Set<RelationName> relations = RelationNames.getShallow(condition);
                // Conditions without any relation are pushed to the lhs
                if (lhsRelations.containsAll(relations)) {
                    lhsConditions.add(condition);
                    continue;
                }
                if (rhsRelations.containsAll(relations)) {
                    rhsConditions.add(condition);
                    continue;
                }
            }
            joinConditions.add(condition);
        }
        return new JoinPlan(
            getNewSource(AndOperator.join(lhsConditions, null), join.lhs()),
            getNewSource(AndOperator.join(rhsConditions, null), join.rhs()),
            joinConditions.isEmpty() ? JoinType.CROSS : JoinType.INNER,
            AndOperator.join(joinConditions, null),
            join.isFiltered(),
            join.isRewriteFilterOnOuterJoinToInnerJoinDone(),
            join.isLookUpJoinRuleApplied(),
            join.moveConstantJoinConditionRuleApplied(),
            join.eliminateCrossJoinRuleIsApplied(),
            join.lookUpJoin()
        );
    }
}
