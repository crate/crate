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

import java.util.function.UnaryOperator;

import io.crate.analyze.relations.AbstractTableRelation;
import io.crate.metadata.NodeContext;
import io.crate.metadata.TransactionContext;
import io.crate.planner.operators.JoinPlan;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.operators.LookupJoin;
import io.crate.planner.optimizer.Rule;
import io.crate.planner.optimizer.costs.PlanStats;
import io.crate.planner.optimizer.matcher.Captures;
import io.crate.planner.optimizer.matcher.Pattern;

public class UseLookupJoin implements Rule<JoinPlan> {

    private final Pattern<JoinPlan> pattern = typeOf(JoinPlan.class).with(j -> j.isLookUpJoinRuleApplied() == false);

    @Override
    public Pattern<JoinPlan> pattern() {
        return pattern;
    }

    @Override
    public LogicalPlan apply(JoinPlan plan,
                             Captures captures,
                             PlanStats planStats,
                             TransactionContext txnCtx,
                             NodeContext nodeCtx,
                             UnaryOperator<LogicalPlan> resolvePlan) {
        LogicalPlan lhs = resolvePlan.apply(plan.lhs());
        LogicalPlan rhs =  resolvePlan.apply(plan.rhs());

        var lhsStats = planStats.get(lhs).numDocs();
        var rhsStats = planStats.get(rhs).numDocs();

        if (lhsStats == -1 || rhsStats == -1) {
            return null;
        }

        LogicalPlan largerSide;
        LogicalPlan smallerSide;

        if (lhsStats > rhsStats) {
            largerSide = lhs;
            smallerSide = rhs;
        } else {
            largerSide = rhs;
            smallerSide = lhs;
        }

        AbstractTableRelation<?> largerRelation = largerSide.baseTables().get(0);
        AbstractTableRelation<?> smallerRelation = smallerSide.baseTables().get(0);

        LookupJoin lookupJoin = LookupJoin.create(
            largerRelation,
            smallerRelation,
            plan.joinCondition(),
            nodeCtx,
            txnCtx);

        return new JoinPlan(lookupJoin,
            smallerSide,
            plan.joinType(),
            plan.joinCondition(),
            plan.isFiltered(),
            plan.isRewriteFilterOnOuterJoinToInnerJoinDone(),
            true);
    }
}
