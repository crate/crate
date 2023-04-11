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

import java.util.function.Function;

import io.crate.metadata.NodeContext;
import io.crate.metadata.TransactionContext;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.operators.NestedLoopJoin;
import io.crate.planner.optimizer.Rule;
import io.crate.planner.optimizer.matcher.Captures;
import io.crate.planner.optimizer.matcher.Pattern;
import io.crate.planner.optimizer.stats.StatsEstimator;
import io.crate.statistics.TableStats;

public class SwapTablesInNestedLoopJoin implements Rule<NestedLoopJoin> {

    private final Pattern<NestedLoopJoin> pattern = typeOf(NestedLoopJoin.class)
        .with(nl -> nl.isRewriteNestedLoopJoinToHashJoinDone() == true &&
                    nl.orderByWasPushedDown() == false);

    @Override
    public Pattern<NestedLoopJoin> pattern() {
        return pattern;
    }

    @Override
    public LogicalPlan apply(NestedLoopJoin join,
                             Captures captures,
                             TableStats tableStats,
                             TransactionContext txnCtx,
                             NodeContext nodeCtx,
                             Function<LogicalPlan, LogicalPlan> resolvePlan) {
        StatsEstimator statsEstimator = new StatsEstimator(resolvePlan);
        StatsEstimator.Context lhsStats = new StatsEstimator.Context();
        StatsEstimator.Context rhsStats = new StatsEstimator.Context();
        join.lhs().accept(statsEstimator, lhsStats);
        join.rhs().accept(statsEstimator, rhsStats);
        boolean expectedRowsAvailable = lhs.numExpectedRows() != -1 && rhs.numExpectedRows() != -1;
        if (lhsStats.numExpectedRows < rhsStats.numExpectedRows) {
                 return new NestedLoopJoin(
                     join.rhs(),
                     join.lhs(),
                     join.joinType().invert(),
                     join.joinCondition(),
                     join.isFiltered(),
                     join.topMostLeftRelation(),
                     join.orderByWasPushedDown(),
                     join.isRewriteFilterOnOuterJoinToInnerJoinDone(),
                     join.isJoinConditionOptimised(),
                     join.isRewriteNestedLoopJoinToHashJoinDone());
            }
            return null;
    }
}
