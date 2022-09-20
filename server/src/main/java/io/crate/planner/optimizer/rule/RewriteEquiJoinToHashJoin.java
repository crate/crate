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

import static io.crate.planner.operators.EquiJoinDetector.isEquiJoin;
import static io.crate.planner.optimizer.matcher.Pattern.typeOf;

import io.crate.metadata.NodeContext;
import io.crate.metadata.TransactionContext;
import io.crate.planner.node.dql.join.JoinType;
import io.crate.planner.operators.HashJoin;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.operators.NestedLoopJoin;
import io.crate.planner.optimizer.Rule;
import io.crate.planner.optimizer.matcher.Captures;
import io.crate.planner.optimizer.matcher.Pattern;
import io.crate.statistics.TableStats;

public final class RewriteEquiJoinToHashJoin implements Rule<NestedLoopJoin> {

    private final Pattern<NestedLoopJoin> pattern;

    public RewriteEquiJoinToHashJoin() {
        this.pattern = typeOf(NestedLoopJoin.class)
            .with(nestedLoopJoin -> !nestedLoopJoin.isFinalized() &&
                                    nestedLoopJoin.joinType() == JoinType.INNER);
    }

    @Override
    public Pattern<NestedLoopJoin> pattern() {
        return this.pattern;
    }

    @Override
    public LogicalPlan apply(NestedLoopJoin nestedLoopJoin,
                             Captures captures,
                             TableStats tableStats,
                             TransactionContext txnCtx,
                             NodeContext nodeCtx) {
        if (nestedLoopJoin.joinCondition() != null &&
            txnCtx.sessionSettings().hashJoinsEnabled() &&
            isEquiJoin(nestedLoopJoin.joinCondition())) {
            return new HashJoin(
                nestedLoopJoin.lhs(),
                nestedLoopJoin.rhs(),
                nestedLoopJoin.joinCondition()
            );
        } else {
            return new NestedLoopJoin(
                nestedLoopJoin.lhs(),
                nestedLoopJoin.rhs(),
                nestedLoopJoin.joinType(),
                nestedLoopJoin.joinCondition(),
                nestedLoopJoin.isFiltered(),
                nestedLoopJoin.topMostLeftRelation(),
                nestedLoopJoin.orderByWasPushedDown(),
                nestedLoopJoin.isRewriteFilterOnOuterJoinToInnerJoinDone(),
                nestedLoopJoin.isJoinConditionOptimised(),
                true
            );
        }
    }
}
