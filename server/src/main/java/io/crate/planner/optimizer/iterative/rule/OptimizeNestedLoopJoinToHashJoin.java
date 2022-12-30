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

package io.crate.planner.optimizer.iterative.rule;

import static io.crate.planner.operators.EquiJoinDetector.isHashJoinPossible;
import static io.crate.planner.optimizer.iterative.rule.Pattern.typeOf;

import io.crate.metadata.NodeContext;
import io.crate.metadata.TransactionContext;
import io.crate.planner.operators.HashJoin;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.operators.NestedLoopJoin;
import io.crate.planner.optimizer.iterative.Lookup;
import io.crate.statistics.TableStats;

public class OptimizeNestedLoopJoinToHashJoin implements Rule<NestedLoopJoin> {

    private final Pattern<NestedLoopJoin> pattern = typeOf(NestedLoopJoin.class)
        .with(nl -> nl.isFinalized() == false);

    @Override
    public Pattern<NestedLoopJoin> pattern() {
        return this.pattern;
    }

    @Override
    public LogicalPlan apply(NestedLoopJoin nl,
                             Captures captures,
                             TableStats tableStats,
                             TransactionContext txnCtx,
                             NodeContext nodeCtx,
                             Lookup lookup) {
        if (nl.joinCondition() != null &&
            txnCtx.sessionSettings().hashJoinsEnabled() &&
            isHashJoinPossible(nl.joinType(), nl.joinCondition())) {
            return new HashJoin(
                nl.lhs(),
                nl.rhs(),
                nl.joinCondition(),
                nl.id()
            );
        }
        return new NestedLoopJoin(
            nl.lhs(),
            nl.rhs(),
            nl.joinType(),
            nl.joinCondition(),
            nl.isFiltered(),
            nl.topMostLeftRelation(),
            nl.orderByWasPushedDown(),
            nl.isRewriteFilterOnOuterJoinToInnerJoinDone(),
            nl.isJoinConditionOptimised(),
            true, // Mark as processed
            nl.id()
        );
    }
}
