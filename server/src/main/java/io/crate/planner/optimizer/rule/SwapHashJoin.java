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
import io.crate.planner.operators.HashJoin;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.optimizer.Rule;
import io.crate.planner.optimizer.costs.PlanStats;
import io.crate.planner.optimizer.matcher.Captures;
import io.crate.planner.optimizer.matcher.Pattern;

public class SwapHashJoin implements Rule<HashJoin> {

    private static final Pattern<HashJoin> PATTERN = typeOf(HashJoin.class).with(j -> j.isSwapSideDone() == false);
    @Override
    public Pattern<HashJoin> pattern() {
        return PATTERN;
    }

    @Override
    public LogicalPlan apply(HashJoin join,
                             Captures captures,
                             PlanStats planStats,
                             TransactionContext txnCtx,
                             NodeContext nodeCtx,
                             Function<LogicalPlan, LogicalPlan> resolvePlan) {
       var lhsStats = planStats.apply(join.lhs());
       var rhsStats = planStats.apply(join.rhs());
        // We move smaller table to the right side since benchmarking
        // revealed that this improves performance in most cases.
        var swapSides = false;
        var expectedRowsAvailable = lhsStats.numDocs() != -1 && rhsStats.numDocs() != -1;
        if (expectedRowsAvailable && lhsStats.numDocs() < rhsStats.numDocs()) {
            swapSides = true;
        }
        return new HashJoin(
            join.lhs(),
            join.rhs(),
            join.joinCondition(),
            swapSides,
            true
        );
    }
}
