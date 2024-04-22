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

import io.crate.metadata.NodeContext;
import io.crate.metadata.TransactionContext;
import io.crate.planner.operators.HashJoin;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.optimizer.Rule;
import io.crate.planner.optimizer.costs.PlanStats;
import io.crate.planner.optimizer.matcher.Captures;
import io.crate.planner.optimizer.matcher.Pattern;

public class ReorderHashJoin implements Rule<HashJoin> {

    private final Pattern<HashJoin> pattern = typeOf(HashJoin.class);

    @Override
    public Pattern<HashJoin> pattern() {
        return pattern;
    }

    @Override
    public LogicalPlan apply(HashJoin plan,
                             Captures captures,
                             PlanStats planStats,
                             TransactionContext txnCtx,
                             NodeContext nodeCtx,
                             UnaryOperator<LogicalPlan> resolvePlan) {
        var lhStats = planStats.get(plan.lhs());
        var rhStats = planStats.get(plan.rhs());
        boolean expectedRowsAvailable = lhStats.numDocs() != -1 && rhStats.numDocs() != -1;
        // We move the smaller table to the right side since benchmarking
        // revealed that this improves performance in most cases.
        if (expectedRowsAvailable && lhStats.numDocs() < rhStats.numDocs()) {
            new HashJoin(
                plan.outputs(),
                plan.rhs(),
                plan.lhs(),
                plan.joinCondition()
            );
        }
        return null;
    }
}
