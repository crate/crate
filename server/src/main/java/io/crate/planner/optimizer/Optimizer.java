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

package io.crate.planner.optimizer;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.UnaryOperator;

import org.jetbrains.annotations.Nullable;

import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.NodeContext;
import io.crate.metadata.TransactionContext;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.optimizer.costs.PlanStats;
import io.crate.planner.optimizer.matcher.Captures;
import io.crate.planner.optimizer.matcher.Match;
import io.crate.planner.optimizer.tracer.OptimizerTracer;

public interface Optimizer {

    LogicalPlan optimize(LogicalPlan plan, PlanStats planStats, CoordinatorTxnCtx txnCtx, OptimizerTracer tracer);

    @Nullable
    default <T> LogicalPlan tryMatchAndApply(Rule<T> rule,
                                             LogicalPlan node,
                                             PlanStats planStats,
                                             NodeContext nodeCtx,
                                             TransactionContext txnCtx,
                                             UnaryOperator<LogicalPlan> resolvePlan,
                                             OptimizerTracer tracer) {
        Match<T> match = rule.pattern().accept(node, Captures.empty(), resolvePlan);
        if (match.isPresent()) {
            tracer.ruleMatched(rule);
            return rule.apply(match.value(), match.captures(), planStats, txnCtx, nodeCtx, resolvePlan);
        }
        return null;
    }

    static List<Rule<?>> removeExcludedRules(List<Rule<?>> rules, Set<Class<? extends Rule<?>>> excludedRules) {
        if (excludedRules.isEmpty()) {
            return rules;
        }
        var result = new ArrayList<Rule<?>>(rules.size());
        for (var rule : rules) {
            if (rule.mandatory() == false &&
                excludedRules.contains(rule.getClass())) {
                continue;
            }
            result.add(rule);
        }
        return result;
    }

}
