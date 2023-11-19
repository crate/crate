/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.planner.node.management;

import static io.crate.planner.node.management.ExplainPlan.createPrintContext;

import java.util.ArrayList;
import java.util.List;

import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.optimizer.Rule;
import io.crate.planner.optimizer.costs.PlanStats;
import io.crate.planner.optimizer.tracer.OptimizerProgressTracker;

public class VerboseOptimizerProgressTracker implements OptimizerProgressTracker {

    private final OptimizerProgressTracker defaultTracker;
    private final List<OptimizerStep> steps = new ArrayList<>();
    private final boolean showCosts;

    public VerboseOptimizerProgressTracker(OptimizerProgressTracker defaultTracker, boolean showCosts) {
        this.defaultTracker = defaultTracker;
        this.showCosts = showCosts;
    }

    @Override
    public void optimizationStarted(LogicalPlan initialPlan, PlanStats planStats) {
        // A plan can go through multiple optimizers. Here, we capture the initial plan only once.
        if (steps.isEmpty()) {
            var printContext = createPrintContext(planStats, showCosts);
            initialPlan.print(printContext);
            steps.add(OptimizerStep.initial(printContext.toString()));
        }
        defaultTracker.optimizationStarted(initialPlan, planStats);
    }

    @Override
    public void ruleMatched(Rule<?> rule) {
        defaultTracker.ruleMatched(rule);
    }

    @Override
    public void ruleApplied(Rule<?> rule, LogicalPlan plan, PlanStats planStats) {
        var printContext = createPrintContext(planStats, showCosts);
        plan.print(printContext);
        steps.add(OptimizerStep.ruleApplied(rule, printContext.toString()));
        defaultTracker.ruleApplied(rule, plan, planStats);
    }

    public List<OptimizerStep> getSteps() {
        return steps;
    }
}
