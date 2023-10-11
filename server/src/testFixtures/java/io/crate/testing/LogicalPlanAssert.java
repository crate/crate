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

package io.crate.testing;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import org.assertj.core.api.AbstractAssert;
import org.jetbrains.annotations.Nullable;

import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.operators.PrintContext;
import io.crate.planner.optimizer.costs.PlanStats;

public class LogicalPlanAssert extends AbstractAssert<LogicalPlanAssert, LogicalPlan> {

    @Nullable
    private final PlanStats planStats;

    protected LogicalPlanAssert(LogicalPlan actual, @Nullable PlanStats planStats) {
        super(actual, LogicalPlanAssert.class);
        this.planStats = planStats;
    }

    private String printPlan(LogicalPlan logicalPlan) {
        var printContext = new PrintContext(planStats);
        logicalPlan.print(printContext);
        return printContext.toString();
    }

    public LogicalPlanAssert withPlanStats(PlanStats planStats) {
        return new LogicalPlanAssert(this.actual, planStats);
    }

    public LogicalPlanAssert isEqualTo(String expectedPlan) {
        isNotNull();
        assertThat(expectedPlan).isNotNull();
        assertThat(printPlan(actual)).isEqualTo(expectedPlan.strip());
        return this;
    }

    public LogicalPlanAssert hasOperators(String ... operator) {
        isNotNull();
        assertThat(printPlan(actual).split("\n"))
            .containsExactly(operator);
        return this;
    }

    public LogicalPlanAssert isEqualTo(LogicalPlan expectedPlan) {
        isNotNull();
        assertThat(expectedPlan).isNotNull();
        assertThat(printPlan(actual)).isEqualTo(printPlan(expectedPlan));
        return this;
    }
}
