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

package io.crate.planner.optimizer.stats;

import java.util.function.Function;

import io.crate.planner.operators.Collect;
import io.crate.planner.operators.LimitDistinct;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.operators.LogicalPlanVisitor;
import io.crate.planner.optimizer.iterative.GroupReference;

public class StatsEstimator extends LogicalPlanVisitor<StatsEstimator.Context, Void> {

    private final  Function<LogicalPlan, LogicalPlan> resolvePlan;

    public StatsEstimator(Function<LogicalPlan, LogicalPlan> resolvePlan) {
        this.resolvePlan = resolvePlan;
    }

    @Override
    public Void visitGroupReference(GroupReference groupReference, Context context) {
        resolvePlan.apply(groupReference).accept(this, context);
        return null;
    }

    @Override
    public Void visitCollect(Collect logicalPlan, StatsEstimator.Context context) {
        context.numExpectedRows = logicalPlan.numExpectedRows();
        return null;
    }

    @Override
    public Void visitPlan(LogicalPlan logicalPlan, Context context) {
        for (LogicalPlan source : logicalPlan.sources()) {
            source.accept(this, context);
        }
        return null;
    }

    public static class Context {
        public long numExpectedRows = 0L;
    }
}
