/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.planner;

import io.crate.analyze.symbol.SelectSymbol;
import io.crate.planner.distribution.DistributionInfo;
import io.crate.planner.projection.Projection;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.UUID;

/**
 * Plan which depends on other plans to be executed first.
 *
 * E.g. for sub selects:
 *
 * <pre>
 * select * from t where x = (select 1) or x = (select 2)
 * </pre>
 *
 * The two subselects would be the dependencies.
 *
 * The dependencies themselves may also be MultiPhasePlans.
 */
public class MultiPhasePlan implements ExecutionPlan {

    private final ExecutionPlan rootExecutionPlan;
    private final Map<ExecutionPlan, SelectSymbol> dependencies;

    public static ExecutionPlan createIfNeeded(ExecutionPlan subExecutionPlan, Map<ExecutionPlan, SelectSymbol> dependencies) {
        if (dependencies.isEmpty()) {
            return subExecutionPlan;
        }
        return new MultiPhasePlan(subExecutionPlan, dependencies);
    }

    private MultiPhasePlan(ExecutionPlan rootExecutionPlan, Map<ExecutionPlan, SelectSymbol> dependencies) {
        this.rootExecutionPlan = rootExecutionPlan;
        this.dependencies = dependencies;
    }

    public Map<ExecutionPlan, SelectSymbol> dependencies() {
        return dependencies;
    }

    public ExecutionPlan rootPlan() {
        return rootExecutionPlan;
    }

    @Override
    public <C, R> R accept(ExecutionPlanVisitor<C, R> visitor, C context) {
        return visitor.visitMultiPhasePlan(this, context);
    }

    @Override
    public UUID jobId() {
        return rootExecutionPlan.jobId();
    }

    @Override
    public void addProjection(Projection projection) {
        rootExecutionPlan.addProjection(projection);
    }

    @Override
    public void addProjection(Projection projection,
                              int unfinishedLimit,
                              int unfinishedOffset,
                              @Nullable PositionalOrderBy unfinishedOrderBy) {
        rootExecutionPlan.addProjection(projection, unfinishedLimit, unfinishedOffset, unfinishedOrderBy);
    }

    @Override
    public ResultDescription resultDescription() {
        return rootExecutionPlan.resultDescription();
    }

    @Override
    public void setDistributionInfo(DistributionInfo distributionInfo) {
        rootExecutionPlan.setDistributionInfo(distributionInfo);
    }
}
