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
public class MultiPhasePlan implements Plan {

    private final Plan rootPlan;
    private final Map<Plan, SelectSymbol> dependencies;

    public static Plan createIfNeeded(Plan subPlan, Map<Plan, SelectSymbol> dependencies) {
        if (dependencies.isEmpty()) {
            return subPlan;
        }
        return new MultiPhasePlan(subPlan, dependencies);
    }

    private MultiPhasePlan(Plan rootPlan, Map<Plan, SelectSymbol> dependencies) {
        this.rootPlan = rootPlan;
        this.dependencies = dependencies;
    }

    public Map<Plan, SelectSymbol> dependencies() {
        return dependencies;
    }

    public Plan rootPlan() {
        return rootPlan;
    }

    @Override
    public <C, R> R accept(PlanVisitor<C, R> visitor, C context) {
        return visitor.visitMultiPhasePlan(this, context);
    }

    @Override
    public UUID jobId() {
        return rootPlan.jobId();
    }

    @Override
    public void addProjection(Projection projection) {
        rootPlan.addProjection(projection);
    }

    @Override
    public void addProjection(Projection projection,
                              int unfinishedLimit,
                              int unfinishedOffset,
                              @Nullable PositionalOrderBy unfinishedOrderBy) {
        rootPlan.addProjection(projection, unfinishedLimit, unfinishedOffset, unfinishedOrderBy);
    }

    @Override
    public ResultDescription resultDescription() {
        return rootPlan.resultDescription();
    }

    @Override
    public void setDistributionInfo(DistributionInfo distributionInfo) {
        rootPlan.setDistributionInfo(distributionInfo);
    }
}
