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

import io.crate.data.Row;
import io.crate.planner.distribution.DistributionInfo;
import io.crate.planner.projection.Projection;
import io.crate.planner.projection.builder.ProjectionBuilder;

import javax.annotation.Nullable;

/**
 * A Plan that can only be used as root plan and cannot be used as sub-plan of another plan.
 */
public abstract class UnnestablePlan implements ExecutionPlan, Plan {

    @Override
    public void addProjection(Projection projection) {
        throw new UnsupportedOperationException("addProjection() is not supported on: " + getClass().getSimpleName());
    }

    @Override
    public void addProjection(Projection projection,
                              int unfinishedLimit,
                              int unfinishedOffset,
                              @Nullable PositionalOrderBy unfinishedOrderBy) {
        throw new UnsupportedOperationException("addProjection() is not supported on: " + getClass().getSimpleName());
    }

    @Override
    public ResultDescription resultDescription() {
        throw new UnsupportedOperationException("resultDescription() is not supported on: " + getClass().getSimpleName());
    }

    @Override
    public void setDistributionInfo(DistributionInfo distributionInfo) {
        throw new UnsupportedOperationException("Cannot change distributionInfo on: " + getClass().getSimpleName());
    }

    @Override
    public ExecutionPlan build(PlannerContext plannerContext, ProjectionBuilder projectionBuilder, Row params) {
        return this;
    }
}
