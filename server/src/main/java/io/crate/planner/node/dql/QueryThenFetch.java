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

package io.crate.planner.node.dql;

import io.crate.planner.ExecutionPlan;
import io.crate.planner.ExecutionPlanVisitor;
import io.crate.planner.PositionalOrderBy;
import io.crate.planner.ResultDescription;
import io.crate.planner.distribution.DistributionInfo;
import io.crate.execution.dsl.phases.FetchPhase;
import io.crate.execution.dsl.projection.Projection;

import org.jspecify.annotations.Nullable;
import java.util.Objects;

public class QueryThenFetch implements ExecutionPlan {

    private final FetchPhase fetchPhase;
    private final ExecutionPlan subExecutionPlan;

    public QueryThenFetch(ExecutionPlan subExecutionPlan, FetchPhase fetchPhase) {
        this.subExecutionPlan = subExecutionPlan;
        this.fetchPhase = fetchPhase;
    }

    public FetchPhase fetchPhase() {
        return fetchPhase;
    }

    public ExecutionPlan subPlan() {
        return subExecutionPlan;
    }

    @Override
    public <C, R> R accept(ExecutionPlanVisitor<C, R> visitor, C context) {
        return visitor.visitQueryThenFetch(this, context);
    }

    @Override
    public void addProjection(Projection projection) {
        subExecutionPlan.addProjection(projection);
    }

    @Override
    public void addProjection(Projection projection,
                              int unfinishedLimit,
                              int unfinishedOffset,
                              @Nullable PositionalOrderBy unfinishedOrderBy) {
        subExecutionPlan.addProjection(projection, unfinishedLimit, unfinishedOffset, unfinishedOrderBy);
    }

    @Override
    public ResultDescription resultDescription() {
        return subExecutionPlan.resultDescription();
    }

    @Override
    public void setDistributionInfo(DistributionInfo distributionInfo) {
        subExecutionPlan.setDistributionInfo(distributionInfo);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        QueryThenFetch that = (QueryThenFetch) o;
        return Objects.equals(fetchPhase, that.fetchPhase) && Objects.equals(subExecutionPlan, that.subExecutionPlan);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fetchPhase, subExecutionPlan);
    }
}
