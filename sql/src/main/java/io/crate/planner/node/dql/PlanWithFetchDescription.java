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

package io.crate.planner.node.dql;

import io.crate.planner.Plan;
import io.crate.planner.PlanVisitor;
import io.crate.planner.PositionalOrderBy;
import io.crate.planner.ResultDescription;
import io.crate.planner.distribution.DistributionInfo;
import io.crate.planner.fetch.FetchRewriter;
import io.crate.planner.projection.Projection;
import io.crate.types.DataType;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.UUID;


public class PlanWithFetchDescription implements Plan, ResultDescription {

    private final Plan subPlan;
    private final FetchRewriter.FetchDescription fetchDescription;

    public PlanWithFetchDescription(Plan subPlan, FetchRewriter.FetchDescription fetchDescription) {
        this.subPlan = subPlan;
        this.fetchDescription = fetchDescription;
    }

    @Override
    public <C, R> R accept(PlanVisitor<C, R> visitor, C context) {
        return subPlan.accept(visitor, context);
    }

    @Override
    public UUID jobId() {
        return subPlan.jobId();
    }

    @Override
    public void addProjection(Projection projection) {
        subPlan.addProjection(projection);
    }

    @Override
    public void addProjection(Projection projection,
                              int unfinishedLimit,
                              int unfinishedOffset,
                              @Nullable PositionalOrderBy unfinishedOrderBy) {
        subPlan.addProjection(projection, unfinishedLimit, unfinishedOffset, unfinishedOrderBy);
    }

    @Override
    public ResultDescription resultDescription() {
        return this;
    }

    @Override
    public void setDistributionInfo(DistributionInfo distributionInfo) {
        subPlan.setDistributionInfo(distributionInfo);
    }

    @Override
    public Collection<String> nodeIds() {
        return subPlan.resultDescription().nodeIds();
    }

    @Nullable
    @Override
    public PositionalOrderBy orderBy() {
        return subPlan.resultDescription().orderBy();
    }

    @Override
    public int limit() {
        return subPlan.resultDescription().limit();
    }

    @Override
    public int maxRowsPerNode() {
        return subPlan.resultDescription().maxRowsPerNode();
    }

    @Override
    public int offset() {
        return subPlan.resultDescription().offset();
    }

    @Override
    public int numOutputs() {
        return subPlan.resultDescription().numOutputs();
    }

    @Override
    public List<DataType> streamOutputs() {
        return subPlan.resultDescription().streamOutputs();
    }

    @Override
    public FetchRewriter.FetchDescription fetchDescription() {
        return fetchDescription;
    }

    public Plan subPlan() {
        return subPlan;
    }
}
