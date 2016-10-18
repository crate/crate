/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

import io.crate.operation.projectors.TopN;
import io.crate.planner.Plan;
import io.crate.planner.PlanVisitor;
import io.crate.planner.PositionalOrderBy;
import io.crate.planner.ResultDescription;
import io.crate.planner.distribution.DistributionInfo;
import io.crate.planner.projection.Projection;
import io.crate.types.DataType;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

public class CountPlan implements Plan, ResultDescription {

    private final CountPhase countPhase;
    private final MergePhase mergePhase;
    private final UUID id;

    public CountPlan(CountPhase countPhase, MergePhase mergePhase) {
        this.countPhase = countPhase;
        this.mergePhase = mergePhase;
        this.id = mergePhase.jobId();
    }

    public CountPhase countPhase() {
        return countPhase;
    }

    public MergePhase mergePhase() {
        return mergePhase;
    }

    @Override
    public <C, R> R accept(PlanVisitor<C, R> visitor, C context) {
        return visitor.visitCountPlan(this, context);
    }

    @Override
    public UUID jobId() {
        return id;
    }

    @Override
    public void addProjection(Projection projection,
                              @Nullable Integer newLimit,
                              @Nullable Integer newOffset,
                              @Nullable Integer newNumOutputs,
                              @Nullable PositionalOrderBy newOrderBy) {
        mergePhase.addProjection(projection);
    }

    @Override
    public ResultDescription resultDescription() {
        return this;
    }

    @Override
    public void setDistributionInfo(DistributionInfo distributionInfo) {
        mergePhase.distributionInfo(distributionInfo);
    }

    @Override
    public Collection<String> nodeIds() {
        return mergePhase.nodeIds();
    }

    @Nullable
    @Override
    public PositionalOrderBy orderBy() {
        return null;
    }

    @Override
    public int limit() {
        return TopN.NO_LIMIT;
    }

    @Override
    public int maxRowsPerNode() {
        return 1;
    }

    @Override
    public int offset() {
        return 0;
    }

    @Override
    public int numOutputs() {
        return mergePhase.outputTypes.size();
    }

    @Override
    public List<DataType> streamOutputs() {
        return mergePhase.outputTypes();
    }
}
