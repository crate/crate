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

import io.crate.execution.dsl.phases.CountPhase;
import io.crate.execution.dsl.phases.MergePhase;
import io.crate.execution.engine.pipeline.LimitAndOffset;
import io.crate.planner.ExecutionPlan;
import io.crate.planner.ExecutionPlanVisitor;
import io.crate.planner.PositionalOrderBy;
import io.crate.planner.ResultDescription;
import io.crate.planner.distribution.DistributionInfo;
import io.crate.execution.dsl.projection.Projection;
import io.crate.types.DataType;

import org.jspecify.annotations.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

public class CountPlan implements ExecutionPlan, ResultDescription {

    private final CountPhase countPhase;
    private final MergePhase mergePhase;

    private int unfinishedLimit = LimitAndOffset.NO_LIMIT;
    private int unfinishedOffset = 0;

    @Nullable
    private PositionalOrderBy unfinishedOrderBy = null;

    public CountPlan(CountPhase countPhase, MergePhase mergePhase) {
        this.countPhase = countPhase;
        this.mergePhase = mergePhase;
    }

    public CountPhase countPhase() {
        return countPhase;
    }

    public MergePhase mergePhase() {
        return mergePhase;
    }

    @Override
    public <C, R> R accept(ExecutionPlanVisitor<C, R> visitor, C context) {
        return visitor.visitCountPlan(this, context);
    }

    @Override
    public void addProjection(Projection projection) {
        mergePhase.addProjection(projection);
    }

    @Override
    public void addProjection(Projection projection,
                              int unfinishedLimit,
                              int unfinishedOffset,
                              @Nullable PositionalOrderBy unfinishedOrderBy) {
        mergePhase.addProjection(projection);
        this.unfinishedLimit = unfinishedLimit;
        this.unfinishedOffset = unfinishedOffset;
        this.unfinishedOrderBy = unfinishedOrderBy;
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
        return unfinishedOrderBy;
    }

    @Override
    public int limit() {
        return unfinishedLimit;
    }

    @Override
    public int maxRowsPerNode() {
        return 1;
    }

    @Override
    public int offset() {
        return unfinishedOffset;
    }

    @Override
    public int numOutputs() {
        return mergePhase.outputTypes().size();
    }

    @Override
    public List<DataType<?>> streamOutputs() {
        return mergePhase.outputTypes();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CountPlan countPlan = (CountPlan) o;
        return unfinishedLimit == countPlan.unfinishedLimit &&
               unfinishedOffset == countPlan.unfinishedOffset &&
               Objects.equals(countPhase, countPlan.countPhase) &&
               Objects.equals(mergePhase, countPlan.mergePhase) &&
               Objects.equals(unfinishedOrderBy, countPlan.unfinishedOrderBy);
    }

    @Override
    public int hashCode() {
        return Objects.hash(countPhase, mergePhase, unfinishedLimit, unfinishedOffset, unfinishedOrderBy);
    }
}
