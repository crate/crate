/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.planner.node.dql;

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

public class DistributedGroupBy implements Plan, ResultDescription {

    private final RoutedCollectPhase collectPhase;
    private final MergePhase reducerMergeNode;

    private int limit;
    private int offset;
    private int numOutputs;

    private final int maxRowsPerNode;
    @Nullable
    private PositionalOrderBy orderBy;

    public DistributedGroupBy(RoutedCollectPhase collectPhase,
                              MergePhase reducerMergeNode,
                              int limit,
                              int offset,
                              int numOutputs,
                              int maxRowsPerNode,
                              @Nullable PositionalOrderBy orderBy) {
        this.collectPhase = collectPhase;
        this.reducerMergeNode = reducerMergeNode;
        this.limit = limit;
        this.offset = offset;
        this.numOutputs = numOutputs;
        this.maxRowsPerNode = maxRowsPerNode;
        this.orderBy = orderBy;
    }

    @Override
    public <C, R> R accept(PlanVisitor<C, R> visitor, C context) {
        return visitor.visitDistributedGroupBy(this, context);
    }

    @Override
    public UUID jobId() {
        return collectPhase.jobId();
    }

    public RoutedCollectPhase collectPhase() {
        return collectPhase;
    }

    public MergePhase reducerMergeNode() {
        return reducerMergeNode;
    }

    @Override
    public void addProjection(Projection projection,
                              @Nullable Integer newLimit,
                              @Nullable Integer newOffset,
                              @Nullable Integer newNumOutputs,
                              @Nullable PositionalOrderBy newOrderBy) {
        reducerMergeNode.addProjection(projection);
        if (newLimit != null) {
            limit = newLimit;
        }
        if (newOffset != null) {
            offset = newOffset;
        }
        if (newOrderBy != null) {
            orderBy = newOrderBy;
        }
        if (newNumOutputs != null) {
            numOutputs = newNumOutputs;
        }
    }

    @Override
    public ResultDescription resultDescription() {
        return this;
    }

    @Override
    public void setDistributionInfo(DistributionInfo distributionInfo) {
        reducerMergeNode.distributionInfo(distributionInfo);
    }

    @Override
    public Collection<String> nodeIds() {
        return reducerMergeNode.nodeIds();
    }

    @Nullable
    @Override
    public PositionalOrderBy orderBy() {
        return orderBy;
    }

    @Override
    public int limit() {
        return limit;
    }

    @Override
    public int maxRowsPerNode() {
        return maxRowsPerNode;
    }

    @Override
    public int offset() {
        return offset;
    }

    @Override
    public int numOutputs() {
        return numOutputs;
    }

    @Override
    public List<DataType> streamOutputs() {
        return reducerMergeNode.outputTypes();
    }
}
