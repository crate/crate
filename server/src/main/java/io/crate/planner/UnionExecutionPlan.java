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

package io.crate.planner;

import io.crate.execution.dsl.projection.LimitAndOffsetProjection;
import io.crate.planner.distribution.DistributionInfo;
import io.crate.execution.dsl.phases.MergePhase;
import io.crate.execution.dsl.projection.Projection;
import io.crate.types.DataType;

import org.jspecify.annotations.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

/**
 * Plan for Union which uses a MergePhase to combine the results of two plans (= two inputs).
 */
public class UnionExecutionPlan implements ExecutionPlan, ResultDescription {

    private final ExecutionPlan left;
    private final ExecutionPlan right;

    private final MergePhase mergePhase;

    private int unfinishedLimit;
    private int unfinishedOffset;
    private int numOutputs;

    private final int maxRowsPerNode;

    @Nullable
    private PositionalOrderBy orderBy;

    /**
     * Create a Union Plan
     *
     * @param unfinishedLimit the limit a parent must apply after a merge to get the correct result
     * @param unfinishedOffset the offset a parent must apply after a merge to get the correct result
     * <p>
     * If the data should be limited as part of the Merge, add a {@link LimitAndOffsetProjection},
     * if possible. If the limit of the {@link LimitAndOffsetProjection} is final, unfinishedLimit here
     * should be set to NO_LIMIT (-1)
     * </p>
     * <p>
     * See also: {@link ResultDescription}
     * </p>
     */
    public UnionExecutionPlan(ExecutionPlan left,
                              ExecutionPlan right,
                              MergePhase mergePhase,
                              int unfinishedLimit,
                              int unfinishedOffset,
                              int numOutputs,
                              int maxRowsPerNode,
                              @Nullable PositionalOrderBy orderBy) {
        this.left = left;
        this.right = right;
        if (mergePhase.numInputs() != 2) {
            throw new IllegalArgumentException("Number of inputs of MergePhase needs to be two.");
        }
        this.mergePhase = mergePhase;
        this.unfinishedLimit = unfinishedLimit;
        this.unfinishedOffset = unfinishedOffset;
        this.numOutputs = numOutputs;
        this.maxRowsPerNode = maxRowsPerNode;
        this.orderBy = orderBy;
    }

    public MergePhase mergePhase() {
        return mergePhase;
    }

    public ExecutionPlan left() {
        return left;
    }

    public ExecutionPlan right() {
        return right;
    }

    @Override
    public <C, R> R accept(ExecutionPlanVisitor<C, R> visitor, C context) {
        return visitor.visitUnionPlan(this, context);
    }

    @Override
    public void addProjection(Projection projection) {
        mergePhase.addProjection(projection);
        numOutputs = projection.outputs().size();
    }

    @Override
    public void addProjection(Projection projection,
                              int unfinishedLimit,
                              int unfinishedOffset,
                              @Nullable PositionalOrderBy unfinishedOrderBy) {
        addProjection(projection);
        this.unfinishedLimit = unfinishedLimit;
        this.unfinishedOffset = unfinishedOffset;
        this.orderBy = unfinishedOrderBy;
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
        return orderBy;
    }

    @Override
    public int limit() {
        return unfinishedLimit;
    }

    @Override
    public int maxRowsPerNode() {
        return maxRowsPerNode;
    }

    @Override
    public int offset() {
        return unfinishedOffset;
    }

    @Override
    public int numOutputs() {
        return numOutputs;
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
        UnionExecutionPlan that = (UnionExecutionPlan) o;
        return unfinishedLimit == that.unfinishedLimit &&
               unfinishedOffset == that.unfinishedOffset &&
               numOutputs == that.numOutputs &&
               maxRowsPerNode == that.maxRowsPerNode &&
               Objects.equals(left, that.left) &&
               Objects.equals(right, that.right) &&
               Objects.equals(mergePhase, that.mergePhase) &&
               Objects.equals(orderBy, that.orderBy);
    }

    @Override
    public int hashCode() {
        return Objects.hash(left, right, mergePhase, unfinishedLimit, unfinishedOffset, numOutputs,
            maxRowsPerNode, orderBy);
    }
}
