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

package io.crate.planner.node.dql.join;

import io.crate.execution.dsl.phases.JoinPhase;
import io.crate.execution.dsl.projection.Projection;
import io.crate.planner.ExecutionPlan;
import io.crate.planner.ExecutionPlanVisitor;
import io.crate.planner.PositionalOrderBy;
import io.crate.planner.ResultDescription;
import io.crate.planner.distribution.DistributionInfo;
import io.crate.types.DataType;

import org.jspecify.annotations.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

/**
 * Plan that will execute a join.
 * The join can be executed either with NestedLoop or HashJoin algorithms
 * <p>
 * This Plan makes a lot of assumptions:
 * <p>
 * <ul>
 * <li> limit and offset are already pushed down to left and right plan nodes
 * <li> where clause is already splitted to left and right plan nodes
 * <li> order by symbols are already splitted, too
 * <p>
 * </ul>
 * <p>
 * Properties:
 * <p>
 * <ul>
 * <li> the resulting outputs from the join operations are the same, so the projections
 * added can assume the same order of symbols, first symbols from left, then from right.
 * If sth. else is selected a projection has to reorder those.
 */
public class Join implements ExecutionPlan, ResultDescription {

    private final ExecutionPlan left;
    private final ExecutionPlan right;
    private final JoinPhase joinPhase;

    private int limit;
    private int offset;
    private int numOutputs;

    private final int maxRowsPerNode;
    @Nullable
    private PositionalOrderBy orderBy;

    public Join(JoinPhase joinPhase,
                ExecutionPlan left,
                ExecutionPlan right,
                int limit,
                int offset,
                int maxRowsPerNode,
                int numOutputs,
                @Nullable PositionalOrderBy orderBy) {
        this.left = left;
        this.right = right;
        this.joinPhase = joinPhase;
        this.limit = limit;
        this.offset = offset;
        this.maxRowsPerNode = maxRowsPerNode;
        this.orderBy = orderBy;
        this.numOutputs = numOutputs;
    }

    public ExecutionPlan left() {
        return left;
    }

    public ExecutionPlan right() {
        return right;
    }

    public JoinPhase joinPhase() {
        return joinPhase;
    }

    @Override
    public ResultDescription resultDescription() {
        return this;
    }

    @Override
    public void setDistributionInfo(DistributionInfo distributionInfo) {
        joinPhase.distributionInfo(distributionInfo);
    }

    @Override
    public <C, R> R accept(ExecutionPlanVisitor<C, R> visitor, C context) {
        return visitor.visitJoin(this, context);
    }

    @Override
    public void addProjection(Projection projection) {
        joinPhase.addProjection(projection);
        numOutputs = projection.outputs().size();
    }

    @Override
    public void addProjection(Projection projection,
                              int unfinishedLimit,
                              int unfinishedOffset,
                              @Nullable PositionalOrderBy unfinishedOrderBy) {
        joinPhase.addProjection(projection);
        limit = unfinishedLimit;
        offset = unfinishedOffset;
        orderBy = unfinishedOrderBy;
        numOutputs = projection.outputs().size();
    }

    @Override
    public Collection<String> nodeIds() {
        return joinPhase.nodeIds();
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
    public List<DataType<?>> streamOutputs() {
        return joinPhase.outputTypes();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Join join = (Join) o;
        return limit == join.limit &&
               offset == join.offset &&
               numOutputs == join.numOutputs &&
               maxRowsPerNode == join.maxRowsPerNode &&
               Objects.equals(left, join.left) &&
               Objects.equals(right, join.right) &&
               Objects.equals(joinPhase, join.joinPhase) &&
               Objects.equals(orderBy, join.orderBy);
    }

    @Override
    public int hashCode() {
        return Objects.hash(left, right, joinPhase, limit, offset, numOutputs, maxRowsPerNode, orderBy);
    }
}
