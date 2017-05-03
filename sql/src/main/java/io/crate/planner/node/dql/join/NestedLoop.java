/*
* Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
* license agreements. See the NOTICE file distributed with this work for
* additional information regarding copyright ownership. Crate licenses
* this file to you under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License. You may
* obtain a copy of the License at
*
*   http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
* WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
* License for the specific language governing permissions and limitations
* under the License.
*
* However, if you have executed another commercial license agreement
* with Crate these terms will supersede the license and you may use the
* software solely pursuant to the terms of the relevant commercial agreement.
*/
package io.crate.planner.node.dql.join;

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

/**
 * Plan that will be executed with the awesome nested loop algorithm
 * performing CROSS JOINs
 * <p>
 * This Plan makes a lot of assumptions:
 * <p>
 * <ul>
 * <li> limit and offset are already pushed down to left and right plan nodes
 * <li> where clause is already splitted to left and right plan nodes
 * <li> order by symbols are already splitted, too
 * <li> if the first order by symbol in the whole statement is from the left node,
 * set <code>leftOuterLoop</code> to true, otherwise to false
 * <p>
 * </ul>
 * <p>
 * Properties:
 * <p>
 * <ul>
 * <li> the resulting outputs from the join operations are the same, no matter if
 * <code>leftOuterLoop</code> is true or not - so the projections added,
 * can assume the same order of symbols, first symbols from left, then from right.
 * If sth. else is selected a projection has to reorder those.
 */
public class NestedLoop implements Plan, ResultDescription {

    private final Plan left;
    private final Plan right;
    private final NestedLoopPhase nestedLoopPhase;

    private int limit;
    private int offset;
    private int numOutputs;

    private final int maxRowsPerNode;
    @Nullable
    private PositionalOrderBy orderBy;
    private final UUID jobId;

    /**
     * create a new NestedLoop
     * <p>
     * side in the outer loop, the right in the inner.
     * Resulting in rows like:
     * <p>
     * a | 1
     * a | 2
     * a | 3
     * b | 1
     * b | 2
     * b | 3
     * <p>
     * This is the case if the left relation is referenced
     * by the first order by symbol references. E.g.
     * for <code>ORDER BY left.a, right.b</code>
     * If false, the nested loop is executed the other way around.
     * With the following results:
     * <p>
     * a | 1
     * b | 1
     * a | 2
     * b | 2
     * a | 3
     * b | 3
     */
    public NestedLoop(NestedLoopPhase nestedLoopPhase,
                      Plan left,
                      Plan right,
                      int limit,
                      int offset,
                      int maxRowsPerNode,
                      int numOutputs,
                      @Nullable PositionalOrderBy orderBy) {
        this.jobId = nestedLoopPhase.jobId();
        this.left = left;
        this.right = right;
        this.nestedLoopPhase = nestedLoopPhase;
        this.limit = limit;
        this.offset = offset;
        this.maxRowsPerNode = maxRowsPerNode;
        this.orderBy = orderBy;
        this.numOutputs = numOutputs;
    }

    public Plan left() {
        return left;
    }

    public Plan right() {
        return right;
    }

    public NestedLoopPhase nestedLoopPhase() {
        return nestedLoopPhase;
    }

    @Override
    public ResultDescription resultDescription() {
        return this;
    }

    @Override
    public void setDistributionInfo(DistributionInfo distributionInfo) {
        nestedLoopPhase.distributionInfo(distributionInfo);
    }

    @Override
    public <C, R> R accept(PlanVisitor<C, R> visitor, C context) {
        return visitor.visitNestedLoop(this, context);
    }

    @Override
    public UUID jobId() {
        return jobId;
    }

    @Override
    public void addProjection(Projection projection,
                              @Nullable Integer newLimit,
                              @Nullable Integer newOffset,
                              @Nullable PositionalOrderBy newOrderBy) {
        nestedLoopPhase.addProjection(projection);
        if (newLimit != null) {
            limit = newLimit;
        }
        if (newOffset != null) {
            offset = newOffset;
        }
        if (newOrderBy != null) {
            orderBy = newOrderBy;
        }
        numOutputs = projection.outputs().size();
    }

    @Override
    public Collection<String> nodeIds() {
        return nestedLoopPhase.nodeIds();
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
        return nestedLoopPhase.outputTypes();
    }
}
