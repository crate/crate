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

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import org.jetbrains.annotations.Nullable;

import io.crate.data.Paging;
import io.crate.execution.dsl.phases.CollectPhase;
import io.crate.execution.dsl.phases.ExecutionPhases;
import io.crate.execution.dsl.phases.MergePhase;
import io.crate.execution.dsl.phases.RoutedCollectPhase;
import io.crate.execution.dsl.projection.LimitAndOffsetProjection;
import io.crate.execution.dsl.projection.Projection;
import io.crate.execution.dsl.projection.builder.ProjectionBuilder;
import io.crate.execution.engine.pipeline.LimitAndOffset;
import io.crate.planner.distribution.DistributionInfo;
import io.crate.planner.node.dql.Collect;
import io.crate.types.DataType;

public class Merge implements ExecutionPlan, ResultDescription {

    private final ExecutionPlan subExecutionPlan;
    private final MergePhase mergePhase;

    private int unfinishedLimit;
    private int unfinishedOffset;
    private int numOutputs;

    private final int maxRowsPerNode;
    @Nullable
    private PositionalOrderBy orderBy;

    /**
     * Wrap the subPlan into a Merge plan if it isn't executed on the handler.
     * @param projections projections to be applied on the subPlan or merge plan.
     *                    These projections must not affect the limit, offset, order by or numOutputs
     * <p>
     * If the subPlan contains a limit/offset or orderBy in its resultDescription this method will
     * add a {@link LimitAndOffset} AFTER the projections.
     */
    public static ExecutionPlan ensureOnHandler(ExecutionPlan subExecutionPlan, PlannerContext plannerContext, List<Projection> projections) {
        ResultDescription resultDescription = subExecutionPlan.resultDescription();
        assert resultDescription != null : "all plans must have a result description. Plan without: " +
                                           subExecutionPlan;

        // If a sub-Plan applies a limit it is usually limit+offset
        // So even if the execution is on the handler the limit may be too large and the final limit needs to be applied as well
        Projection limitAndOffset = ProjectionBuilder.limitAndOffsetOrEvalIfNeeded(
            resultDescription.limit(),
            resultDescription.offset(),
            resultDescription.numOutputs(),
            resultDescription.streamOutputs());
        if (ExecutionPhases.executesOnHandler(plannerContext.handlerNode(), resultDescription.nodeIds())) {
            return addProjections(subExecutionPlan, projections, resultDescription, limitAndOffset);
        }
        maybeUpdatePageSizeHint(subExecutionPlan, resultDescription.maxRowsPerNode());
        Collection<String> handlerNodeIds = Collections.singletonList(plannerContext.handlerNode());

        MergePhase mergePhase = new MergePhase(
            plannerContext.jobId(),
            plannerContext.nextExecutionPhaseId(),
            "mergeOnHandler",
            resultDescription.nodeIds().size(),
            1,
            handlerNodeIds,
            resultDescription.streamOutputs(),
            addProjection(projections, limitAndOffset),
            resultDescription.nodeIds(),
            DistributionInfo.DEFAULT_BROADCAST,
            resultDescription.orderBy()
        );
        return new Merge(
            subExecutionPlan,
            mergePhase,
            LimitAndOffset.NO_LIMIT,
            0,
            resultDescription.numOutputs(),
            resultDescription.limit(),
            resultDescription.orderBy()
        );
    }

    private static void maybeUpdatePageSizeHint(ExecutionPlan subExecutionPlan, int maxRowsPerNode) {
        if (Paging.shouldPage(maxRowsPerNode)) {
            updateNodePageSizeHint(subExecutionPlan, maxRowsPerNode);
        }
    }

    private static void updateNodePageSizeHint(ExecutionPlan subExecutionPlan, int nodePageSize) {
        if (!(subExecutionPlan instanceof Collect) || nodePageSize == -1) {
            return;
        }
        CollectPhase collectPhase = ((Collect) subExecutionPlan).collectPhase();
        if (collectPhase instanceof RoutedCollectPhase) {
            ((RoutedCollectPhase) collectPhase).pageSizeHint(nodePageSize);
        }
    }

    private static ExecutionPlan addProjections(ExecutionPlan subExecutionPlan,
                                                List<Projection> projections,
                                                ResultDescription resultDescription,
                                                Projection limitAndOffset) {
        for (Projection projection : projections) {
            assert projection.outputs().size() == resultDescription.numOutputs()
                : "projection must not affect numOutputs";
            subExecutionPlan.addProjection(projection);
        }
        if (limitAndOffset != null) {
            subExecutionPlan.addProjection(limitAndOffset, LimitAndOffset.NO_LIMIT, 0, resultDescription.orderBy());
        }
        return subExecutionPlan;
    }

    private static List<Projection> addProjection(List<Projection> projections, @Nullable Projection projection) {
        if (projection == null) {
            return projections;
        }
        if (projections.isEmpty()) {
            return Collections.singletonList(projection);
        } else {
            projections.add(projection);
            return projections;
        }
    }

    public static ExecutionPlan ensureOnHandler(ExecutionPlan subExecutionPlan, PlannerContext plannerContext) {
        return ensureOnHandler(subExecutionPlan, plannerContext, Collections.emptyList());
    }

    /**
     * Create a Merge Plan
     *
     * @param unfinishedLimit the limit a parent must apply after a merge to get the correct result
     * @param unfinishedOffset the offset a parent must apply after a merge to get the correct result
     * <p>
     * If the data should be limited as part of the Merge, add a {@link LimitAndOffsetProjection},
     * if possible. If the limit of the {@link LimitAndOffsetProjection} is final,
     * unfinishedLimit here should be set to NO_LIMIT (-1)
     * <p>
     * See also: {@link ResultDescription}
     */
    public Merge(ExecutionPlan subExecutionPlan,
                 MergePhase mergePhase,
                 int unfinishedLimit,
                 int unfinishedOffset,
                 int numOutputs,
                 int maxRowsPerNode,
                 @Nullable PositionalOrderBy orderBy) {
        this.subExecutionPlan = subExecutionPlan;
        this.mergePhase = mergePhase;
        this.unfinishedLimit = unfinishedLimit;
        this.unfinishedOffset = unfinishedOffset;
        this.numOutputs = numOutputs;
        this.maxRowsPerNode = maxRowsPerNode;
        this.orderBy = orderBy;
    }

    @Override
    public <C, R> R accept(ExecutionPlanVisitor<C, R> visitor, C context) {
        return visitor.visitMerge(this, context);
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

    public MergePhase mergePhase() {
        return mergePhase;
    }

    public ExecutionPlan subPlan() {
        return subExecutionPlan;
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
        Merge merge = (Merge) o;
        return unfinishedLimit == merge.unfinishedLimit &&
               unfinishedOffset == merge.unfinishedOffset &&
               numOutputs == merge.numOutputs &&
               maxRowsPerNode == merge.maxRowsPerNode &&
               Objects.equals(subExecutionPlan, merge.subExecutionPlan) &&
               Objects.equals(mergePhase, merge.mergePhase) &&
               Objects.equals(orderBy, merge.orderBy);
    }

    @Override
    public int hashCode() {
        return Objects.hash(subExecutionPlan, mergePhase, unfinishedLimit, unfinishedOffset, numOutputs, maxRowsPerNode, orderBy);
    }
}
