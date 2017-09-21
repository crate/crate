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

import io.crate.operation.Paging;
import io.crate.operation.projectors.TopN;
import io.crate.planner.distribution.DistributionInfo;
import io.crate.planner.node.ExecutionPhases;
import io.crate.planner.node.dql.MergePhase;
import io.crate.planner.projection.Projection;
import io.crate.planner.projection.builder.ProjectionBuilder;
import io.crate.types.DataType;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

public class Merge implements Plan, ResultDescription {

    private final Plan subPlan;
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
     *
     *                    If the subPlan contains a limit/offset or orderBy in its resultDescription this method will
     *                    add a TopNProjection AFTER the projections.
     */
    public static Plan ensureOnHandler(Plan subPlan, Planner.Context plannerContext, List<Projection> projections) {
        ResultDescription resultDescription = subPlan.resultDescription();
        assert resultDescription != null : "all plans must have a result description. Plan without: " + subPlan;

        // If a sub-Plan applies a limit it is usually limit+offset
        // So even if the execution is on the handler the limit may be too large and the final limit needs to be applied as well
        Projection topN = ProjectionBuilder.topNOrEvalIfNeeded(
            resultDescription.limit(),
            resultDescription.offset(),
            resultDescription.numOutputs(),
            resultDescription.streamOutputs());
        if (ExecutionPhases.executesOnHandler(plannerContext.handlerNode(), resultDescription.nodeIds())) {
            return addProjections(subPlan, projections, resultDescription, topN);
        }
        maybeUpdatePageSizeHint(subPlan, resultDescription.maxRowsPerNode());
        Collection<String> handlerNodeIds = Collections.singletonList(plannerContext.handlerNode());

        MergePhase mergePhase = new MergePhase(
            plannerContext.jobId(),
            plannerContext.nextExecutionPhaseId(),
            "mergeOnHandler",
            resultDescription.nodeIds().size(),
            handlerNodeIds,
            resultDescription.streamOutputs(),
            addProjection(projections, topN),
            DistributionInfo.DEFAULT_SAME_NODE,
            resultDescription.orderBy()
        );
        return new Merge(
            subPlan,
            mergePhase,
            TopN.NO_LIMIT,
            0,
            resultDescription.numOutputs(),
            resultDescription.limit(),
            resultDescription.orderBy()
        );
    }

    private static void maybeUpdatePageSizeHint(Plan subPlan, int maxRowsPerNode) {
        if (Paging.shouldPage(maxRowsPerNode)) {
            Paging.updateNodePageSizeHint(subPlan, maxRowsPerNode);
        }
    }

    private static Plan addProjections(Plan subPlan,
                                       List<Projection> projections,
                                       ResultDescription resultDescription,
                                       Projection topN) {
        for (Projection projection : projections) {
            assert projection.outputs().size() == resultDescription.numOutputs()
                : "projection must not affect numOutputs";
            subPlan.addProjection(projection);
        }
        if (topN != null) {
            subPlan.addProjection(topN, TopN.NO_LIMIT, 0, null);
        }
        // resultDescription.orderBy can be ignored here because it is only relevant to do a sorted merge
        // (of a pre-sorted result)
        // In this case there is only one node/bucket which is already correctly sorted
        return subPlan;
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

    public static Plan ensureOnHandler(Plan subPlan, Planner.Context plannerContext) {
        return ensureOnHandler(subPlan, plannerContext, Collections.emptyList());
    }

    /**
     * Create a Merge Plan
     *
     * @param unfinishedLimit the limit a parent must apply after a merge to get the correct result
     * @param unfinishedOffset the offset a parent must apply after a merge to get the correct result
     *
     * If the data should be limited as part of the Merge, add a {@link io.crate.planner.projection.TopNProjection},
     * if possible. If the limit of the TopNProjection is final, unfinishedLimit here should be set to NO_LIMIT (-1)
     *
     * See also: {@link ResultDescription}
     */
    public Merge(Plan subPlan,
                 MergePhase mergePhase,
                 int unfinishedLimit,
                 int unfinishedOffset,
                 int numOutputs,
                 int maxRowsPerNode,
                 @Nullable PositionalOrderBy orderBy) {
        this.subPlan = subPlan;
        this.mergePhase = mergePhase;
        this.unfinishedLimit = unfinishedLimit;
        this.unfinishedOffset = unfinishedOffset;
        this.numOutputs = numOutputs;
        this.maxRowsPerNode = maxRowsPerNode;
        this.orderBy = orderBy;
    }

    @Override
    public <C, R> R accept(PlanVisitor<C, R> visitor, C context) {
        return visitor.visitMerge(this, context);
    }

    @Override
    public UUID jobId() {
        return subPlan.jobId();
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

    public Plan subPlan() {
        return subPlan;
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
    public List<DataType> streamOutputs() {
        return mergePhase.outputTypes();
    }

}
