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

package io.crate.planner.operators;

import static io.crate.execution.engine.pipeline.LimitAndOffset.NO_LIMIT;
import static io.crate.planner.operators.Limit.limitAndOffset;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SequencedCollection;
import java.util.Set;

import org.jetbrains.annotations.Nullable;

import io.crate.analyze.OrderBy;
import io.crate.common.collections.Lists;
import io.crate.common.collections.Maps;
import io.crate.common.collections.Tuple;
import io.crate.data.Row;
import io.crate.execution.dsl.phases.MergePhase;
import io.crate.execution.dsl.phases.NestedLoopPhase;
import io.crate.execution.dsl.projection.EvalProjection;
import io.crate.execution.dsl.projection.Projection;
import io.crate.execution.dsl.projection.builder.InputColumns;
import io.crate.execution.dsl.projection.builder.ProjectionBuilder;
import io.crate.expression.symbol.SelectSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.Symbols;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.ExecutionPlan;
import io.crate.planner.PlannerContext;
import io.crate.planner.PositionalOrderBy;
import io.crate.planner.ResultDescription;
import io.crate.planner.distribution.DistributionInfo;
import io.crate.planner.node.dql.join.Join;
import io.crate.sql.tree.JoinType;

public class NestedLoopJoin extends AbstractJoinPlan {

    private final boolean isFiltered;
    private boolean orderByWasPushedDown = false;
    // this can be removed
    private boolean rewriteNestedLoopJoinToHashJoinDone = false;

    public NestedLoopJoin(LogicalPlan lhs,
                   LogicalPlan rhs,
                   JoinType joinType,
                   @Nullable Symbol joinCondition) {
        super(buildOutputs(lhs.outputs(), rhs.outputs(), joinType), lhs, rhs, joinCondition, joinType);
        this.isFiltered = joinCondition != null;
    }

    public NestedLoopJoin(List<Symbol> outputs,
                          LogicalPlan lhs,
                          LogicalPlan rhs,
                          JoinType joinType,
                          @Nullable Symbol joinCondition,
                          boolean isFiltered,
                          boolean orderByWasPushedDown,
                          boolean rewriteEquiJoinToHashJoinDone) {
        super(outputs, lhs, rhs, joinCondition, joinType);
        this.isFiltered = isFiltered || joinCondition != null;
        this.orderByWasPushedDown = orderByWasPushedDown;
        this.rewriteNestedLoopJoinToHashJoinDone = rewriteEquiJoinToHashJoinDone;
    }

    public boolean isRewriteNestedLoopJoinToHashJoinDone() {
        return rewriteNestedLoopJoinToHashJoinDone;
    }

    public boolean isFiltered() {
        return isFiltered;
    }

    @Override
    public Map<LogicalPlan, SelectSymbol> dependencies() {
        return Maps.concat(lhs.dependencies(), rhs.dependencies());
    }

    @Override
    public ExecutionPlan build(DependencyCarrier executor,
                               PlannerContext plannerContext,
                               Set<PlanHint> hints,
                               ProjectionBuilder projectionBuilder,
                               int limit,
                               int offset,
                               @Nullable OrderBy order,
                               @Nullable Integer pageSizeHint,
                               Row params,
                               SubQueryResults subQueryResults) {
        /*
         * Benchmarks reveal that if rows are filtered out distributed execution gives better performance.
         * Therefore if `filterNeeded` is true (there is joinCondition or a filtering after the join operation)
         * then it's a good indication that distributed execution will be faster.
         *
         * We may at some point add some kind of session-settings to override this behaviour
         * or otherwise come up with a better heuristic.
         */
        Integer childPageSizeHint = !isFiltered && limit != NO_LIMIT
            ? limitAndOffset(limit, offset)
            : null;

        ExecutionPlan left = lhs.build(
            executor, plannerContext, hints, projectionBuilder, NO_LIMIT, 0, null, childPageSizeHint, params, subQueryResults);
        ExecutionPlan right = rhs.build(
            executor, plannerContext, hints, projectionBuilder, NO_LIMIT, 0, null, childPageSizeHint, params, subQueryResults);

        boolean isDistributed = supportsDistributedReads() && isFiltered && !joinType.isOuter();

        LogicalPlan leftLogicalPlan = lhs;
        LogicalPlan rightLogicalPlan = rhs;
        isDistributed = isDistributed &&
                        (!left.resultDescription().nodeIds().isEmpty() && !right.resultDescription().nodeIds().isEmpty());
        boolean blockNlPossible = !isDistributed && isBlockNlPossible(left, right);

        JoinType joinType = this.joinType;
        var lhStats = plannerContext.planStats().get(lhs);
        var rhStats = plannerContext.planStats().get(rhs);
        boolean expectedRowsAvailable = lhStats.numDocs() != -1 && rhStats.numDocs() != -1;
        if (expectedRowsAvailable) {
            if (!orderByWasPushedDown && joinType.supportsInversion() &&
                (blockNlPossible && lhStats.numDocs() > rhStats.numDocs())) {
                // For block nested loop, the left side should always be smaller. Benchmarks have shown that the
                // performance decreases if the left side is much larger and no limit is applied.
                ExecutionPlan tmpExecutionPlan = left;
                left = right;
                right = tmpExecutionPlan;
                leftLogicalPlan = rhs;
                rightLogicalPlan = lhs;
                joinType = joinType.invert();
            }
        }

        Tuple<Collection<String>, List<MergePhase>> joinExecutionNodesAndMergePhases =
            configureExecution(left, right, plannerContext, isDistributed);

        List<Symbol> joinOutputs = Lists.concat(leftLogicalPlan.outputs(), rightLogicalPlan.outputs());
        SubQueryAndParamBinder paramBinder = new SubQueryAndParamBinder(params, subQueryResults);

        Symbol joinInput = null;
        if (joinCondition != null) {
            joinInput = InputColumns.create(paramBinder.apply(joinCondition), joinOutputs);
        }


        NestedLoopPhase nlPhase = new NestedLoopPhase(
            plannerContext.jobId(),
            plannerContext.nextExecutionPhaseId(),
            isDistributed ? "distributed-nested-loop" : "nested-loop",
            Collections.singletonList(createJoinProjection(outputs, joinOutputs)),
            joinExecutionNodesAndMergePhases.v2().get(0),
            joinExecutionNodesAndMergePhases.v2().get(1),
            leftLogicalPlan.outputs().size(),
            rightLogicalPlan.outputs().size(),
            joinExecutionNodesAndMergePhases.v1(),
            joinType,
            joinInput,
            Symbols.typeView(leftLogicalPlan.outputs()),
            lhStats.averageSizePerRowInBytes(),
            lhStats.numDocs(),
            blockNlPossible
        );

        PositionalOrderBy orderByFromLeft = left.resultDescription().orderBy();

        return new Join(
            nlPhase,
            left,
            right,
            NO_LIMIT,
            0,
            NO_LIMIT,
            outputs().size(),
            orderByFromLeft
        );
    }

    @Override
    public List<LogicalPlan> sources() {
        return List.of(lhs, rhs);
    }

    @Override
    public LogicalPlan replaceSources(List<LogicalPlan> sources) {
        return new NestedLoopJoin(
            outputs,
            sources.get(0),
            sources.get(1),
            joinType,
            joinCondition,
            isFiltered,
            orderByWasPushedDown,
            rewriteNestedLoopJoinToHashJoinDone
        );
    }

    @Override
    public LogicalPlan pruneOutputsExcept(SequencedCollection<Symbol> outputsToKeep) {
        PrunedOutputsResult pruned = pruneOutputs(outputsToKeep);
        if (pruned == null) {
            return this;
        }
        return new NestedLoopJoin(
            pruned.outputs(),
            pruned.lhs(),
            pruned.rhs(),
            joinType,
            joinCondition,
            isFiltered,
            orderByWasPushedDown,
            rewriteNestedLoopJoinToHashJoinDone
        );
    }

    @Nullable
    @Override
    public FetchRewrite rewriteToFetch(Collection<Symbol> usedColumns) {
        RewriteToFetchResult result = fetchRewrite(usedColumns);
        if (result == null) {
            return null;
        }

        return new FetchRewrite(
            result.allReplacedOutputs(),
            new NestedLoopJoin(
                result.outputs(),
                result.lhs(),
                result.rhs(),
                joinType,
                joinCondition,
                isFiltered,
                orderByWasPushedDown,
                rewriteNestedLoopJoinToHashJoinDone
            )
        );
    }

    static void setReplacedOutputs(LogicalPlan operator, FetchRewrite rewrite, LinkedHashMap<Symbol, Symbol> allReplacedOutputs) {
        if (rewrite == null) {
            for (var output : operator.outputs()) {
                allReplacedOutputs.put(output, output);
            }
        } else {
            allReplacedOutputs.putAll(rewrite.replacedOutputs());
        }
    }

    private Tuple<Collection<String>, List<MergePhase>> configureExecution(ExecutionPlan left,
                                                                           ExecutionPlan right,
                                                                           PlannerContext plannerContext,
                                                                           boolean isDistributed) {
        Collection<String> nlExecutionNodes = Set.of(plannerContext.handlerNode());
        ResultDescription leftResultDesc = left.resultDescription();
        ResultDescription rightResultDesc = right.resultDescription();
        MergePhase leftMerge = null;
        MergePhase rightMerge = null;

        if (leftResultDesc.nodeIds().size() == 1
            && Lists.equals(leftResultDesc.nodeIds(), rightResultDesc.nodeIds())
            && !rightResultDesc.hasRemainingLimitOrOffset()) {
            // if the left and the right plan are executed on the same single node the mergePhase
            // should be omitted. This is the case if the left and right table have only one shards which
            // are on the same node
            nlExecutionNodes = leftResultDesc.nodeIds();
            left.setDistributionInfo(DistributionInfo.DEFAULT_SAME_NODE);
            right.setDistributionInfo(DistributionInfo.DEFAULT_SAME_NODE);
        } else if (isDistributed && !leftResultDesc.hasRemainingLimitOrOffset()) {
            // run join phase distributed on all nodes of the left relation
            nlExecutionNodes = leftResultDesc.nodeIds();
            left.setDistributionInfo(DistributionInfo.DEFAULT_SAME_NODE);
            right.setDistributionInfo(DistributionInfo.DEFAULT_BROADCAST);
            rightMerge = buildMergePhaseForJoin(plannerContext, rightResultDesc, nlExecutionNodes);
        } else {
            // run join phase non-distributed on the handler
            left.setDistributionInfo(DistributionInfo.DEFAULT_BROADCAST);
            right.setDistributionInfo(DistributionInfo.DEFAULT_BROADCAST);
            if (isMergePhaseNeeded(nlExecutionNodes, leftResultDesc, false)) {
                leftMerge = buildMergePhaseForJoin(plannerContext, leftResultDesc, nlExecutionNodes);
            }
            if (isMergePhaseNeeded(nlExecutionNodes, rightResultDesc, false)) {
                rightMerge = buildMergePhaseForJoin(plannerContext, rightResultDesc, nlExecutionNodes);
            }
        }
        return new Tuple<>(nlExecutionNodes, Arrays.asList(leftMerge, rightMerge));
    }

    /**
     * Creates an {@link EvalProjection} to ensure that the join output symbols are emitted in the original order as
     * a possible outer operator (e.g. GROUP BY) is relying on the order.
     * The order could have been changed due to the switch-table optimizations
     *
     * @param outputs       List of join output symbols in their original order.
     * @param joinOutputs   List of join output symbols after possible re-ordering due optimizations.
     */
    static Projection createJoinProjection(List<Symbol> outputs, List<Symbol> joinOutputs) {
        List<Symbol> projectionOutputs = InputColumns.create(
            outputs,
            new InputColumns.SourceSymbols(joinOutputs));
        return new EvalProjection(projectionOutputs);
    }

    @Override
    public <C, R> R accept(LogicalPlanVisitor<C, R> visitor, C context) {
        return visitor.visitNestedLoopJoin(this, context);
    }

    private static boolean isBlockNlPossible(ExecutionPlan left, ExecutionPlan right) {
        return left.resultDescription().orderBy() == null &&
               left.resultDescription().nodeIds().size() <= 1 &&
               Lists.equals(left.resultDescription().nodeIds(), right.resultDescription().nodeIds());
    }

    public boolean orderByWasPushedDown() {
        return orderByWasPushedDown;
    }

    @Override
    public void print(PrintContext printContext) {
        printContext
            .text("NestedLoopJoin[")
            .text(joinType.toString());
        if (joinCondition != null) {
            printContext
                .text(" | ")
                .text(joinCondition.toString());
        }
        printContext.text("]");
        printStats(printContext);
        printContext.nest(Lists.map(sources(), x -> x::print));
    }

    private static boolean isMergePhaseNeeded(Collection<String> executionNodes,
                                              ResultDescription resultDescription,
                                              boolean isDistributed) {
        return isDistributed ||
               resultDescription.hasRemainingLimitOrOffset() ||
               !Lists.equals(resultDescription.nodeIds(), executionNodes);
    }

    @Override
    public String toString() {
        return "NestedLoopJoin{" +
            "joinCondition=" + joinCondition +
            ", joinType=" + joinType +
            ", isFiltered=" + isFiltered +
            ", lhs=" + lhs +
            ", rhs=" + rhs +
            ", outputs=" + outputs() +
            '}';
    }
}
