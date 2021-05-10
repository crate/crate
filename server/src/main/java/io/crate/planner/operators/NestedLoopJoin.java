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

import io.crate.analyze.OrderBy;
import io.crate.analyze.relations.AbstractTableRelation;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.DocTableRelation;
import io.crate.common.collections.Lists2;
import io.crate.common.collections.Maps;
import io.crate.common.collections.Tuple;
import io.crate.data.Row;
import io.crate.execution.dsl.phases.MergePhase;
import io.crate.execution.dsl.phases.NestedLoopPhase;
import io.crate.execution.dsl.projection.builder.InputColumns;
import io.crate.execution.dsl.projection.builder.ProjectionBuilder;
import io.crate.execution.engine.join.JoinOperations;
import io.crate.execution.engine.pipeline.TopN;
import io.crate.expression.symbol.SelectSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.SymbolVisitors;
import io.crate.expression.symbol.Symbols;
import io.crate.planner.ExecutionPlan;
import io.crate.planner.PlannerContext;
import io.crate.planner.PositionalOrderBy;
import io.crate.planner.ResultDescription;
import io.crate.planner.distribution.DistributionInfo;
import io.crate.planner.node.dql.join.Join;
import io.crate.planner.node.dql.join.JoinType;
import io.crate.statistics.TableStats;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.crate.planner.operators.Limit.limitAndOffset;
import static io.crate.planner.operators.LogicalPlanner.NO_LIMIT;

public class NestedLoopJoin implements LogicalPlan {

    @Nullable
    private final Symbol joinCondition;
    private final AnalyzedRelation topMostLeftRelation;
    private final JoinType joinType;
    private final boolean isFiltered;
    final LogicalPlan lhs;
    final LogicalPlan rhs;
    private final List<Symbol> outputs;
    private final List<AbstractTableRelation<?>> baseTables;
    private final Map<LogicalPlan, SelectSymbol> dependencies;
    private boolean orderByWasPushedDown = false;
    private boolean rewriteFilterOnOuterJoinToInnerJoinDone = false;

    NestedLoopJoin(LogicalPlan lhs,
                   LogicalPlan rhs,
                   JoinType joinType,
                   @Nullable Symbol joinCondition,
                   boolean isFiltered,
                   AnalyzedRelation topMostLeftRelation) {
        this.joinType = joinType;
        this.isFiltered = isFiltered || joinCondition != null;
        this.lhs = lhs;
        this.rhs = rhs;
        if (joinType == JoinType.SEMI) {
            this.outputs = lhs.outputs();
        } else {
            this.outputs = Lists2.concat(lhs.outputs(), rhs.outputs());
        }
        this.baseTables = Lists2.concat(lhs.baseTables(), rhs.baseTables());
        this.topMostLeftRelation = topMostLeftRelation;
        this.joinCondition = joinCondition;
        this.dependencies = Maps.concat(lhs.dependencies(), rhs.dependencies());
    }

    public NestedLoopJoin(LogicalPlan lhs,
                          LogicalPlan rhs,
                          JoinType joinType,
                          @Nullable Symbol joinCondition,
                          boolean isFiltered,
                          AnalyzedRelation topMostLeftRelation,
                          boolean orderByWasPushedDown,
                          boolean rewriteFilterOnOuterJoinToInnerJoinDone) {
        this(lhs, rhs, joinType, joinCondition, isFiltered, topMostLeftRelation);
        this.orderByWasPushedDown = orderByWasPushedDown;
        this.rewriteFilterOnOuterJoinToInnerJoinDone = rewriteFilterOnOuterJoinToInnerJoinDone;
    }

    public boolean isRewriteFilterOnOuterJoinToInnerJoinDone() {
        return rewriteFilterOnOuterJoinToInnerJoinDone;
    }

    public boolean isFiltered() {
        return true;
    }

    public AnalyzedRelation topMostLeftRelation() {
        return topMostLeftRelation;
    }

    public JoinType joinType() {
        return joinType;
    }

    @Nullable
    public Symbol joinCondition() {
        return joinCondition;
    }

    @Override
    public Map<LogicalPlan, SelectSymbol> dependencies() {
        return dependencies;
    }

    @Override
    public ExecutionPlan build(PlannerContext plannerContext,
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
        Integer childPageSizeHint = !isFiltered && limit != TopN.NO_LIMIT
            ? limitAndOffset(limit, offset)
            : null;

        ExecutionPlan left = lhs.build(
            plannerContext, projectionBuilder, NO_LIMIT, 0, null, childPageSizeHint, params, subQueryResults);
        ExecutionPlan right = rhs.build(
            plannerContext, projectionBuilder, NO_LIMIT, 0, null, childPageSizeHint, params, subQueryResults);

        PositionalOrderBy orderByFromLeft = left.resultDescription().orderBy();
        boolean hasDocTables = baseTables.stream().anyMatch(r -> r instanceof DocTableRelation);
        boolean isDistributed = hasDocTables && isFiltered && !joinType.isOuter();

        LogicalPlan leftLogicalPlan = lhs;
        LogicalPlan rightLogicalPlan = rhs;
        isDistributed = isDistributed &&
                        (!left.resultDescription().nodeIds().isEmpty() && !right.resultDescription().nodeIds().isEmpty());
        boolean blockNlPossible = !isDistributed && isBlockNlPossible(left, right);

        JoinType joinType = this.joinType;
        if (!orderByWasPushedDown && joinType.supportsInversion() &&
            (isDistributed && lhs.numExpectedRows() < rhs.numExpectedRows() && orderByFromLeft == null) ||
            (blockNlPossible && lhs.numExpectedRows() > rhs.numExpectedRows())) {
            // 1) The right side is always broadcast-ed, so for performance reasons we switch the tables so that
            //    the right table is the smaller (numOfRows). If left relation has a pushed-down OrderBy that needs
            //    to be preserved, then the switch is not possible.
            // 2) For block nested loop, the left side should always be smaller. Benchmarks have shown that the
            //    performance decreases if the left side is much larger and no limit is applied.
            ExecutionPlan tmpExecutionPlan = left;
            left = right;
            right = tmpExecutionPlan;
            leftLogicalPlan = rhs;
            rightLogicalPlan = lhs;
            joinType = joinType.invert();
        }
        Tuple<Collection<String>, List<MergePhase>> joinExecutionNodesAndMergePhases =
            configureExecution(left, right, plannerContext, isDistributed);

        List<Symbol> joinOutputs = Lists2.concat(leftLogicalPlan.outputs(), rightLogicalPlan.outputs());
        SubQueryAndParamBinder paramBinder = new SubQueryAndParamBinder(params, subQueryResults);

        Symbol joinInput = null;
        if (joinCondition != null) {
            joinInput = InputColumns.create(paramBinder.apply(joinCondition), joinOutputs);
        }

        NestedLoopPhase nlPhase = new NestedLoopPhase(
            plannerContext.jobId(),
            plannerContext.nextExecutionPhaseId(),
            isDistributed ? "distributed-nested-loop" : "nested-loop",
            Collections.singletonList(JoinOperations.createJoinProjection(outputs, joinOutputs)),
            joinExecutionNodesAndMergePhases.v2().get(0),
            joinExecutionNodesAndMergePhases.v2().get(1),
            leftLogicalPlan.outputs().size(),
            rightLogicalPlan.outputs().size(),
            joinExecutionNodesAndMergePhases.v1(),
            joinType,
            joinInput,
            Symbols.typeView(leftLogicalPlan.outputs()),
            leftLogicalPlan.estimatedRowSize(),
            leftLogicalPlan.numExpectedRows(),
            blockNlPossible
        );
        return new Join(
            nlPhase,
            left,
            right,
            TopN.NO_LIMIT,
            0,
            TopN.NO_LIMIT,
            outputs.size(),
            orderByFromLeft
        );
    }

    @Override
    public List<Symbol> outputs() {
        return outputs;
    }

    @Override
    public List<AbstractTableRelation<?>> baseTables() {
        return baseTables;
    }

    @Override
    public List<LogicalPlan> sources() {
        return List.of(lhs, rhs);
    }

    @Override
    public LogicalPlan replaceSources(List<LogicalPlan> sources) {
        return new NestedLoopJoin(
            sources.get(0),
            sources.get(1),
            joinType,
            joinCondition,
            isFiltered,
            topMostLeftRelation,
            orderByWasPushedDown,
            rewriteFilterOnOuterJoinToInnerJoinDone
        );
    }

    @Override
    public LogicalPlan pruneOutputsExcept(TableStats tableStats, Collection<Symbol> outputsToKeep) {
        LinkedHashSet<Symbol> lhsToKeep = new LinkedHashSet<>();
        LinkedHashSet<Symbol> rhsToKeep = new LinkedHashSet<>();
        for (Symbol outputToKeep : outputsToKeep) {
            SymbolVisitors.intersection(outputToKeep, lhs.outputs(), lhsToKeep::add);
            SymbolVisitors.intersection(outputToKeep, rhs.outputs(), rhsToKeep::add);
        }
        if (joinCondition != null) {
            SymbolVisitors.intersection(joinCondition, lhs.outputs(), lhsToKeep::add);
            SymbolVisitors.intersection(joinCondition, rhs.outputs(), rhsToKeep::add);
        }
        LogicalPlan newLhs = lhs.pruneOutputsExcept(tableStats, lhsToKeep);
        LogicalPlan newRhs = rhs.pruneOutputsExcept(tableStats, rhsToKeep);
        if (newLhs == lhs && newRhs == rhs) {
            return this;
        }
        return new NestedLoopJoin(
            newLhs,
            newRhs,
            joinType,
            joinCondition,
            isFiltered,
            topMostLeftRelation,
            orderByWasPushedDown,
            rewriteFilterOnOuterJoinToInnerJoinDone
        );
    }

    @Nullable
    @Override
    public FetchRewrite rewriteToFetch(TableStats tableStats, Collection<Symbol> usedColumns) {
        LinkedHashSet<Symbol> usedFromLeft = new LinkedHashSet<>();
        LinkedHashSet<Symbol> usedFromRight = new LinkedHashSet<>();
        for (Symbol usedColumn : usedColumns) {
            SymbolVisitors.intersection(usedColumn, lhs.outputs(), usedFromLeft::add);
            SymbolVisitors.intersection(usedColumn, rhs.outputs(), usedFromRight::add);
        }
        if (joinCondition != null) {
            SymbolVisitors.intersection(joinCondition, lhs.outputs(), usedFromLeft::add);
            SymbolVisitors.intersection(joinCondition, rhs.outputs(), usedFromRight::add);
        }
        FetchRewrite lhsFetchRewrite = lhs.rewriteToFetch(tableStats, usedFromLeft);
        if (lhsFetchRewrite == null) {
            return null;
        }
        FetchRewrite rhsFetchRewrite = rhs.rewriteToFetch(tableStats, usedFromRight);
        if (rhsFetchRewrite == null) {
            return null;
        }
        LinkedHashMap<Symbol, Symbol> allReplacedOutputs = new LinkedHashMap<>(lhsFetchRewrite.replacedOutputs());
        allReplacedOutputs.putAll(rhsFetchRewrite.replacedOutputs());
        return new FetchRewrite(
            allReplacedOutputs,
            new NestedLoopJoin(
                lhsFetchRewrite.newPlan(),
                rhsFetchRewrite.newPlan(),
                joinType,
                joinCondition,
                isFiltered,
                topMostLeftRelation,
                orderByWasPushedDown,
                rewriteFilterOnOuterJoinToInnerJoinDone
            )
        );
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
            && leftResultDesc.nodeIds().equals(rightResultDesc.nodeIds())
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
            rightMerge = JoinOperations.buildMergePhaseForJoin(plannerContext, rightResultDesc, nlExecutionNodes);
        } else {
            // run join phase non-distributed on the handler
            left.setDistributionInfo(DistributionInfo.DEFAULT_BROADCAST);
            right.setDistributionInfo(DistributionInfo.DEFAULT_BROADCAST);
            if (JoinOperations.isMergePhaseNeeded(nlExecutionNodes, leftResultDesc, false)) {
                leftMerge = JoinOperations.buildMergePhaseForJoin(plannerContext, leftResultDesc, nlExecutionNodes);
            }
            if (JoinOperations.isMergePhaseNeeded(nlExecutionNodes, rightResultDesc, false)) {
                rightMerge = JoinOperations.buildMergePhaseForJoin(plannerContext, rightResultDesc, nlExecutionNodes);
            }
        }
        return new Tuple<>(nlExecutionNodes, Arrays.asList(leftMerge, rightMerge));
    }

    @Override
    public long numExpectedRows() {
        if (joinType == JoinType.CROSS) {
            return lhs.numExpectedRows() * rhs.numExpectedRows();
        } else {
            // We don't have any cardinality estimates, so just take the bigger table
            return Math.max(lhs.numExpectedRows(), rhs.numExpectedRows());
        }
    }

    @Override
    public long estimatedRowSize() {
        return lhs.estimatedRowSize() + rhs.estimatedRowSize();
    }

    @Override
    public <C, R> R accept(LogicalPlanVisitor<C, R> visitor, C context) {
        return visitor.visitNestedLoopJoin(this, context);
    }

    private static boolean isBlockNlPossible(ExecutionPlan left, ExecutionPlan right) {
        return left.resultDescription().orderBy() == null &&
               left.resultDescription().nodeIds().size() <= 1 &&
               left.resultDescription().nodeIds().equals(right.resultDescription().nodeIds());
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
        printContext
            .text("]")
            .nest(Lists2.map(sources(), x -> x::print));
    }

    @Override
    public String toString() {
        return "NestedLoopJoin{" +
            "joinCondition=" + joinCondition +
            ", joinType=" + joinType +
            ", isFiltered=" + isFiltered +
            ", lhs=" + lhs +
            ", rhs=" + rhs +
            ", outputs=" + outputs +
            '}';
    }
}
