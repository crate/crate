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

package io.crate.planner.operators;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import io.crate.analyze.OrderBy;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.collections.Lists2;
import io.crate.data.Paging;
import io.crate.data.Row;
import io.crate.execution.dsl.phases.HashJoinPhase;
import io.crate.execution.dsl.phases.MergePhase;
import io.crate.execution.dsl.projection.EvalProjection;
import io.crate.execution.dsl.projection.builder.InputColumns;
import io.crate.execution.dsl.projection.builder.ProjectionBuilder;
import io.crate.execution.engine.join.JoinOperations;
import io.crate.execution.engine.pipeline.TopN;
import io.crate.expression.symbol.InputColumn;
import io.crate.expression.symbol.SelectSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.Symbols;
import io.crate.planner.ExecutionPlan;
import io.crate.planner.PlannerContext;
import io.crate.planner.ResultDescription;
import io.crate.planner.distribution.DistributionInfo;
import io.crate.planner.node.dql.join.Join;
import io.crate.planner.node.dql.join.JoinType;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.crate.planner.operators.LogicalPlanner.NO_LIMIT;

class HashJoin extends TwoInputPlan {

    private final Symbol joinCondition;
    @VisibleForTesting
    AnalyzedRelation topMostLeftRelation;
    @VisibleForTesting
    AnalyzedRelation rightRelation;

    HashJoin(LogicalPlan lhs,
             LogicalPlan rhs,
             Symbol joinCondition,
             AnalyzedRelation topMostLeftRelation,
             AnalyzedRelation rightRelation) {
        super(lhs, rhs, new ArrayList<>());
        this.topMostLeftRelation = topMostLeftRelation;
        this.rightRelation = rightRelation;
        this.joinCondition = joinCondition;
        this.outputs.addAll(lhs.outputs());
        this.outputs.addAll(rhs.outputs());
    }

    public JoinType joinType() {
        return JoinType.INNER;
    }

    protected Symbol joinCondition() {
        return joinCondition;
    }

    public Map<LogicalPlan, SelectSymbol> dependencies() {
        HashMap<LogicalPlan, SelectSymbol> deps = new HashMap<>(lhs.dependencies().size() + rhs.dependencies().size());
        deps.putAll(lhs.dependencies());
        deps.putAll(rhs.dependencies());
        return deps;
    }

    @Override
    public ExecutionPlan build(PlannerContext plannerContext,
                               ProjectionBuilder projectionBuilder,
                               int limit,
                               int offset,
                               @Nullable OrderBy order,
                               @Nullable Integer pageSizeHint,
                               Row params,
                               Map<SelectSymbol, Object> subQueryValues) {

        ExecutionPlan left = lhs.build(
            plannerContext, projectionBuilder, NO_LIMIT, 0, null, null, params, subQueryValues);
        ExecutionPlan right = rhs.build(
            plannerContext, projectionBuilder, NO_LIMIT, 0, null, null, params, subQueryValues);

        ResultDescription leftResultDesc = left.resultDescription();
        ResultDescription rightResultDesc = right.resultDescription();
        Collection<String> nlExecutionNodes = ImmutableSet.of(plannerContext.handlerNode());

        MergePhase leftMerge = null;
        MergePhase rightMerge = null;
        left.setDistributionInfo(DistributionInfo.DEFAULT_BROADCAST);
        if (JoinOperations.isMergePhaseNeeded(nlExecutionNodes, leftResultDesc, false)) {
            leftMerge = JoinOperations.buildMergePhaseForJoin(plannerContext, leftResultDesc, nlExecutionNodes);
        }
        if (nlExecutionNodes.size() == 1
            && nlExecutionNodes.equals(rightResultDesc.nodeIds())
            && !rightResultDesc.hasRemainingLimitOrOffset()) {
            // if the left and the right plan are executed on the same single node the mergePhase
            // should be omitted. This is the case if the left and right table have only one shards which
            // are on the same node
            right.setDistributionInfo(DistributionInfo.DEFAULT_SAME_NODE);
        } else {
            if (JoinOperations.isMergePhaseNeeded(nlExecutionNodes, rightResultDesc, false)) {
                rightMerge = JoinOperations.buildMergePhaseForJoin(plannerContext, rightResultDesc, nlExecutionNodes);
            }
            right.setDistributionInfo(DistributionInfo.DEFAULT_BROADCAST);
        }

        Symbol joinInput = InputColumns.create(joinCondition, Lists2.concat(lhs.outputs(), rhs.outputs()));
        Map<AnalyzedRelation, List<Symbol>> hashJoinSymbols = HashJoinConditionSymbolsExtractor.extract(joinCondition);
        List<Symbol> rightHashJoinSymbols = hashJoinSymbols.remove(rightRelation);
        List<Symbol> rightHashInputs = new ArrayList<>(rightHashJoinSymbols.size());
        for (Symbol symbol : rightHashJoinSymbols) {
            rightHashInputs.add(InputColumns.create(symbol, rhs.outputs()));
        }
        // All leftover extracted symbols belong to the left relation, the left relation might not be
        // a concrete table  as the HashJoin can be nested further down a Join tree.
        List<Symbol> leftHashJoinSymbols =
            hashJoinSymbols.values().stream().flatMap(List::stream).collect(Collectors.toList());
        List<Symbol> leftHashInputs = new ArrayList<>(leftHashJoinSymbols.size());
        for (Symbol symbol : leftHashJoinSymbols) {
            leftHashInputs.add(InputColumns.create(symbol, lhs.outputs()));
        }

        HashJoinPhase joinPhase = new HashJoinPhase(
            plannerContext.jobId(),
            plannerContext.nextExecutionPhaseId(),
            "hash-join",
            // JoinPhase ctor wants at least one projection
            Collections.singletonList(new EvalProjection(InputColumn.fromSymbols(outputs))),
            leftMerge,
            rightMerge,
            lhs.outputs().size(),
            rhs.outputs().size(),
            nlExecutionNodes,
            joinInput,
            leftHashInputs,
            rightHashInputs,
            Symbols.typeView(lhs.outputs()),
            Paging.PAGE_SIZE);
        return new Join(
            joinPhase,
            left,
            right,
            TopN.NO_LIMIT,
            0,
            TopN.NO_LIMIT,
            outputs.size(),
            null
        );
    }

    @Override
    protected LogicalPlan updateSources(LogicalPlan newLeftSource, LogicalPlan newRightSource) {
        return new HashJoin(newLeftSource, newRightSource, joinCondition, topMostLeftRelation, rightRelation);
    }

    @Override
    public long numExpectedRows() {
        // We don't have any cardinality estimates, so just take the bigger table
        return Math.max(lhs.numExpectedRows(), rhs.numExpectedRows());
    }

    @Override
    public <C, R> R accept(LogicalPlanVisitor<C, R> visitor, C context) {
        return visitor.visitHashJoin(this, context);
    }
}
