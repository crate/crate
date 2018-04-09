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
import io.crate.analyze.relations.DocTableRelation;
import io.crate.collections.Lists2;
import io.crate.data.Row;
import io.crate.execution.dsl.phases.HashJoinPhase;
import io.crate.execution.dsl.phases.MergePhase;
import io.crate.execution.dsl.projection.EvalProjection;
import io.crate.execution.dsl.projection.builder.InputColumns;
import io.crate.execution.dsl.projection.builder.ProjectionBuilder;
import io.crate.execution.engine.join.JoinOperations;
import io.crate.execution.engine.pipeline.TopN;
import io.crate.expression.symbol.SelectSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.Symbols;
import io.crate.planner.ExecutionPlan;
import io.crate.planner.PlannerContext;
import io.crate.planner.ResultDescription;
import io.crate.planner.TableStats;
import io.crate.planner.distribution.DistributionInfo;
import io.crate.planner.node.dql.join.Join;
import io.crate.planner.node.dql.join.JoinType;
import org.elasticsearch.common.collect.Tuple;

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
    private final TableStats tableStats;
    @VisibleForTesting
    final AnalyzedRelation concreteRelation;

    HashJoin(LogicalPlan lhs,
             LogicalPlan rhs,
             Symbol joinCondition,
             AnalyzedRelation concreteRelation,
             TableStats tableStats) {
        super(lhs, rhs, new ArrayList<>());
        this.concreteRelation = concreteRelation;
        this.joinCondition = joinCondition;
        this.outputs.addAll(lhs.outputs());
        this.outputs.addAll(rhs.outputs());
        this.tableStats = tableStats;
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


        ExecutionPlan leftExecutionPlan = lhs.build(
            plannerContext, projectionBuilder, NO_LIMIT, 0, null, null, params, subQueryValues);
        ExecutionPlan rightExecutionPlan = rhs.build(
            plannerContext, projectionBuilder, NO_LIMIT, 0, null, null, params, subQueryValues);

        LogicalPlan leftLogicalPlan = lhs;
        LogicalPlan rightLogicalPlan = rhs;

        boolean tablesSwitched = false;
        // We move smaller table to the right side since benchmarking
        // revealed that this improves performance in most cases.
        if (lhs.numExpectedRows() < rhs.numExpectedRows()) {
            tablesSwitched = true;
            leftLogicalPlan = rhs;
            rightLogicalPlan = lhs;

            ExecutionPlan tmp = leftExecutionPlan;
            leftExecutionPlan = rightExecutionPlan;
            rightExecutionPlan = tmp;
        }

        ResultDescription leftResultDesc = leftExecutionPlan.resultDescription();
        ResultDescription rightResultDesc = rightExecutionPlan.resultDescription();

        boolean hasDocTables = baseTables.stream().anyMatch(r -> r instanceof DocTableRelation);
        boolean isDistributed =
            hasDocTables && (!leftResultDesc.nodeIds().isEmpty() && !rightResultDesc.nodeIds().isEmpty());

        Collection<String> joinExecutionNodes = ImmutableSet.of(plannerContext.handlerNode());
        MergePhase leftMerge = null;
        MergePhase rightMerge = null;

        if (isDistributed) {
            // always execute on the left side, the larger branch
            leftExecutionPlan.setDistributionInfo(DistributionInfo.DEFAULT_SAME_NODE);
            joinExecutionNodes = leftResultDesc.nodeIds();
        } else {
            leftExecutionPlan.setDistributionInfo(DistributionInfo.DEFAULT_BROADCAST);
            if (JoinOperations.isMergePhaseNeeded(joinExecutionNodes, leftResultDesc, false)) {
                leftMerge = JoinOperations.buildMergePhaseForJoin(plannerContext, leftResultDesc, joinExecutionNodes);
            }
        }

        if (joinExecutionNodes.size() == 1
            && joinExecutionNodes.equals(rightResultDesc.nodeIds())
            && !rightResultDesc.hasRemainingLimitOrOffset()) {
            rightExecutionPlan.setDistributionInfo(DistributionInfo.DEFAULT_SAME_NODE);
        } else {
            if (JoinOperations.isMergePhaseNeeded(joinExecutionNodes, rightResultDesc, isDistributed)) {
                rightMerge = JoinOperations.buildMergePhaseForJoin(plannerContext, rightResultDesc, joinExecutionNodes);
            }
            rightExecutionPlan.setDistributionInfo(DistributionInfo.DEFAULT_BROADCAST);
        }

        Symbol joinConditionInput = InputColumns.create(
            joinCondition,
            Lists2.concat(leftLogicalPlan.outputs(), rightLogicalPlan.outputs()));
        Tuple<List<Symbol>, List<Symbol>> hashInputs =
            extractHashJoinInputsFromJoinSymbolsAndSplitPerSide(tablesSwitched);


        List<Symbol> joinOutputs = Lists2.concat(leftLogicalPlan.outputs(), rightLogicalPlan.outputs());
        // The projection operates on the the outputs of the join operation, which may be inverted due to a table switch,
        // and must produce outputs that match the order of the original outputs, because a "parent" (eg. GROUP BY)
        // operator of the join expects the original outputs order.
        List<Symbol> projectionOutputs = InputColumns.create(
            outputs,
            new InputColumns.SourceSymbols(joinOutputs));

        HashJoinPhase joinPhase = new HashJoinPhase(
            plannerContext.jobId(),
            plannerContext.nextExecutionPhaseId(),
            isDistributed ? "distributed-hash-join" : "hash-join",
            // JoinPhase ctor wants at least one projection
            Collections.singletonList(new EvalProjection(projectionOutputs)),
            leftMerge,
            rightMerge,
            leftLogicalPlan.outputs().size(),
            rightLogicalPlan.outputs().size(),
            joinExecutionNodes,
            joinConditionInput,
            hashInputs.v1(),
            hashInputs.v2(),
            Symbols.typeView(leftLogicalPlan.outputs()),
            leftLogicalPlan.estimatedRowSize(),
            leftLogicalPlan.numExpectedRows(),
            getRowsToBeConsumed(limit, order));
        return new Join(
            joinPhase,
            leftExecutionPlan,
            rightExecutionPlan,
            TopN.NO_LIMIT,
            0,
            TopN.NO_LIMIT,
            outputs.size(),
            null
        );
    }

    private int getRowsToBeConsumed(int limit, @Nullable OrderBy order) {
        if (order != null || limit == NO_LIMIT) {
            return Integer.MAX_VALUE;
        } else {
            return limit;
        }
    }

    private Tuple<List<Symbol>, List<Symbol>> extractHashJoinInputsFromJoinSymbolsAndSplitPerSide(boolean switchedTables) {
        Map<AnalyzedRelation, List<Symbol>> hashJoinSymbols = HashJoinConditionSymbolsExtractor.extract(joinCondition);

        // First extract the symbols that belong to the concrete relation
        List<Symbol> hashJoinSymbolsForConcreteRelation = hashJoinSymbols.remove(concreteRelation);
        List<Symbol> hashInputsForConcreteRelation = InputColumns.create(
            hashJoinSymbolsForConcreteRelation,
            new InputColumns.SourceSymbols(rhs.outputs()));

        // All leftover extracted symbols belong to the other relation which might be a
        // "concrete" relation too but can already be a tree of relation.
        List<Symbol> hashJoinSymbolsForJoinTree =
            hashJoinSymbols.values().stream().flatMap(List::stream).collect(Collectors.toList());
        List<Symbol> hashInputsForJoinTree = InputColumns.create(
            hashJoinSymbolsForJoinTree,
            new InputColumns.SourceSymbols(lhs.outputs()));

        if (switchedTables) {
            return new Tuple<>(hashInputsForConcreteRelation, hashInputsForJoinTree);
        }
        return new Tuple<>(hashInputsForJoinTree, hashInputsForConcreteRelation);
    }

    @Override
    protected LogicalPlan updateSources(LogicalPlan newLeftSource, LogicalPlan newRightSource) {
        return new HashJoin(newLeftSource, newRightSource, joinCondition, concreteRelation, tableStats);
    }

    @Override
    public long numExpectedRows() {
        // We don't have any cardinality estimates, so just take the bigger table
        return Math.max(lhs.numExpectedRows(), rhs.numExpectedRows());
    }

    @Override
    public long estimatedRowSize() {
        return lhs.estimatedRowSize() + rhs.estimatedRowSize();
    }

    @Override
    public <C, R> R accept(LogicalPlanVisitor<C, R> visitor, C context) {
        return visitor.visitHashJoin(this, context);
    }
}
