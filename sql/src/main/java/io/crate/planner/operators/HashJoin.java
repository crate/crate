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
import io.crate.analyze.OrderBy;
import io.crate.analyze.relations.AnalyzedRelation;
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
import io.crate.planner.distribution.DistributionType;
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

    JoinType joinType() {
        return JoinType.INNER;
    }

    Symbol joinCondition() {
        return joinCondition;
    }

    @Override
    public Map<LogicalPlan, SelectSymbol> dependencies() {
        Map<LogicalPlan, SelectSymbol> leftDeps = lhs.dependencies();
        Map<LogicalPlan, SelectSymbol> rightDeps = rhs.dependencies();
        HashMap<LogicalPlan, SelectSymbol> deps = new HashMap<>(leftDeps.size() + rightDeps.size());
        deps.putAll(leftDeps);
        deps.putAll(rightDeps);
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

        Tuple<List<Symbol>, List<Symbol>> hashSymbols =
            extractHashJoinSymbolsFromJoinSymbolsAndSplitPerSide(tablesSwitched);

        ResultDescription leftResultDesc = leftExecutionPlan.resultDescription();
        ResultDescription rightResultDesc = rightExecutionPlan.resultDescription();
        Collection<String> joinExecutionNodes = leftResultDesc.nodeIds();

        List<Symbol> leftOutputs = leftLogicalPlan.outputs();
        List<Symbol> rightOutputs = rightLogicalPlan.outputs();
        MergePhase leftMerge = null;
        MergePhase rightMerge = null;

        // We can only run the join distributed if no remaining limit or offset must be applied on the source relations.
        // Because on distributed joins, every join is running on a slice (modulo) set of the data and so no limit/offset
        // could be applied. Limit/offset can only be applied on the whole data set after all partial rows from the
        // shards are merged
        boolean isDistributed = leftResultDesc.hasRemainingLimitOrOffset() == false
                                && rightResultDesc.hasRemainingLimitOrOffset() == false;

        if (joinExecutionNodes.size() == 1
            && joinExecutionNodes.equals(rightResultDesc.nodeIds())
            && !rightResultDesc.hasRemainingLimitOrOffset()) {
            // If the left and the right plan are executed on the same single node the mergePhase
            // should be omitted. This is the case if the left and right table have only one shards which
            // are on the same node
            leftExecutionPlan.setDistributionInfo(DistributionInfo.DEFAULT_SAME_NODE);
            rightExecutionPlan.setDistributionInfo(DistributionInfo.DEFAULT_SAME_NODE);
        } else {
            if (isDistributed) {
                // Run the join distributed by modulo distribution algorithm
                leftOutputs = setModuloDistribution(hashSymbols.v1(), leftLogicalPlan.outputs(), leftExecutionPlan);
                rightOutputs = setModuloDistribution(hashSymbols.v2(), rightLogicalPlan.outputs(), rightExecutionPlan);
            } else {
                // Run the join non-distributed on the handler node
                joinExecutionNodes = Collections.singletonList(plannerContext.handlerNode());
                leftExecutionPlan.setDistributionInfo(DistributionInfo.DEFAULT_BROADCAST);
                rightExecutionPlan.setDistributionInfo(DistributionInfo.DEFAULT_BROADCAST);
            }
            leftMerge = JoinOperations.buildMergePhaseForJoin(plannerContext, leftResultDesc, joinExecutionNodes);
            rightMerge = JoinOperations.buildMergePhaseForJoin(plannerContext, rightResultDesc, joinExecutionNodes);
        }

        List<Symbol> joinOutputs = Lists2.concat(leftOutputs, rightOutputs);

        HashJoinPhase joinPhase = new HashJoinPhase(
            plannerContext.jobId(),
            plannerContext.nextExecutionPhaseId(),
            Collections.singletonList(JoinOperations.createJoinProjection(outputs, joinOutputs)),
            leftMerge,
            rightMerge,
            leftOutputs.size(),
            rightOutputs.size(),
            joinExecutionNodes,
            InputColumns.create(joinCondition, joinOutputs),
            InputColumns.create(hashSymbols.v1(), new InputColumns.SourceSymbols(leftOutputs)),
            InputColumns.create(hashSymbols.v2(), new InputColumns.SourceSymbols(rightOutputs)),
            Symbols.typeView(leftOutputs),
            leftLogicalPlan.estimatedRowSize(),
            leftLogicalPlan.numExpectedRows());
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

    private Tuple<List<Symbol>, List<Symbol>> extractHashJoinSymbolsFromJoinSymbolsAndSplitPerSide(boolean switchedTables) {
        Map<AnalyzedRelation, List<Symbol>> hashJoinSymbols = HashJoinConditionSymbolsExtractor.extract(joinCondition);

        // First extract the symbols that belong to the concrete relation
        List<Symbol> hashJoinSymbolsForConcreteRelation = hashJoinSymbols.remove(concreteRelation);

        // All leftover extracted symbols belong to the other relation which might be a
        // "concrete" relation too but can already be a tree of relation.
        List<Symbol> hashJoinSymbolsForJoinTree =
            hashJoinSymbols.values().stream().flatMap(List::stream).collect(Collectors.toList());

        if (switchedTables) {
            return new Tuple<>(hashJoinSymbolsForConcreteRelation, hashJoinSymbolsForJoinTree);
        }
        return new Tuple<>(hashJoinSymbolsForJoinTree, hashJoinSymbolsForConcreteRelation);
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

    private List<Symbol> setModuloDistribution(List<Symbol> joinSymbols,
                                               List<Symbol> planOutputs,
                                               ExecutionPlan executionPlan) {
        List<Symbol> outputs = planOutputs;
        Symbol firstJoinSymbol = joinSymbols.get(0);
        int distributeBySymbolPos = planOutputs.indexOf(firstJoinSymbol);
        if (distributeBySymbolPos < 0) {
            // Looks like a function symbol, it must be evaluated BEFORE distribution
            outputs = createEvalProjectionForDistributionJoinSymbol(firstJoinSymbol, planOutputs, executionPlan);
            distributeBySymbolPos = planOutputs.size();
        }
        executionPlan.setDistributionInfo(new DistributionInfo(DistributionType.MODULO, distributeBySymbolPos));
        return outputs;
    }

    private List<Symbol> createEvalProjectionForDistributionJoinSymbol(Symbol firstJoinSymbol,
                                                                       List<Symbol> outputs,
                                                                       ExecutionPlan executionPlan) {
        List<Symbol> projectionOutputs = new ArrayList<>(outputs.size() + 1);
        projectionOutputs.addAll(outputs);
        projectionOutputs.add(firstJoinSymbol);
        EvalProjection evalProjection = new EvalProjection(InputColumns.create(projectionOutputs, new InputColumns.SourceSymbols(outputs)));
        executionPlan.addProjection(evalProjection);
        return projectionOutputs;
    }
}
