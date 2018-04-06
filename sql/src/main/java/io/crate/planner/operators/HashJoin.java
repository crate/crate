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
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.InputColumn;
import io.crate.expression.symbol.SelectSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.SymbolVisitor;
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

        Tuple<List<Symbol>, List<Symbol>> hashInputs =
            extractHashJoinInputsFromJoinSymbolsAndSplitPerSide(tablesSwitched);

        ResultDescription leftResultDesc = leftExecutionPlan.resultDescription();
        Collection<String> joinExecutionNodes;
        boolean isDistributed = true;
        if (isDistributed) {
            joinExecutionNodes = leftResultDesc.nodeIds();
        } else {
            joinExecutionNodes = Collections.singletonList(plannerContext.handlerNode());
        }

        MergePhase leftMerge = null;
        MergePhase rightMerge = null;
        leftExecutionPlan.setDistributionInfo(DistributionInfo.DEFAULT_BROADCAST);
        if (isDistributed) {
            leftMerge = JoinOperations.buildMergePhaseForJoin(plannerContext, leftResultDesc, joinExecutionNodes);
            Integer distributeByColumn = DistributionColumnExtractor.getColumnIndex(hashInputs.v1().get(0));
            assert distributeByColumn != null : "could not resolve the left distributeBy column index";
            leftExecutionPlan.setDistributionInfo(new DistributionInfo(DistributionType.MODULO, distributeByColumn));
        }

        ResultDescription rightResultDesc = rightExecutionPlan.resultDescription();
        if (joinExecutionNodes.size() == 1
            && joinExecutionNodes.equals(rightResultDesc.nodeIds())
            && !rightResultDesc.hasRemainingLimitOrOffset()) {
            // if the left and the right plan are executed on the same single node the mergePhase
            // should be omitted. This is the case if the left and right table have only one shards which
            // are on the same node
            rightExecutionPlan.setDistributionInfo(DistributionInfo.DEFAULT_SAME_NODE);
        } else {
            rightExecutionPlan.setDistributionInfo(DistributionInfo.DEFAULT_BROADCAST);
            if (isDistributed) {
                rightMerge = JoinOperations.buildMergePhaseForJoin(plannerContext, rightResultDesc, joinExecutionNodes);
                Integer distributeByColumn = DistributionColumnExtractor.getColumnIndex(hashInputs.v2().get(0));
                assert distributeByColumn != null : "could not resolve the right distributeBy column index";
                rightExecutionPlan.setDistributionInfo(new DistributionInfo(DistributionType.MODULO, distributeByColumn));
            }
        }

        List<Symbol> joinOutputs = Lists2.concat(leftLogicalPlan.outputs(), rightLogicalPlan.outputs());
        // The projection operates on the the outputs of the join operation, which may be inverted due to a table switch,
        // and must produce outputs that match the order of the original outputs, because a "parent" (eg. GROUP BY)
        // operator of the join expects the original outputs order.
        List<Symbol> projectionOutputs = InputColumns.create(
                outputs,
                new InputColumns.SourceSymbols(joinOutputs));

        Symbol joinConditionInput = InputColumns.create(
            joinCondition,
            Lists2.concat(leftLogicalPlan.outputs(), rightLogicalPlan.outputs()));

        HashJoinPhase joinPhase = new HashJoinPhase(
            plannerContext.jobId(),
            plannerContext.nextExecutionPhaseId(),
            "hash-join",
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

    private static class DistributionColumnExtractor extends SymbolVisitor<Void, Integer> {

        private static final DistributionColumnExtractor INSTANCE = new DistributionColumnExtractor();

        private static Integer getColumnIndex(Symbol symbol) {
            return INSTANCE.process(symbol, null);
        }

        @Override
        public Integer visitFunction(Function symbol, Void context) {
            for (Symbol arg : symbol.arguments()) {
                Integer idx = process(arg, context);
                if (idx != null) {
                    return idx;
                }
            }
            return null;
        }

        @Override
        public Integer visitInputColumn(InputColumn inputColumn, Void context) {
            return inputColumn.index();
        }
    }
}
