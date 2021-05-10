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

import static io.crate.planner.operators.LogicalPlanner.NO_LIMIT;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import io.crate.analyze.OrderBy;
import io.crate.analyze.relations.AbstractTableRelation;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.common.annotations.VisibleForTesting;
import io.crate.common.collections.Lists2;
import io.crate.common.collections.Tuple;
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
import io.crate.expression.symbol.SymbolVisitors;
import io.crate.expression.symbol.Symbols;
import io.crate.metadata.RelationName;
import io.crate.planner.ExecutionPlan;
import io.crate.planner.PlannerContext;
import io.crate.planner.ResultDescription;
import io.crate.planner.distribution.DistributionInfo;
import io.crate.planner.distribution.DistributionType;
import io.crate.planner.node.dql.join.Join;
import io.crate.planner.node.dql.join.JoinType;
import io.crate.statistics.TableStats;

public class HashJoin implements LogicalPlan {

    private final Symbol joinCondition;
    @VisibleForTesting
    final AnalyzedRelation concreteRelation;
    private final List<Symbol> outputs;
    final LogicalPlan rhs;
    final LogicalPlan lhs;

    public HashJoin(LogicalPlan lhs,
                    LogicalPlan rhs,
                    Symbol joinCondition,
                    AnalyzedRelation concreteRelation) {
        this.outputs = Lists2.concat(lhs.outputs(), rhs.outputs());
        this.lhs = lhs;
        this.rhs = rhs;
        this.concreteRelation = concreteRelation;
        this.joinCondition = joinCondition;
    }

    public JoinType joinType() {
        return JoinType.INNER;
    }

    public Symbol joinCondition() {
        return joinCondition;
    }

    public LogicalPlan lhs() {
        return lhs;
    }

    public LogicalPlan rhs() {
        return rhs;
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
                               SubQueryResults subQueryResults) {
        ExecutionPlan leftExecutionPlan = lhs.build(
            plannerContext, projectionBuilder, NO_LIMIT, 0, null, null, params, subQueryResults);
        ExecutionPlan rightExecutionPlan = rhs.build(
            plannerContext, projectionBuilder, NO_LIMIT, 0, null, null, params, subQueryResults);

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

        SubQueryAndParamBinder paramBinder = new SubQueryAndParamBinder(params, subQueryResults);
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

        if (joinExecutionNodes.isEmpty()) {
            // The left source might have zero execution nodes, for example in the case of `sys.shards` without any tables
            // If the join then also uses zero execution nodes, a distributed plan no longer works because
            // the source operators wouldn't have a downstream node where they can send the results to.
            // → we switch to non-distributed which results in the join running on the handlerNode.
            isDistributed = false;
        }
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
                leftOutputs = setModuloDistribution(Lists2.map(hashSymbols.v1(), paramBinder), leftLogicalPlan.outputs(), leftExecutionPlan);
                rightOutputs = setModuloDistribution(Lists2.map(hashSymbols.v2(), paramBinder), rightLogicalPlan.outputs(), rightExecutionPlan);
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
            "hash-join",
            Collections.singletonList(JoinOperations.createJoinProjection(outputs, joinOutputs)),
            leftMerge,
            rightMerge,
            leftOutputs.size(),
            rightOutputs.size(),
            joinExecutionNodes,
            InputColumns.create(paramBinder.apply(joinCondition), joinOutputs),
            InputColumns.create(Lists2.map(hashSymbols.v1(), paramBinder), new InputColumns.SourceSymbols(leftOutputs)),
            InputColumns.create(Lists2.map(hashSymbols.v2(), paramBinder), new InputColumns.SourceSymbols(rightOutputs)),
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

    @Override
    public List<Symbol> outputs() {
        return outputs;
    }

    @Override
    public List<AbstractTableRelation<?>> baseTables() {
        return Lists2.concat(lhs.baseTables(), rhs.baseTables());
    }

    @Override
    public List<LogicalPlan> sources() {
        return List.of(lhs, rhs);
    }

    @Override
    public LogicalPlan replaceSources(List<LogicalPlan> sources) {
        return new HashJoin(
            sources.get(0),
            sources.get(1),
            joinCondition,
            concreteRelation
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
        SymbolVisitors.intersection(joinCondition, lhs.outputs(), lhsToKeep::add);
        SymbolVisitors.intersection(joinCondition, rhs.outputs(), rhsToKeep::add);
        LogicalPlan newLhs = lhs.pruneOutputsExcept(tableStats, lhsToKeep);
        LogicalPlan newRhs = rhs.pruneOutputsExcept(tableStats, rhsToKeep);
        if (newLhs == lhs && newRhs == rhs) {
            return this;
        }
        return new HashJoin(
            newLhs,
            newRhs,
            joinCondition,
            concreteRelation
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
        SymbolVisitors.intersection(joinCondition, lhs.outputs(), usedFromLeft::add);
        SymbolVisitors.intersection(joinCondition, rhs.outputs(), usedFromRight::add);
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
            new HashJoin(
                lhsFetchRewrite.newPlan(),
                rhsFetchRewrite.newPlan(),
                joinCondition,
                concreteRelation
            )
        );
    }

    private Tuple<List<Symbol>, List<Symbol>> extractHashJoinSymbolsFromJoinSymbolsAndSplitPerSide(boolean switchedTables) {
        Map<RelationName, List<Symbol>> hashJoinSymbols = HashJoinConditionSymbolsExtractor.extract(joinCondition);

        // First extract the symbols that belong to the concrete relation
        List<Symbol> hashJoinSymbolsForConcreteRelation = hashJoinSymbols.remove(concreteRelation.relationName());

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

    @Override
    public void print(PrintContext printContext) {
        printContext
            .text("HashJoin[")
            .text(joinCondition.toString())
            .text("]")
            .nest(
                lhs::print,
                rhs::print
            );
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
