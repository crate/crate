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
import static io.crate.planner.operators.NestedLoopJoin.createJoinProjection;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.SequencedCollection;
import java.util.Set;

import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.VisibleForTesting;

import io.crate.analyze.OrderBy;
import io.crate.analyze.relations.QuerySplitter;
import io.crate.common.collections.Lists;
import io.crate.data.Row;
import io.crate.execution.dsl.phases.HashJoinPhase;
import io.crate.execution.dsl.phases.MergePhase;
import io.crate.execution.dsl.projection.EvalProjection;
import io.crate.execution.dsl.projection.builder.InputColumns;
import io.crate.execution.dsl.projection.builder.ProjectionBuilder;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.SymbolVisitors;
import io.crate.expression.symbol.Symbols;
import io.crate.metadata.RelationName;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.ExecutionPlan;
import io.crate.planner.PlannerContext;
import io.crate.planner.ResultDescription;
import io.crate.planner.distribution.DistributionInfo;
import io.crate.planner.distribution.DistributionType;
import io.crate.planner.node.dql.join.Join;
import io.crate.sql.tree.JoinType;

public class HashJoin extends AbstractJoinPlan {

    public HashJoin(LogicalPlan lhs,
                    LogicalPlan rhs,
                    Symbol joinCondition) {
        super(lhs, rhs, joinCondition, JoinType.INNER, LookUpJoin.NONE);
    }

    public HashJoin(LogicalPlan lhs,
                    LogicalPlan rhs,
                    Symbol joinCondition,
                    LookUpJoin lookUpJoin) {
        super(lhs, rhs, joinCondition, JoinType.INNER, lookUpJoin);
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
        ExecutionPlan leftExecutionPlan = lhs.build(
            executor, plannerContext, hints, projectionBuilder, NO_LIMIT, 0, null, null, params, subQueryResults);
        ExecutionPlan rightExecutionPlan = rhs.build(
            executor, plannerContext, hints, projectionBuilder, NO_LIMIT, 0, null, null, params, subQueryResults);

        SubQueryAndParamBinder paramBinder = new SubQueryAndParamBinder(params, subQueryResults);
        var hashSymbols = createHashSymbols(lhs.relationNames(), rhs.relationNames(), joinCondition);

        var lhsHashSymbols = hashSymbols.lhsHashSymbols();
        var rhsHashSymbols = hashSymbols.rhsHashSymbols();

        ResultDescription leftResultDesc = leftExecutionPlan.resultDescription();
        ResultDescription rightResultDesc = rightExecutionPlan.resultDescription();
        Collection<String> joinExecutionNodes = leftResultDesc.nodeIds();

        List<Symbol> leftOutputs = lhs.outputs();
        List<Symbol> rightOutputs = rhs.outputs();
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
            && Lists.equals(joinExecutionNodes, rightResultDesc.nodeIds())
            && !rightResultDesc.hasRemainingLimitOrOffset()) {
            // If the left and the right plan are executed on the same single node the mergePhase
            // should be omitted. This is the case if the left and right table have only one shards which
            // are on the same node
            leftExecutionPlan.setDistributionInfo(DistributionInfo.DEFAULT_SAME_NODE);
            rightExecutionPlan.setDistributionInfo(DistributionInfo.DEFAULT_SAME_NODE);
        } else {
            if (isDistributed) {
                // Run the join distributed by modulo distribution algorithm
                leftOutputs = setModuloDistribution(lhsHashSymbols, lhs.outputs(), leftExecutionPlan);
                rightOutputs = setModuloDistribution(rhsHashSymbols, rhs.outputs(), rightExecutionPlan);
            } else {
                // Run the join non-distributed on the handler node
                joinExecutionNodes = Collections.singletonList(plannerContext.handlerNode());
                leftExecutionPlan.setDistributionInfo(DistributionInfo.DEFAULT_BROADCAST);
                rightExecutionPlan.setDistributionInfo(DistributionInfo.DEFAULT_BROADCAST);
            }
            leftMerge = buildMergePhaseForJoin(plannerContext, leftResultDesc, joinExecutionNodes);
            rightMerge = buildMergePhaseForJoin(plannerContext, rightResultDesc, joinExecutionNodes);
        }

        List<Symbol> joinOutputs = Lists.concat(leftOutputs, rightOutputs);
        var lhStats = plannerContext.planStats().get(lhs);
        HashJoinPhase joinPhase = new HashJoinPhase(
            plannerContext.jobId(),
            plannerContext.nextExecutionPhaseId(),
            "hash-join",
            Collections.singletonList(createJoinProjection(outputs(), joinOutputs)),
            leftMerge,
            rightMerge,
            leftOutputs.size(),
            rightOutputs.size(),
            joinExecutionNodes,
            InputColumns.create(paramBinder.apply(joinCondition), joinOutputs),
            InputColumns.create(lhsHashSymbols, new InputColumns.SourceSymbols(leftOutputs)),
            InputColumns.create(rhsHashSymbols, new InputColumns.SourceSymbols(rightOutputs)),
            Symbols.typeView(leftOutputs),
            lhStats.estimateSizeForColumns(leftOutputs)
        );
        return new Join(
            joinPhase,
            leftExecutionPlan,
            rightExecutionPlan,
            NO_LIMIT,
            0,
            NO_LIMIT,
            outputs().size(),
            null
        );
    }

    public LogicalPlan replaceSources(List<LogicalPlan> sources) {
        return new HashJoin(
            sources.get(0),
            sources.get(1),
            joinCondition,
            lookupJoin
        );
    }

    @Override
    public LogicalPlan pruneOutputsExcept(SequencedCollection<Symbol> outputsToKeep) {
        LinkedHashSet<Symbol> lhsToKeep = new LinkedHashSet<>();
        LinkedHashSet<Symbol> rhsToKeep = new LinkedHashSet<>();
        for (Symbol outputToKeep : outputsToKeep) {
            SymbolVisitors.intersection(outputToKeep, lhs.outputs(), lhsToKeep::add);
            SymbolVisitors.intersection(outputToKeep, rhs.outputs(), rhsToKeep::add);
        }
        // If there a lookup-join in place, and the outputs belong only to the lookup side,
        // we can drop the join and return only the lookup-side
        if (lhsToKeep.isEmpty() && lookupJoin == LookUpJoin.RIGHT) {
            SymbolVisitors.intersection(joinCondition, rhs.outputs(), rhsToKeep::add);
            return rhs.pruneOutputsExcept(rhsToKeep);
        } else if (rhsToKeep.isEmpty() && lookupJoin == LookUpJoin.LEFT) {
            SymbolVisitors.intersection(joinCondition, lhs.outputs(), lhsToKeep::add);
            return lhs.pruneOutputsExcept(lhsToKeep);
        } else {
            SymbolVisitors.intersection(joinCondition, lhs.outputs(), lhsToKeep::add);
            SymbolVisitors.intersection(joinCondition, rhs.outputs(), rhsToKeep::add);
            LogicalPlan newLhs = lhs.pruneOutputsExcept(lhsToKeep);
            LogicalPlan newRhs = rhs.pruneOutputsExcept(rhsToKeep);
            if (newLhs == lhs && newRhs == rhs) {
                return this;
            }
            return new HashJoin(
                newLhs,
                newRhs,
                joinCondition,
                lookupJoin
            );
        }
    }

    @Nullable
    @Override
    public FetchRewrite rewriteToFetch(Collection<Symbol> usedColumns) {
        LinkedHashSet<Symbol> usedFromLeft = new LinkedHashSet<>();
        LinkedHashSet<Symbol> usedFromRight = new LinkedHashSet<>();
        for (Symbol usedColumn : usedColumns) {
            SymbolVisitors.intersection(usedColumn, lhs.outputs(), usedFromLeft::add);
            SymbolVisitors.intersection(usedColumn, rhs.outputs(), usedFromRight::add);
        }
        SymbolVisitors.intersection(joinCondition, lhs.outputs(), usedFromLeft::add);
        SymbolVisitors.intersection(joinCondition, rhs.outputs(), usedFromRight::add);
        FetchRewrite lhsFetchRewrite = lhs.rewriteToFetch(usedFromLeft);
        FetchRewrite rhsFetchRewrite = rhs.rewriteToFetch(usedFromRight);
        if (lhsFetchRewrite == null && rhsFetchRewrite == null) {
            return null;
        }
        LinkedHashMap<Symbol, Symbol> allReplacedOutputs = new LinkedHashMap<>();
        NestedLoopJoin.setReplacedOutputs(lhs, lhsFetchRewrite, allReplacedOutputs);
        NestedLoopJoin.setReplacedOutputs(rhs, rhsFetchRewrite, allReplacedOutputs);
        return new FetchRewrite(
            allReplacedOutputs,
            new HashJoin(
                lhsFetchRewrite == null ? lhs : lhsFetchRewrite.newPlan(),
                rhsFetchRewrite == null ? rhs : rhsFetchRewrite.newPlan(),
                joinCondition,
                lookupJoin
            )
        );
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
            .text("]");
        printStats(printContext);
        printContext.nest(
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

    @VisibleForTesting
    static HashSymbols createHashSymbols(List<RelationName> lhsRelationNames,
                                         List<RelationName> rhsRelationNames,
                                         Symbol symbol) {
        /* It is important here to process the hashSymbols in order as there values are used for building the
         *  hash codes. For example:
         *
         *      join-condition:     t1.a = t2.c AND t1.b = t2.d
         *      left hashSymbols:   [t1.a, t1.b]
         *      right hashSymbols:  [t2.c, t2.d]
         *
         *      with rows:          left ->     [1, 3]
         *                          right ->    [1, 3]
         *
         * if the order is not guaranteed, one side may use [3, 1] for hash code generation which yields different results
         *
         */
        var lhsHashSymbols = new ArrayList<Symbol>();
        var rhsHashSymbols = new ArrayList<Symbol>();
        for (var condition : QuerySplitter.split(symbol).entrySet()) {
            for (var entry : JoinConditionSymbolsExtractor.extract(condition.getValue()).entrySet()) {
                var relationName = entry.getKey();
                var symbols = entry.getValue();
                if (symbols != null) {
                    if (rhsRelationNames.contains(relationName)) {
                        rhsHashSymbols.addAll(symbols);
                    } else if (lhsRelationNames.contains(relationName)) {
                        lhsHashSymbols.addAll(symbols);
                    }
                }
            }
        }
        assert rhsHashSymbols.size() == lhsHashSymbols.size() : "Number of hash values for left and right hand side of a hash-join must be equal";
        return new HashSymbols(lhsHashSymbols, rhsHashSymbols);
    }

    record HashSymbols(List<Symbol> lhsHashSymbols, List<Symbol> rhsHashSymbols) { }

}
