/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.SequencedCollection;
import java.util.SequencedMap;

import org.jetbrains.annotations.Nullable;

import io.crate.common.collections.Lists;
import io.crate.execution.dsl.phases.MergePhase;
import io.crate.execution.dsl.projection.Projection;
import io.crate.execution.dsl.projection.builder.ProjectionBuilder;
import io.crate.expression.symbol.FetchMarker;
import io.crate.expression.symbol.SelectSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.SymbolVisitors;
import io.crate.metadata.RelationName;
import io.crate.planner.PlannerContext;
import io.crate.planner.ResultDescription;
import io.crate.planner.distribution.DistributionInfo;
import io.crate.sql.tree.JoinType;

public abstract class AbstractJoinPlan implements LogicalPlan {

    protected final List<Symbol> outputs;
    protected final LogicalPlan lhs;
    protected final LogicalPlan rhs;
    @Nullable
    protected final Symbol joinCondition;
    protected final JoinType joinType;

    protected AbstractJoinPlan(List<Symbol> outputs,
                               LogicalPlan lhs,
                               LogicalPlan rhs,
                               @Nullable Symbol joinCondition,
                               JoinType joinType) {
        this.outputs = outputs;
        this.lhs = lhs;
        this.rhs = rhs;
        this.joinCondition = joinCondition;
        this.joinType = joinType;
    }

    protected static List<Symbol> buildOutputs(List<Symbol> lhs, List<Symbol> rhs, JoinType joinType) {
        if (joinType == JoinType.SEMI) {
            return lhs;
        }
        return Lists.concat(lhs, rhs);
    }

    public LogicalPlan lhs() {
        return lhs;
    }

    public LogicalPlan rhs() {
        return rhs;
    }

    @Override
    public List<Symbol> outputs() {
        return outputs;
    }

    @Nullable
    protected AbstractJoinPlan.PrunedOutputsResult pruneOutputs(SequencedCollection<Symbol> outputsToKeep) {
        LinkedHashSet<Symbol> newOutputsToKeep = new LinkedHashSet<>();
        LinkedHashSet<Symbol> lhsToKeep = new LinkedHashSet<>();
        LinkedHashSet<Symbol> rhsToKeep = new LinkedHashSet<>();
        for (Symbol outputToKeep : outputsToKeep) {
            SymbolVisitors.intersection(outputToKeep, outputs, newOutputsToKeep::add);
            SymbolVisitors.intersection(outputToKeep, lhs.outputs(), lhsToKeep::add);
            SymbolVisitors.intersection(outputToKeep, rhs.outputs(), rhsToKeep::add);
        }
        if (joinCondition != null) {
            SymbolVisitors.intersection(joinCondition, lhs.outputs(), lhsToKeep::add);
            SymbolVisitors.intersection(joinCondition, rhs.outputs(), rhsToKeep::add);
        }
        LogicalPlan newLhs = lhs.pruneOutputsExcept(lhsToKeep);
        LogicalPlan newRhs = rhs.pruneOutputsExcept(rhsToKeep);

        List<Symbol> newOutputs = List.copyOf(newOutputsToKeep);
        if (newLhs == lhs && newRhs == rhs && outputs.equals(newOutputs)) {
            return null;
        }

        return new PrunedOutputsResult(lhs, rhs, newOutputs);
    }

    protected record PrunedOutputsResult(LogicalPlan lhs, LogicalPlan rhs, List<Symbol> outputs) {}

    @Nullable
    protected AbstractJoinPlan.RewriteToFetchResult fetchRewrite(Collection<Symbol> usedColumns) {
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
        FetchRewrite lhsFetchRewrite = lhs.rewriteToFetch(usedFromLeft);
        FetchRewrite rhsFetchRewrite = rhs.rewriteToFetch(usedFromRight);
        if (lhsFetchRewrite == null && rhsFetchRewrite == null) {
            return null;
        }
        LinkedHashMap<Symbol, Symbol> allReplacedOutputs = new LinkedHashMap<>();
        NestedLoopJoin.setReplacedOutputs(lhs, lhsFetchRewrite, allReplacedOutputs);
        NestedLoopJoin.setReplacedOutputs(rhs, rhsFetchRewrite, allReplacedOutputs);

        var newLhs = lhsFetchRewrite == null ? lhs : lhsFetchRewrite.newPlan();
        var newRhs = rhsFetchRewrite == null ? rhs : rhsFetchRewrite.newPlan();

        ArrayList<Symbol> newOutputs = new ArrayList<>();
        for (Symbol sourceOutput : newLhs.outputs()) {
            if (sourceOutput instanceof FetchMarker) {
                newOutputs.add(sourceOutput);
            }
        }
        for (Symbol sourceOutput : newRhs.outputs()) {
            if (sourceOutput instanceof FetchMarker) {
                newOutputs.add(sourceOutput);
            }
        }

        for (Symbol output : outputs) {
            if (SymbolVisitors.any(newLhs.outputs()::contains, output)) {
                newOutputs.add(output);
            } else if (SymbolVisitors.any(newRhs.outputs()::contains, output)) {
                newOutputs.add(output);
            }
        }

        return new RewriteToFetchResult(
            newLhs,
            newRhs,
            newOutputs,
            allReplacedOutputs);
    }

    protected record RewriteToFetchResult(
        LogicalPlan lhs,
        LogicalPlan rhs,
        List<Symbol> outputs,
        SequencedMap<Symbol, Symbol> allReplacedOutputs
    ) {}


    @Override
    public Map<LogicalPlan, SelectSymbol> dependencies() {
        Map<LogicalPlan, SelectSymbol> leftDeps = lhs.dependencies();
        Map<LogicalPlan, SelectSymbol> rightDeps = rhs.dependencies();
        HashMap<LogicalPlan, SelectSymbol> deps = new HashMap<>(leftDeps.size() + rightDeps.size());
        deps.putAll(leftDeps);
        deps.putAll(rightDeps);
        return deps;
    }

    @Nullable
    public Symbol joinCondition() {
        return joinCondition;
    }

    public JoinType joinType() {
        return joinType;
    }

    @Override
    public List<RelationName> relationNames() {
        return Lists.concatUnique(lhs.relationNames(), rhs.relationNames());
    }

    @Override
    public List<LogicalPlan> sources() {
        return List.of(lhs, rhs);
    }

    @Override
    public boolean supportsDistributedReads() {
        return lhs.supportsDistributedReads() && rhs.supportsDistributedReads();
    }

    protected static MergePhase buildMergePhaseForJoin(PlannerContext plannerContext,
                                                       ResultDescription resultDescription,
                                                       Collection<String> executionNodes) {
        List<Projection> projections = Collections.emptyList();
        if (resultDescription.hasRemainingLimitOrOffset()) {
            projections = Collections.singletonList(ProjectionBuilder.limitAndOffsetOrEvalIfNeeded(
                resultDescription.limit(),
                resultDescription.offset(),
                resultDescription.numOutputs(),
                resultDescription.streamOutputs()
            ));
        }

        return new MergePhase(
            plannerContext.jobId(),
            plannerContext.nextExecutionPhaseId(),
            "join-merge",
            resultDescription.nodeIds().size(),
            1,
            executionNodes,
            resultDescription.streamOutputs(),
            projections,
            DistributionInfo.DEFAULT_SAME_NODE,
            resultDescription.orderBy()
        );
    }
}
