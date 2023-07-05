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

import org.jetbrains.annotations.Nullable;

import io.crate.analyze.OrderBy;
import io.crate.analyze.relations.AbstractTableRelation;
import io.crate.common.collections.Lists2;
import io.crate.common.collections.Sets;
import io.crate.data.Row;
import io.crate.execution.dsl.phases.MergePhase;
import io.crate.execution.dsl.projection.Projection;
import io.crate.execution.dsl.projection.builder.ProjectionBuilder;
import io.crate.expression.symbol.SelectSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.SymbolVisitors;
import io.crate.metadata.RelationName;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.ExecutionPlan;
import io.crate.planner.PlannerContext;
import io.crate.planner.ResultDescription;
import io.crate.planner.distribution.DistributionInfo;
import io.crate.sql.tree.JoinType;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class JoinPlan implements LogicalPlan {

    protected final int id;
    protected final List<Symbol> outputs;
    protected final LogicalPlan lhs;
    protected final LogicalPlan rhs;
    @Nullable
    protected final Symbol joinCondition;
    protected final JoinType joinType;
    private boolean isReordered;


    public JoinPlan(int id,
                    List<Symbol> outputs,
                    LogicalPlan lhs,
                    LogicalPlan rhs,
                    @Nullable Symbol joinCondition,
                    JoinType joinType,
                    boolean isReordered) {
        this.id = id;
        this.outputs = outputs;
        this.lhs = lhs;
        this.rhs = rhs;
        this.joinCondition = joinCondition;
        this.joinType = joinType;
        this.isReordered = isReordered;
    }

    public LogicalPlan lhs() {
        return lhs;
    }

    public LogicalPlan rhs() {
        return rhs;
    }

    public boolean isReordered() {
        return isReordered;
    }

    @Override
    public List<Symbol> outputs() {
        return outputs;
    }

    @Override
    public ExecutionPlan build(DependencyCarrier dependencyCarrier,
                               PlannerContext plannerContext,
                               Set<PlanHint> planHints,
                               ProjectionBuilder projectionBuilder,
                               int limit,
                               int offset,
                               @Nullable OrderBy order,
                               @Nullable Integer pageSizeHint,
                               Row params,
                               SubQueryResults subQueryResults) {
        throw new UnsupportedOperationException();
    }

    public int id() {
        return id;
    }

    @Override
    public LogicalPlan pruneOutputsExcept(Collection<Symbol> outputsToKeep) {
        LinkedHashSet<Symbol> lhsToKeep = new LinkedHashSet<>();
        LinkedHashSet<Symbol> rhsToKeep = new LinkedHashSet<>();
        for (Symbol outputToKeep : outputsToKeep) {
            SymbolVisitors.intersection(outputToKeep, lhs.outputs(), lhsToKeep::add);
            SymbolVisitors.intersection(outputToKeep, rhs.outputs(), rhsToKeep::add);
        }
        SymbolVisitors.intersection(joinCondition, lhs.outputs(), lhsToKeep::add);
        SymbolVisitors.intersection(joinCondition, rhs.outputs(), rhsToKeep::add);
        LogicalPlan newLhs = lhs.pruneOutputsExcept(lhsToKeep);
        LogicalPlan newRhs = rhs.pruneOutputsExcept(rhsToKeep);
        if (newLhs == lhs && newRhs == rhs) {
            return this;
        }
        return new JoinPlan(
            id,
            outputs,
            newLhs,
            newRhs,
            joinCondition,
            joinType,
            isReordered
        );
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
    public <C, R> R accept(LogicalPlanVisitor<C, R> visitor, C context) {
        return visitor.visitJoinPlan(this, context);
    }

    @Nullable
    public Symbol joinCondition() {
        return joinCondition;
    }

    public JoinType joinType() {
        return joinType;
    }

    @Override
    public Set<RelationName> getRelationNames() {
        return Sets.union(lhs.getRelationNames(), rhs.getRelationNames());
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
        return new JoinPlan(
            id,
            outputs,
            sources.get(0),
            sources.get(1),
            joinCondition,
            joinType,
            isReordered
        );
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
