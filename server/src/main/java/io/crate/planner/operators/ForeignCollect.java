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

import java.util.List;
import java.util.Map;
import java.util.SequencedCollection;
import java.util.Set;

import org.jetbrains.annotations.Nullable;

import io.crate.analyze.OrderBy;
import io.crate.analyze.WhereClause;
import io.crate.common.collections.Lists;
import io.crate.data.Row;
import io.crate.execution.dsl.phases.ForeignCollectPhase;
import io.crate.execution.dsl.projection.builder.ProjectionBuilder;
import io.crate.execution.engine.pipeline.LimitAndOffset;
import io.crate.expression.symbol.SelectSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.fdw.ForeignDataWrapper;
import io.crate.fdw.ForeignTableRelation;
import io.crate.metadata.RelationName;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.ExecutionPlan;
import io.crate.planner.PlannerContext;

public class ForeignCollect implements LogicalPlan {

    private final ForeignDataWrapper fdw;
    private final ForeignTableRelation relation;
    private final List<Symbol> toCollect;
    private final WhereClause where;
    private final String executeAs;

    public ForeignCollect(ForeignDataWrapper fdw,
                          ForeignTableRelation relation,
                          List<Symbol> toCollect,
                          WhereClause where,
                          String executeAs) {
        this.fdw = fdw;
        this.relation = relation;
        this.toCollect = toCollect;
        this.where = where;
        this.executeAs = executeAs;
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

        var binder = new SubQueryAndParamBinder(params, subQueryResults);
        ForeignCollectPhase phase = new ForeignCollectPhase(
            plannerContext.jobId(),
            plannerContext.nextExecutionPhaseId(),
            plannerContext.handlerNode(),
            relation.relationName(),
            Lists.map(toCollect, binder),
            where.map(binder).queryOrFallback(),
            executeAs
        );
        return new io.crate.planner.node.dql.Collect(
            phase,
            LimitAndOffset.NO_LIMIT,
            0,
            toCollect.size(),
            LimitAndOffset.NO_LIMIT,
            null
        );
    }

    public ForeignDataWrapper fdw() {
        return fdw;
    }

    public ForeignTableRelation relation() {
        return relation;
    }

    public WhereClause where() {
        return where;
    }

    @Override
    public List<Symbol> outputs() {
        return toCollect;
    }

    @Override
    public List<LogicalPlan> sources() {
        return List.of();
    }

    @Override
    public LogicalPlan replaceSources(List<LogicalPlan> sources) {
        assert sources.isEmpty() : "ForeignCollect has no sources, cannot replace them";
        return this;
    }

    @Override
    public LogicalPlan pruneOutputsExcept(SequencedCollection<Symbol> outputsToKeep) {
        if (outputsToKeep.containsAll(toCollect)) {
            return this;
        }
        return new ForeignCollect(fdw, relation, List.copyOf(outputsToKeep), where, executeAs);
    }

    public String executeAs() {
        return executeAs;
    }

    @Override
    public Map<LogicalPlan, SelectSymbol> dependencies() {
        return Map.of();
    }

    @Override
    public <C, R> R accept(LogicalPlanVisitor<C, R> visitor, C context) {
        return visitor.visitForeignCollect(this, context);
    }

    @Override
    public List<RelationName> relationNames() {
        return List.of(relation.relationName());
    }

    @Override
    public void print(PrintContext printContext) {
        printContext
            .text("ForeignCollect[")
            .text(relation.relationName().toString())
            .text(" | [")
            .text(Lists.joinOn(", ", toCollect, Symbol::toString))
            .text("] | ")
            .text(where.queryOrFallback().toString())
            .text("]");
        printStats(printContext);
    }
}
