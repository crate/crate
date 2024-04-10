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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SequencedCollection;
import java.util.SequencedMap;
import java.util.Set;

import org.jetbrains.annotations.Nullable;

import io.crate.analyze.OrderBy;
import io.crate.common.collections.Lists;
import io.crate.common.collections.Maps;
import io.crate.data.Row;
import io.crate.execution.dsl.projection.builder.ProjectionBuilder;
import io.crate.expression.symbol.SelectSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.RelationName;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.ExecutionPlan;
import io.crate.planner.MultiPhasePlan;
import io.crate.planner.PlannerContext;

/**
 * This is the {@link LogicalPlan} equivalent of the {@link MultiPhasePlan} plan.
 * It's used to describe that other logical plans needed to be executed first.
  */
public class MultiPhase implements LogicalPlan {

    private final LogicalPlan source;
    private final SequencedMap<LogicalPlan, SelectSymbol> subQueries;

    public static LogicalPlan createIfNeeded(SequencedMap<LogicalPlan, SelectSymbol> uncorrelatedSubQueries, LogicalPlan source) {
        if (uncorrelatedSubQueries.isEmpty()) {
            return source;
        } else {
            return new MultiPhase(source, uncorrelatedSubQueries);
        }
    }

    private MultiPhase(LogicalPlan source, SequencedMap<LogicalPlan, SelectSymbol> subQueries) {
        this.source = source;
        this.subQueries = subQueries;
    }

    @Override
    public ExecutionPlan build(DependencyCarrier executor,
                               PlannerContext plannerContext,
                               Set<PlanHint> planHints,
                               ProjectionBuilder projectionBuilder,
                               int limit,
                               int offset,
                               @Nullable OrderBy order,
                               @Nullable Integer pageSizeHint,
                               Row params,
                               SubQueryResults subQueryResults) {
        return source.build(
            executor, plannerContext, planHints, projectionBuilder, limit, offset, order, pageSizeHint, params, subQueryResults);
    }

    @Override
    public List<Symbol> outputs() {
        return source.outputs();
    }

    @Override
    public List<LogicalPlan> sources() {
        var result = new ArrayList<LogicalPlan>();
        result.add(source);
        result.addAll(subQueries.sequencedKeySet());
        return result;
    }

    @Override
    public LogicalPlan replaceSources(List<LogicalPlan> sources) {
        assert sources.size() == subQueries.size() + 1 : "Invalid number of sources for MultiPhase";
        var iterator = sources.iterator();
        var newSource = iterator.next();
        SequencedMap<LogicalPlan, SelectSymbol> newSubqueries = new LinkedHashMap<>();
        for (var entries : this.subQueries.sequencedEntrySet()) {
            newSubqueries.put(iterator.next(), entries.getValue());
        }
        return new MultiPhase(newSource, newSubqueries);
    }

    @Override
    public LogicalPlan pruneOutputsExcept(SequencedCollection<Symbol> outputsToKeep) {
        LogicalPlan newSource = source.pruneOutputsExcept(outputsToKeep);
        if (newSource == source) {
            return this;
        }
        var newSources = new ArrayList<LogicalPlan>();
        newSources.add(source);
        newSources.addAll(subQueries.sequencedKeySet());
        return replaceSources(newSources);
    }

    @Override
    public Map<LogicalPlan, SelectSymbol> dependencies() {
        return Maps.concat(source.dependencies(), subQueries);
    }

    @Override
    public <C, R> R accept(LogicalPlanVisitor<C, R> visitor, C context) {
        return visitor.visitMultiPhase(this, context);
    }

    @Override
    public List<RelationName> relationNames() {
        var result = new ArrayList<RelationName>();
        result.addAll(source.relationNames());
        for (var subQueries : subQueries.sequencedKeySet()) {
            result.addAll(subQueries.relationNames());
        }
        return source.relationNames();
    }

    @Override
    public void print(PrintContext printContext) {
        printContext
            .text("MultiPhase");
        printStats(printContext);
        printContext.nest(source::print)
            .nest(Lists.map(subQueries.keySet(), x -> x::print));
    }
}
