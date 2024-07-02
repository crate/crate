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

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.SequencedCollection;
import java.util.Set;

import io.crate.analyze.OrderBy;
import io.crate.common.collections.Lists;
import io.crate.data.Row;
import io.crate.execution.dsl.projection.CorrelatedJoinProjection;
import io.crate.execution.dsl.projection.builder.ProjectionBuilder;
import io.crate.execution.engine.pipeline.LimitAndOffset;
import io.crate.expression.symbol.OuterColumn;
import io.crate.expression.symbol.SelectSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.RelationName;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.ExecutionPlan;
import io.crate.planner.Merge;
import io.crate.planner.PlannerContext;

/**
 * Operator that takes two relations.
 * One input relation providing rows, and a second relation that is a correlated subquery.
 * For each row in the input relation it binds and executes the correlated subquery, joining the result.
 *
 * <pre>
 * {@code
 * SELECT (SELECT t.mountain) FROM sys.summits t
 *        ^^^^^^^^^^^^^^^^^^^      ^^^^^^^^^^^^^
 *        SubQuery                 Input
 *                                  output: [mountain]
 *
 *
 *         CorrelatedJoin (output: [mountain, (SELECT t.mountain)]
 *             /     \              ^^^^^^^^  ^^^^^^^^^^^^^^^^^^^
 *         Input     SubQuery       │         └ SelectSymbol
 *                                  └─ ScopedSymbol
 * }
 * </pre>
 *
 **/
public class CorrelatedJoin implements LogicalPlan {

    private final LogicalPlan inputPlan;
    private final LogicalPlan subQueryPlan;

    private final List<Symbol> outputs;
    private final SelectSymbol selectSymbol;

    public CorrelatedJoin(LogicalPlan inputPlan,
                          SelectSymbol selectSymbol,
                          LogicalPlan subQueryPlan) {
        this(inputPlan, selectSymbol, subQueryPlan, Lists.concat(inputPlan.outputs(), selectSymbol));
    }

    private CorrelatedJoin(LogicalPlan inputPlan,
                           SelectSymbol selectSymbol,
                           LogicalPlan subQueryPlan,
                           List<Symbol> outputs) {
        this.inputPlan = inputPlan;
        this.subQueryPlan = subQueryPlan;
        this.selectSymbol = selectSymbol;
        this.outputs = outputs;
    }

    @Override
    public ExecutionPlan build(DependencyCarrier executor,
                               PlannerContext plannerContext,
                               Set<PlanHint> planHints,
                               ProjectionBuilder projectionBuilder,
                               int limit,
                               int offset,
                               OrderBy order,
                               Integer pageSizeHint,
                               Row params,
                               SubQueryResults subQueryResults) {
        ExecutionPlan sourcePlan = Merge.ensureOnHandler(
            inputPlan.build(
                executor,
                plannerContext,
                planHints,
                projectionBuilder,
                LimitAndOffset.NO_LIMIT,
                LimitAndOffset.NO_OFFSET,
                null,
                null,
                params,
                subQueryResults
            ),
            plannerContext
        );
        var projection = new CorrelatedJoinProjection(
            executor,
            subQueryPlan,
            selectSymbol,
            plannerContext,
            subQueryResults,
            params,
            inputPlan.outputs(),
            outputs
        );
        sourcePlan.addProjection(projection);
        return sourcePlan;
    }

    @Override
    public List<Symbol> outputs() {
        return outputs;
    }

    @Override
    public List<LogicalPlan> sources() {
        return List.of(inputPlan);
    }

    @Override
    public LogicalPlan replaceSources(List<LogicalPlan> sources) {
        return new CorrelatedJoin(
            Lists.getOnlyElement(sources),
            selectSymbol,
            subQueryPlan
        );
    }

    @Override
    public LogicalPlan pruneOutputsExcept(SequencedCollection<Symbol> outputsToKeep) {
        var toCollect = new LinkedHashSet<>(outputsToKeep);
        selectSymbol.relation().visitSymbols(tree ->
            tree.visit(OuterColumn.class, outerColumn -> toCollect.add(outerColumn.symbol()))
        );
        LogicalPlan newInputPlan = inputPlan.pruneOutputsExcept(toCollect);

        if (inputPlan == newInputPlan) {
            return this;
        }

        return new CorrelatedJoin(
            newInputPlan,
            selectSymbol,
            subQueryPlan,
            Lists.concat(newInputPlan.outputs(), selectSymbol)
        );
    }

    @Override
    public Map<LogicalPlan, SelectSymbol> dependencies() {
        return inputPlan.dependencies();
    }

    @Override
    public <C, R> R accept(LogicalPlanVisitor<C, R> visitor, C context) {
        return visitor.visitCorrelatedJoin(this, context);
    }

    @Override
    public List<RelationName> relationNames() {
        return inputPlan.relationNames();
    }

    @Override
    public void print(PrintContext printContext) {
        printContext
            .text(getClass().getSimpleName())
            .text("[")
            .text(Lists.joinOn(", ", outputs(), Symbol::toString))
            .text("]");
        printStats(printContext);
        printContext.nest(Lists.map(sources(), x -> x::print))
            .nest(p -> {
                p.text("SubPlan");
                p.nest(subQueryPlan::print);
            });
    }
}
