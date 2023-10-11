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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jetbrains.annotations.Nullable;

import io.crate.analyze.OrderBy;
import io.crate.analyze.SymbolEvaluator;
import io.crate.analyze.WhereClause;
import io.crate.analyze.relations.AbstractTableRelation;
import io.crate.analyze.relations.TableFunctionRelation;
import io.crate.common.collections.Lists2;
import io.crate.data.Row;
import io.crate.execution.dsl.phases.TableFunctionCollectPhase;
import io.crate.execution.dsl.projection.builder.ProjectionBuilder;
import io.crate.execution.engine.pipeline.LimitAndOffset;
import io.crate.expression.eval.EvaluatingNormalizer;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.SelectSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.RelationName;
import io.crate.metadata.RowGranularity;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.ExecutionPlan;
import io.crate.planner.PlannerContext;
import io.crate.planner.node.dql.Collect;


public final class TableFunction implements LogicalPlan {

    private final TableFunctionRelation relation;
    private final List<Symbol> toCollect;
    final WhereClause where;

    public static LogicalPlan create(TableFunctionRelation relation, List<Symbol> toCollect, WhereClause where) {
        return new TableFunction(relation, toCollect, where);
    }

    public TableFunction(TableFunctionRelation relation, List<Symbol> toCollect, WhereClause where) {
        this.relation = relation;
        this.toCollect = toCollect;
        this.where = where;
    }

    public TableFunctionRelation relation() {
        return relation;
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
        List<Symbol> args = relation.function().arguments();
        ArrayList<Literal<?>> functionArguments = new ArrayList<>(args.size());
        EvaluatingNormalizer normalizer = new EvaluatingNormalizer(
            plannerContext.nodeContext(),
            RowGranularity.CLUSTER,
            null,
            relation
        );
        var binder = new SubQueryAndParamBinder(params, subQueryResults)
            .andThen(x -> normalizer.normalize(x, plannerContext.transactionContext()));
        for (Symbol arg : args) {
            // It's not possible to use columns as argument to a table function, so it's safe to evaluate at this point.
            functionArguments.add(
                Literal.ofUnchecked(
                    arg.valueType(),
                    SymbolEvaluator.evaluate(
                        plannerContext.transactionContext(), plannerContext.nodeContext(), arg, params, subQueryResults)
                )
            );
        }
        TableFunctionCollectPhase collectPhase = new TableFunctionCollectPhase(
            plannerContext.jobId(),
            plannerContext.nextExecutionPhaseId(),
            plannerContext.handlerNode(),
            relation.functionImplementation(),
            functionArguments,
            Lists2.map(toCollect, binder),
            binder.apply(where.queryOrFallback())
        );
        return new Collect(collectPhase, LimitAndOffset.NO_LIMIT, 0, toCollect.size(), LimitAndOffset.NO_LIMIT, null);
    }

    @Override
    public List<Symbol> outputs() {
        return toCollect;
    }

    @Override
    public List<AbstractTableRelation<?>> baseTables() {
        return List.of();
    }

    @Override
    public List<RelationName> getRelationNames() {
        return List.of(relation.relationName());
    }

    @Override
    public List<LogicalPlan> sources() {
        return List.of();
    }

    @Override
    public LogicalPlan replaceSources(List<LogicalPlan> sources) {
        assert sources.isEmpty() : "TableFunction has no sources, cannot replace them";
        return this;
    }

    @Override
    public LogicalPlan pruneOutputsExcept(Collection<Symbol> outputsToKeep) {
        if (outputsToKeep.containsAll(toCollect)) {
            return this;
        }
        return new TableFunction(relation, List.copyOf(outputsToKeep), where);
    }

    @Override
    public Map<LogicalPlan, SelectSymbol> dependencies() {
        return Map.of();
    }

    @Override
    public <C, R> R accept(LogicalPlanVisitor<C, R> visitor, C context) {
        return visitor.visitTableFunction(this, context);
    }

    @Override
    public void print(PrintContext printContext) {
        printContext
            .text("TableFunction[")
            .text(relation.function().name())
            .text(" | [")
            .text(Lists2.joinOn(", ", toCollect, Symbol::toString))
            .text("] | ")
            .text(where.queryOrFallback().toString())
            .text("]");
        printStats(printContext);
    }

    @Override
    public String toString() {
        return "TableFunction{" +
            "relation=" + relation +
            ", toCollect=" + toCollect +
            ", where=" + where +
            '}';
    }
}
