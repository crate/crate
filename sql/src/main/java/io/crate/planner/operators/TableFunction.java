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

import io.crate.analyze.OrderBy;
import io.crate.analyze.SymbolEvaluator;
import io.crate.analyze.WhereClause;
import io.crate.analyze.relations.AbstractTableRelation;
import io.crate.analyze.relations.TableFunctionRelation;
import io.crate.common.collections.Lists2;
import io.crate.data.Row;
import io.crate.execution.dsl.phases.TableFunctionCollectPhase;
import io.crate.execution.dsl.projection.builder.ProjectionBuilder;
import io.crate.execution.engine.pipeline.TopN;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.SelectSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.planner.ExecutionPlan;
import io.crate.planner.PlannerContext;
import io.crate.planner.node.dql.Collect;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


public final class TableFunction implements LogicalPlan {

    private final TableFunctionRelation relation;
    private final List<Symbol> toCollect;
    final WhereClause where;

    public static Builder create(TableFunctionRelation relation, List<Symbol> toCollect, WhereClause where) {
        return (tableStats, hints, params) -> new TableFunction(relation, toCollect, where);
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
    public ExecutionPlan build(PlannerContext plannerContext,
                               ProjectionBuilder projectionBuilder,
                               int limit,
                               int offset,
                               @Nullable OrderBy order,
                               @Nullable Integer pageSizeHint,
                               Row params,
                               SubQueryResults subQueryResults) {
        List<Symbol> args = relation.function().arguments();
        ArrayList<Literal<?>> functionArguments = new ArrayList<>(args.size());
        SubQueryAndParamBinder paramBinder = new SubQueryAndParamBinder(params, subQueryResults);
        for (Symbol arg : args) {
            // It's not possible to use columns as argument to a table function, so it's safe to evaluate at this point.
            functionArguments.add(
                Literal.ofUnchecked(
                    arg.valueType(),
                    SymbolEvaluator.evaluate(
                        plannerContext.transactionContext(), plannerContext.functions(), arg, params, subQueryResults)
                )
            );
        }
        TableFunctionCollectPhase collectPhase = new TableFunctionCollectPhase(
            plannerContext.jobId(),
            plannerContext.nextExecutionPhaseId(),
            plannerContext.handlerNode(),
            relation.functionImplementation(),
            functionArguments,
            Lists2.map(toCollect, paramBinder),
            paramBinder.apply(where.queryOrFallback())
        );
        return new Collect(collectPhase, TopN.NO_LIMIT, 0, toCollect.size(), TopN.NO_LIMIT, null);
    }

    @Override
    public List<Symbol> outputs() {
        return toCollect;
    }

    @Override
    public List<AbstractTableRelation> baseTables() {
        return List.of();
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
    public Map<LogicalPlan, SelectSymbol> dependencies() {
        return Map.of();
    }

    @Override
    public long numExpectedRows() {
        return -1;
    }

    @Override
    public long estimatedRowSize() {
        // We don't have any estimates for table functions, but could go through the types of `outputs` to make a guess
        return 0;
    }

    @Override
    public <C, R> R accept(LogicalPlanVisitor<C, R> visitor, C context) {
        return visitor.visitTableFunction(this, context);
    }
}
