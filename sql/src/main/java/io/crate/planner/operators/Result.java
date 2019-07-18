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
import io.crate.analyze.WhereClause;
import io.crate.analyze.relations.AbstractTableRelation;
import io.crate.analyze.relations.ValuesRelation;
import io.crate.common.collections.Lists2;
import io.crate.data.Row;
import io.crate.execution.dsl.phases.ValuesPhase;
import io.crate.execution.dsl.projection.builder.ProjectionBuilder;
import io.crate.expression.symbol.Field;
import io.crate.expression.symbol.SelectSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.planner.ExecutionPlan;
import io.crate.planner.PlannerContext;
import io.crate.types.FixedWidthType;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

public final class Result implements LogicalPlan {

    public static Builder create(ValuesRelation values, List<Symbol> toCollect, WhereClause where) {
        return ((tableStats, hints, usedBeforeNextFetch) -> new Result(values, toCollect, where));
    }

    private final ValuesRelation values;
    private final List<Symbol> toCollect;
    private final WhereClause where;

    public Result(ValuesRelation values, List<Symbol> toCollect, WhereClause where) {
        this.values = values;
        this.toCollect = toCollect;
        this.where = where;
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
        SubQueryAndParamBinder binder = new SubQueryAndParamBinder(params, subQueryResults);
        List<Symbol> outputs = Lists2.map(toCollect, binder);
        List<List<Symbol>> boundRows = Lists2.map(values.rows(), cells -> Lists2.map(cells, binder));
        return new ValuesPhase(
            plannerContext.jobId(),
            plannerContext.nextExecutionPhaseId(),
            plannerContext.handlerNode(),
            outputs,
            boundRows,
            binder.apply(where.queryOrFallback())
        );
    }

    @Override
    public List<Symbol> outputs() {
        return toCollect;
    }

    @Override
    public Map<Symbol, Symbol> expressionMapping() {
        return Map.of();
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
        assert sources.isEmpty() : "Result has no sources, cannot replace them";
        return this;
    }

    @Override
    public Map<LogicalPlan, SelectSymbol> dependencies() {
        return Map.of();
    }

    @Override
    public long numExpectedRows() {
        return values.rows().size();
    }

    @Override
    public long estimatedRowSize() {
        long estimate = 0;
        for (Field field : values.fields()) {
            if (field.valueType() instanceof FixedWidthType) {
                estimate += ((FixedWidthType) field.valueType()).fixedSize();
            }
        }
        return estimate;
    }

    @Override
    public <C, R> R accept(LogicalPlanVisitor<C, R> visitor, C context) {
        return visitor.visitResult(this, context);
    }
}
