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
import io.crate.analyze.QueryClause;
import io.crate.data.Row;
import io.crate.execution.dsl.projection.FilterProjection;
import io.crate.execution.dsl.projection.builder.ProjectionBuilder;
import io.crate.expression.operator.AndOperator;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.SelectSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.RowGranularity;
import io.crate.planner.ExecutionPlan;
import io.crate.planner.PlannerContext;
import io.crate.types.DataTypes;

import javax.annotation.Nullable;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import static io.crate.planner.operators.LogicalPlanner.extractColumns;

class Filter extends OneInputPlan {

    final Symbol query;

    static LogicalPlan.Builder create(LogicalPlan.Builder sourceBuilder, @Nullable QueryClause queryClause) {
        if (queryClause == null) {
            return sourceBuilder;
        }
        Symbol query = queryClause.queryOrFallback();
        if (isMatchAll(query)) {
            return sourceBuilder;
        }
        Set<Symbol> columnsInQuery = extractColumns(query);
        return (tableStats, usedColumns) -> {
            Set<Symbol> allUsedColumns = new LinkedHashSet<>();
            allUsedColumns.addAll(columnsInQuery);
            allUsedColumns.addAll(usedColumns);
            return new Filter(sourceBuilder.build(tableStats, allUsedColumns), query);
        };
    }

    public static LogicalPlan create(LogicalPlan source, Symbol query) {
        assert query.valueType().equals(DataTypes.BOOLEAN)
            : "query must have a boolean result type, got: " + query.valueType();
        if (isMatchAll(query)) {
            return source;
        }
        return new Filter(source, query);
    }

    private static boolean isMatchAll(Symbol query) {
        return query instanceof Literal && ((Literal) query).value() == Boolean.TRUE;
    }

    private Filter(LogicalPlan source, Symbol query) {
        super(source);
        this.query = query;
    }

    @Override
    public ExecutionPlan build(PlannerContext plannerContext,
                               ProjectionBuilder projectionBuilder,
                               int limit,
                               int offset,
                               @Nullable OrderBy order,
                               @Nullable Integer pageSizeHint,
                               Row params,
                               Map<SelectSymbol, Object> subQueryValues) {
        ExecutionPlan executionPlan = source.build(
            plannerContext, projectionBuilder, limit, offset, order, pageSizeHint, params, subQueryValues);
        FilterProjection filterProjection = ProjectionBuilder.filterProjection(source.outputs(), query);
        if (executionPlan.resultDescription().executesOnShard()) {
            filterProjection.requiredGranularity(RowGranularity.SHARD);
        }
        executionPlan.addProjection(filterProjection);
        return executionPlan;
    }

    @Override
    protected LogicalPlan updateSource(LogicalPlan newSource, SymbolMapper mapper) {
        return new Filter(newSource, query);
    }

    @Override
    public LogicalPlan tryOptimize(@Nullable LogicalPlan pushDown, SymbolMapper mapper) {
        Filter currentFilter = this;
        if (pushDown instanceof Filter) {
            Symbol ancestorQuery = mapper.apply(outputs, ((Filter) pushDown).query);
            currentFilter = new Filter(source, AndOperator.of(ancestorQuery, query));
        }
        LogicalPlan newSourceWithFilter = source.tryOptimize(currentFilter, mapper);
        if (newSourceWithFilter == null) {
            return super.tryOptimize(pushDown, mapper);
        }
        return newSourceWithFilter;
    }

    @Override
    public <C, R> R accept(LogicalPlanVisitor<C, R> visitor, C context) {
        return visitor.visitFilter(this, context);
    }
}
