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
import io.crate.analyze.WhereClause;
import io.crate.data.Row;
import io.crate.execution.dsl.projection.FilterProjection;
import io.crate.execution.dsl.projection.builder.ProjectionBuilder;
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

    final QueryClause queryClause;

    static LogicalPlan.Builder create(LogicalPlan.Builder sourceBuilder, @Nullable QueryClause queryClause) {
        if (queryClause == null) {
            return sourceBuilder;
        }
        if (queryClause.hasQuery()) {
            Set<Symbol> columnsInQuery = extractColumns(queryClause.query());
            return (tableStats, usedColumns) -> {
                Set<Symbol> allUsedColumns = new LinkedHashSet<>();
                allUsedColumns.addAll(columnsInQuery);
                allUsedColumns.addAll(usedColumns);
                return new Filter(sourceBuilder.build(tableStats, allUsedColumns), queryClause);
            };
        }
        if (queryClause.noMatch()) {
            return (tableStats, usedColumns) ->
                new Filter(sourceBuilder.build(tableStats, usedColumns), WhereClause.NO_MATCH);
        }
        return sourceBuilder;
    }

    public static LogicalPlan create(LogicalPlan source, Symbol query) {
        assert query.valueType().equals(DataTypes.BOOLEAN)
            : "query must have a boolean result type, got: " + query.valueType();

        if (query.symbolType().isValueSymbol() && ((Literal) query).value() == Boolean.TRUE) {
            return source;
        }
        return new Filter(source, new WhereClause(query));
    }

    private Filter(LogicalPlan source, QueryClause queryClause) {
        super(source);
        this.queryClause = queryClause;
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
        FilterProjection filterProjection = ProjectionBuilder.filterProjection(source.outputs(), queryClause);
        if (executionPlan.resultDescription().executesOnShard()) {
            filterProjection.requiredGranularity(RowGranularity.SHARD);
        }
        executionPlan.addProjection(filterProjection);
        return executionPlan;
    }

    @Override
    protected LogicalPlan updateSource(LogicalPlan newSource, SymbolMapper mapper) {
        return new Filter(newSource, queryClause);
    }

    @Override
    public <C, R> R accept(LogicalPlanVisitor<C, R> visitor, C context) {
        return visitor.visitFilter(this, context);
    }
}
