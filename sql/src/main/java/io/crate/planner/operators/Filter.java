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
import io.crate.common.collections.Lists2;
import io.crate.data.Row;
import io.crate.execution.dsl.projection.FilterProjection;
import io.crate.execution.dsl.projection.builder.ProjectionBuilder;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.RowGranularity;
import io.crate.planner.ExecutionPlan;
import io.crate.planner.PlannerContext;
import io.crate.types.DataTypes;

import javax.annotation.Nullable;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static io.crate.planner.operators.LogicalPlanner.extractColumns;

public final class Filter extends ForwardingLogicalPlan {

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
        return (tableStats, hints, usedColumns) -> {
            Set<Symbol> allUsedColumns = new LinkedHashSet<>();
            allUsedColumns.addAll(columnsInQuery);
            allUsedColumns.addAll(usedColumns);
            return new Filter(sourceBuilder.build(tableStats, hints, allUsedColumns), query);
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

    public Filter(LogicalPlan source, Symbol query) {
        super(source);
        this.query = query;
    }

    public Symbol query() {
        return query;
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
        ExecutionPlan executionPlan = source.build(
            plannerContext, projectionBuilder, limit, offset, order, pageSizeHint, params, subQueryResults);
        Symbol boundQuery = SubQueryAndParamBinder.convert(query, params, subQueryResults);
        FilterProjection filterProjection = ProjectionBuilder.filterProjection(source.outputs(), boundQuery);
        if (executionPlan.resultDescription().executesOnShard()) {
            filterProjection.requiredGranularity(RowGranularity.SHARD);
        }
        executionPlan.addProjection(filterProjection);
        return executionPlan;
    }

    @Override
    public LogicalPlan replaceSources(List<LogicalPlan> sources) {
        return new Filter(Lists2.getOnlyElement(sources), query);
    }

    @Override
    public <C, R> R accept(LogicalPlanVisitor<C, R> visitor, C context) {
        return visitor.visitFilter(this, context);
    }
}
