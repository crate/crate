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
import io.crate.analyze.relations.AbstractTableRelation;
import io.crate.analyze.symbol.Symbol;
import io.crate.metadata.RowGranularity;
import io.crate.planner.Plan;
import io.crate.planner.Planner;
import io.crate.planner.projection.FilterProjection;
import io.crate.planner.projection.builder.ProjectionBuilder;

import javax.annotation.Nullable;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.crate.planner.operators.LogicalPlanner.extractColumns;

class Filter implements LogicalPlan {

    final LogicalPlan source;
    final QueryClause queryClause;

    static LogicalPlan.Builder create(LogicalPlan.Builder sourceBuilder, @Nullable QueryClause queryClause) {
        if (queryClause == null) {
            return sourceBuilder;
        }
        if (queryClause.hasQuery()) {
            Set<Symbol> columnsInQuery = extractColumns(queryClause.query());
            return usedColumns -> {
                Set<Symbol> allUsedColumns = new HashSet<>();
                allUsedColumns.addAll(columnsInQuery);
                allUsedColumns.addAll(usedColumns);
                return new Filter(sourceBuilder.build(allUsedColumns), queryClause);
            };
        }
        if (queryClause.noMatch()) {
            return usedColumns -> new Filter(sourceBuilder.build(usedColumns), WhereClause.NO_MATCH);
        }
        return sourceBuilder;
    }

    Filter(LogicalPlan source, QueryClause queryClause) {
        this.source = source;
        this.queryClause = queryClause;
    }

    @Override
    public Plan build(Planner.Context plannerContext,
                      ProjectionBuilder projectionBuilder,
                      int limit,
                      int offset,
                      @Nullable OrderBy order,
                      @Nullable Integer pageSizeHint) {
        Plan plan = source.build(plannerContext, projectionBuilder, limit, offset, order, pageSizeHint);
        FilterProjection filterProjection = ProjectionBuilder.filterProjection(source.outputs(), queryClause);
        if (plan.resultDescription().executesOnShard()) {
            filterProjection.requiredGranularity(RowGranularity.SHARD);
        }
        plan.addProjection(filterProjection);
        return plan;
    }

    @Override
    public LogicalPlan tryCollapse() {
        return this;
    }

    @Override
    public List<Symbol> outputs() {
        return source.outputs();
    }

    @Override
    public Map<Symbol, Symbol> expressionMapping() {
        return source.expressionMapping();
    }

    @Override
    public List<AbstractTableRelation> baseTables() {
        return source.baseTables();
    }
}
