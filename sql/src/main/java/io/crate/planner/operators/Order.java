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
import io.crate.analyze.symbol.InputColumn;
import io.crate.analyze.symbol.Symbol;
import io.crate.metadata.table.TableInfo;
import io.crate.planner.Plan;
import io.crate.planner.Planner;
import io.crate.planner.PositionalOrderBy;
import io.crate.planner.projection.OrderedTopNProjection;
import io.crate.planner.projection.builder.InputColumns;
import io.crate.planner.projection.builder.ProjectionBuilder;

import javax.annotation.Nullable;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static io.crate.planner.operators.LogicalPlanner.NO_LIMIT;

class Order implements LogicalPlan {

    final LogicalPlan source;
    final OrderBy orderBy;

    static LogicalPlan.Builder create(LogicalPlan.Builder source, @Nullable OrderBy orderBy) {
        if (orderBy == null) {
            return source;
        }
        Set<Symbol> columnsInOrderBy = new HashSet<>(orderBy.orderBySymbols());
        return usedColumns -> {
            columnsInOrderBy.addAll(usedColumns);
            return new Order(source.build(columnsInOrderBy), orderBy);
        };
    }

    private Order(LogicalPlan source, OrderBy orderBy) {
        this.source = source;
        this.orderBy = orderBy;
    }

    @Override
    public Plan build(Planner.Context plannerContext,
                      ProjectionBuilder projectionBuilder,
                      int limit,
                      int offset,
                      @Nullable OrderBy order,
                      @Nullable Integer pageSizeHint) {
        Plan plan = source.build(plannerContext, projectionBuilder, limit, offset, orderBy, pageSizeHint);
        if (plan.resultDescription().orderBy() == null) {
            InputColumns.Context ctx = new InputColumns.Context(source.outputs());
            OrderedTopNProjection topNProjection = new OrderedTopNProjection(
                limit,
                offset,
                InputColumn.fromSymbols(source.outputs()),
                InputColumns.create(orderBy.orderBySymbols(), ctx),
                orderBy.reverseFlags(),
                orderBy.nullsFirst()
            );
            plan.addProjection(
                topNProjection,
                NO_LIMIT,
                0,
                PositionalOrderBy.of(orderBy, source.outputs())
            );
        }
        return plan;
    }

    @Override
    public LogicalPlan tryCollapse() {
        LogicalPlan collapsed = source.tryCollapse();
        if (collapsed == source) {
            return this;
        }
        return new Order(collapsed, orderBy);
    }

    @Override
    public List<Symbol> outputs() {
        return source.outputs();
    }

    @Override
    public List<TableInfo> baseTables() {
        return source.baseTables();
    }

    @Override
    public String toString() {
        return "Order{" +
               "src=" + source +
               ", " + orderBy +
               '}';
    }
}
