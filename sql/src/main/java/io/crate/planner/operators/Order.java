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
import io.crate.common.collections.Lists2;
import io.crate.data.Row;
import io.crate.execution.dsl.projection.OrderedTopNProjection;
import io.crate.execution.dsl.projection.builder.InputColumns;
import io.crate.execution.dsl.projection.builder.ProjectionBuilder;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.format.SymbolFormatter;
import io.crate.planner.ExecutionPlan;
import io.crate.planner.Merge;
import io.crate.planner.PlannerContext;
import io.crate.planner.PositionalOrderBy;

import javax.annotation.Nullable;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static io.crate.planner.operators.LogicalPlanner.extractColumns;

public class Order extends ForwardingLogicalPlan {

    final OrderBy orderBy;
    private final List<Symbol> outputs;

    static LogicalPlan.Builder create(LogicalPlan.Builder source, @Nullable OrderBy orderBy) {
        if (orderBy == null) {
            return source;
        }
        return (tableStats, hints, usedColumns) -> {
            Set<Symbol> allUsedColumns = new LinkedHashSet<>();
            allUsedColumns.addAll(orderBy.orderBySymbols());
            allUsedColumns.addAll(usedColumns);
            return new Order(source.build(tableStats, hints, allUsedColumns), orderBy);
        };
    }

    public Order(LogicalPlan source, OrderBy orderBy) {
        super(source);
        this.outputs = Lists2.concatUnique(source.outputs(), orderBy.orderBySymbols());
        this.orderBy = orderBy;
    }

    public OrderBy orderBy() {
        return orderBy;
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
        ExecutionPlan plan = source.build(
            plannerContext, projectionBuilder, limit, offset, orderBy, pageSizeHint, params, subQueryResults);
        if (plan.resultDescription().orderBy() != null) {
            // Collect applied ORDER BY eagerly to produce a optimized execution plan;
            if (source instanceof Collect) {
                return plan;
            }
        }
        if (plan.resultDescription().hasRemainingLimitOrOffset()) {
            plan = Merge.ensureOnHandler(plan, plannerContext);
        }
        InputColumns.SourceSymbols ctx = new InputColumns.SourceSymbols(source.outputs());
        ensureOrderByColumnsArePresentInOutputs(source.outputs(), orderBy.orderBySymbols());
        OrderedTopNProjection topNProjection = new OrderedTopNProjection(
            Limit.limitAndOffset(limit, offset),
            0,
            InputColumns.create(outputs, ctx),
            InputColumns.create(orderBy.orderBySymbols(), ctx),
            orderBy.reverseFlags(),
            orderBy.nullsFirst()
        );
        PositionalOrderBy positionalOrderBy = PositionalOrderBy.of(orderBy, outputs);
        plan.addProjection(
            topNProjection,
            limit,
            offset,
            positionalOrderBy
        );
        return plan;
    }

    @Override
    public List<Symbol> outputs() {
        return outputs;
    }

    @Override
    public LogicalPlan replaceSources(List<LogicalPlan> sources) {
        return new Order(Lists2.getOnlyElement(sources), orderBy);
    }

    private static void ensureOrderByColumnsArePresentInOutputs(List<Symbol> outputs, List<Symbol> orderBySymbols) {
        Set<Symbol> columnsInOutputs = extractColumns(outputs);
        for (Symbol columnInOrderBy : extractColumns(orderBySymbols)) {
            if (!columnsInOutputs.contains(columnInOrderBy)) {
                throw new UnsupportedOperationException(SymbolFormatter.format(
                    "Cannot order by \"%s\", as the column does not appear in the outputs of the underlying relation",
                    columnInOrderBy));
            }
        }
    }

    @Override
    public String toString() {
        return "Order{" +
               "src=" + source +
               ", " + orderBy +
               '}';
    }

    @Override
    public <C, R> R accept(LogicalPlanVisitor<C, R> visitor, C context) {
        return visitor.visitOrder(this, context);
    }
}
