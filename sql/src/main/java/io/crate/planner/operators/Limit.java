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
import io.crate.analyze.relations.AbstractTableRelation;
import io.crate.analyze.symbol.InputColumn;
import io.crate.analyze.symbol.Literal;
import io.crate.analyze.symbol.Symbol;
import io.crate.operation.projectors.TopN;
import io.crate.planner.Plan;
import io.crate.planner.Planner;
import io.crate.planner.ResultDescription;
import io.crate.planner.node.ExecutionPhases;
import io.crate.planner.projection.TopNProjection;
import io.crate.planner.projection.builder.ProjectionBuilder;
import io.crate.types.DataTypes;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

import static com.google.common.base.MoreObjects.firstNonNull;

class Limit implements LogicalPlan {

    final LogicalPlan source;
    final Symbol limit;
    final Symbol offset;

    static LogicalPlan.Builder create(LogicalPlan.Builder source, @Nullable Symbol limit, @Nullable Symbol offset) {
        if (limit == null && offset == null) {
            return source;
        }
        return (tableStats, usedColumns) -> new Limit(
            source.build(tableStats, usedColumns),
            firstNonNull(limit, Literal.of(-1L)),
            firstNonNull(offset, Literal.of(0L))
        );
    }

    private Limit(LogicalPlan source, Symbol limit, Symbol offset) {
        this.source = source;
        this.limit = limit;
        this.offset = offset;
    }

    @Override
    public Plan build(Planner.Context plannerContext,
                      ProjectionBuilder projectionBuilder,
                      int limitHint,
                      int offsetHint,
                      @Nullable OrderBy order,
                      @Nullable Integer pageSizeHint) {
        int limit = firstNonNull(plannerContext.toInteger(this.limit), LogicalPlanner.NO_LIMIT);
        int offset = firstNonNull(plannerContext.toInteger(this.offset), 0);

        Plan plan = source.build(plannerContext, projectionBuilder, limit, offset, order, pageSizeHint);
        List<Symbol> inputCols = InputColumn.fromSymbols(source.outputs());
        ResultDescription resultDescription = plan.resultDescription();
        if (ExecutionPhases.executesOnHandler(plannerContext.handlerNode(), resultDescription.nodeIds())) {
            plan.addProjection(
                new TopNProjection(limit, offset, inputCols), TopN.NO_LIMIT, 0, resultDescription.orderBy());
        } else if (resultDescription.limit() != limit || resultDescription.offset() != 0) {
            plan.addProjection(
                new TopNProjection(limit + offset, 0, inputCols), limit, offset, resultDescription.orderBy());
        }
        return plan;
    }

    @Override
    public LogicalPlan tryCollapse() {
        LogicalPlan collapsed = source.tryCollapse();
        if (collapsed == source) {
            return this;
        }
        return new Limit(collapsed, limit, offset);
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

    @Override
    public long numExpectedRows() {
        if (limit instanceof Literal) {
            return DataTypes.LONG.value(((Literal) limit).value());
        }
        return source.numExpectedRows();
    }

    @Override
    public String toString() {
        return "Limit{" +
               "source=" + source +
               ", limit=" + limit +
               ", offset=" + offset +
               '}';
    }

    static int limitAndOffset(int limit, int offset) {
        if (limit == TopN.NO_LIMIT) {
            return limit;
        }
        return limit + offset;
    }
}
