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
import io.crate.execution.dsl.phases.ExecutionPhases;
import io.crate.execution.dsl.projection.TopNProjection;
import io.crate.execution.dsl.projection.builder.ProjectionBuilder;
import io.crate.execution.engine.pipeline.TopN;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.SelectSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.Symbols;
import io.crate.planner.ExecutionPlan;
import io.crate.planner.Merge;
import io.crate.planner.PlannerContext;
import io.crate.planner.ResultDescription;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static io.crate.analyze.SymbolEvaluator.evaluate;
import static io.crate.planner.operators.LogicalPlanner.NO_LIMIT;

public class Limit extends ForwardingLogicalPlan {

    final Symbol limit;
    final Symbol offset;

    static LogicalPlan create(LogicalPlan source, @Nullable Symbol limit, @Nullable Symbol offset) {
        if (limit == null && offset == null) {
            return source;
        } else {
            return new Limit(
                source,
                Objects.requireNonNullElse(limit, Literal.of(-1L)),
                Objects.requireNonNullElse(offset, Literal.of(0)));
        }
    }

    public Limit(LogicalPlan source, Symbol limit, Symbol offset) {
        super(source);
        this.limit = limit;
        this.offset = offset;
    }

    public Symbol limit() {
        return limit;
    }

    public Symbol offset() {
        return offset;
    }

    @Override
    public ExecutionPlan build(PlannerContext plannerContext,
                               ProjectionBuilder projectionBuilder,
                               int limitHint,
                               int offsetHint,
                               @Nullable OrderBy order,
                               @Nullable Integer pageSizeHint,
                               Row params,
                               SubQueryResults subQueryResults) {
        int limit = Objects.requireNonNullElse(
            DataTypes.INTEGER.value(evaluate(
                plannerContext.transactionContext(),
                plannerContext.functions(),
                this.limit,
                params,
                subQueryResults)),
            NO_LIMIT);
        int offset = Objects.requireNonNullElse(
            DataTypes.INTEGER.value(evaluate(
                plannerContext.transactionContext(),
                plannerContext.functions(),
                this.offset,
                params,
                subQueryResults)),
            0);

        ExecutionPlan executionPlan = source.build(
            plannerContext, projectionBuilder, limit, offset, order, pageSizeHint, params, subQueryResults);
        List<DataType> sourceTypes = Symbols.typeView(source.outputs());
        ResultDescription resultDescription = executionPlan.resultDescription();
        if (resultDescription.hasRemainingLimitOrOffset()
            && (resultDescription.limit() != limit || resultDescription.offset() != offset)) {

            executionPlan = Merge.ensureOnHandler(executionPlan, plannerContext);
            resultDescription = executionPlan.resultDescription();
        }
        if (ExecutionPhases.executesOnHandler(plannerContext.handlerNode(), resultDescription.nodeIds())) {
            executionPlan.addProjection(
                new TopNProjection(limit, offset, sourceTypes), TopN.NO_LIMIT, 0, resultDescription.orderBy());
        } else if (resultDescription.limit() != limit || resultDescription.offset() != 0) {
            executionPlan.addProjection(
                new TopNProjection(limit + offset, 0, sourceTypes), limit, offset, resultDescription.orderBy());
        }
        return executionPlan;
    }

    @Override
    public LogicalPlan replaceSources(List<LogicalPlan> sources) {
        return new Limit(Lists2.getOnlyElement(sources), limit, offset);
    }

    @Override
    public Map<LogicalPlan, SelectSymbol> dependencies() {
        return source.dependencies();
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

    @Override
    public <C, R> R accept(LogicalPlanVisitor<C, R> visitor, C context) {
        return visitor.visitLimit(this, context);
    }

    @Override
    public void print(PrintContext printContext) {
        printContext
            .text("Limit[")
            .text(limit.toString())
            .text(";")
            .text(offset.toString())
            .text("]")
            .nest(source::print);
    }

    static int limitAndOffset(int limit, int offset) {
        if (limit == TopN.NO_LIMIT) {
            return limit;
        }
        return limit + offset;
    }
}
