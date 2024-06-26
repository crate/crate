/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.planner.operators;

import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.SequencedCollection;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;

import org.jetbrains.annotations.Nullable;

import io.crate.analyze.OrderBy;
import io.crate.common.collections.Lists;
import io.crate.data.Row;
import io.crate.execution.dsl.projection.OrderedLimitAndOffsetProjection;
import io.crate.execution.dsl.projection.builder.InputColumns;
import io.crate.execution.dsl.projection.builder.ProjectionBuilder;
import io.crate.expression.symbol.FieldsVisitor;
import io.crate.expression.symbol.RefVisitor;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.Symbols;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.ExecutionPlan;
import io.crate.planner.Merge;
import io.crate.planner.PlannerContext;
import io.crate.planner.PositionalOrderBy;

public class Order extends ForwardingLogicalPlan {

    final OrderBy orderBy;
    private final List<Symbol> outputs;

    static LogicalPlan create(LogicalPlan source, @Nullable OrderBy orderBy) {
        if (orderBy == null) {
            return source;
        } else {
            return new Order(source, orderBy);
        }
    }

    public Order(LogicalPlan source, OrderBy orderBy) {
        super(source);
        this.outputs = Lists.concatUnique(source.outputs(), orderBy.orderBySymbols());
        this.orderBy = orderBy;
    }

    public OrderBy orderBy() {
        return orderBy;
    }

    @Override
    public LogicalPlan pruneOutputsExcept(SequencedCollection<Symbol> outputsToKeep) {
        LinkedHashSet<Symbol> toKeep = new LinkedHashSet<>();
        for (Symbol outputToKeep : outputsToKeep) {
            Symbols.intersection(outputToKeep, source.outputs(), toKeep::add);
        }
        for (Symbol orderBySymbol : orderBy.orderBySymbols()) {
            Symbols.intersection(orderBySymbol, source.outputs(), toKeep::add);
        }
        LogicalPlan newSource = source.pruneOutputsExcept(toKeep);
        if (newSource == source) {
            return this;
        }
        return replaceSources(List.of(newSource));
    }

    @Nullable
    @Override
    public FetchRewrite rewriteToFetch(Collection<Symbol> usedColumns) {
        HashSet<Symbol> allUsedColumns = new HashSet<>(usedColumns);
        allUsedColumns.addAll(orderBy.orderBySymbols());
        FetchRewrite fetchRewrite = source.rewriteToFetch(allUsedColumns);
        if (fetchRewrite == null) {
            return null;
        }
        LogicalPlan newSource = fetchRewrite.newPlan();
        Order newOrderBy = new Order(newSource, orderBy);
        Map<Symbol, Symbol> replacedOutputs = fetchRewrite.replacedOutputs();
        if (newOrderBy.outputs.size() > newSource.outputs().size()) {
            // This is the case if the `orderBy` contains computations on top of the source outputs.
            // e.g. OrderBy [x + y] where the source provides [x, y]
            // We need to extend replacedOutputs in this case because it must always contain entries for all outputs
            LinkedHashMap<Symbol, Symbol> newReplacedOutputs = new LinkedHashMap<>(replacedOutputs);
            UnaryOperator<Symbol> mapToFetchStubs = fetchRewrite.mapToFetchStubs();
            for (int i = newSource.outputs().size(); i < newOrderBy.outputs.size(); i++) {
                Symbol extraOutput = newOrderBy.outputs.get(i);
                newReplacedOutputs.put(extraOutput, mapToFetchStubs.apply(extraOutput));
            }
            return new FetchRewrite(newReplacedOutputs, newOrderBy);
        } else {
            return new FetchRewrite(replacedOutputs, newOrderBy);
        }
    }

    @Override
    public ExecutionPlan build(DependencyCarrier executor,
                               PlannerContext plannerContext,
                               Set<PlanHint> planHints,
                               ProjectionBuilder projectionBuilder,
                               int limit,
                               int offset,
                               @Nullable OrderBy order,
                               @Nullable Integer pageSizeHint,
                               Row params,
                               SubQueryResults subQueryResults) {
        ExecutionPlan plan = source.build(
            executor, plannerContext, planHints, projectionBuilder, limit, offset, orderBy, pageSizeHint, params, subQueryResults);
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
        List<Symbol> orderByInputColumns = InputColumns.create(this.orderBy.orderBySymbols(), ctx);
        ensureOrderByColumnsArePresentInOutputs(orderByInputColumns);
        OrderedLimitAndOffsetProjection orderedLimitAndOffsetProjection = new OrderedLimitAndOffsetProjection(
            Limit.limitAndOffset(limit, offset),
            0,
            InputColumns.create(outputs, ctx),
            orderByInputColumns,
            this.orderBy.reverseFlags(),
            this.orderBy.nullsFirst()
        );
        PositionalOrderBy positionalOrderBy = PositionalOrderBy.of(this.orderBy, outputs);
        plan.addProjection(
            orderedLimitAndOffsetProjection,
            limit,
            offset,
            positionalOrderBy
        );
        return plan;
    }

    private static void ensureOrderByColumnsArePresentInOutputs(List<Symbol> orderByInputColumns) {
        Consumer<? super Symbol> raiseExpressionMissingInOutputsError = symbol -> {
            throw new UnsupportedOperationException(
                    Symbols.format(
                    "Cannot ORDER BY `%s`, the column does not appear in the outputs of the underlying relation",
                    symbol));
        };
        for (Symbol orderByInputColumn : orderByInputColumns) {
            FieldsVisitor.visitFields(orderByInputColumn, raiseExpressionMissingInOutputsError);
            RefVisitor.visitRefs(orderByInputColumn, raiseExpressionMissingInOutputsError);
        }
    }

    @Override
    public List<Symbol> outputs() {
        return outputs;
    }

    @Override
    public LogicalPlan replaceSources(List<LogicalPlan> sources) {
        return new Order(Lists.getOnlyElement(sources), orderBy);
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

    @Override
    public void print(PrintContext printContext) {
        StringBuilder orderByExprRepr = new StringBuilder();
        OrderBy.explainRepresentation(
            orderByExprRepr,
            orderBy.orderBySymbols(),
            orderBy.reverseFlags(),
            orderBy.nullsFirst(),
            Symbol::toString
        );
        printContext
            .text("OrderBy[")
            .text(orderByExprRepr.toString())
            .text("]");
        printStats(printContext);
        printContext.nest(source::print);
    }
}
