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

import static io.crate.analyze.SymbolEvaluator.evaluate;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.SequencedCollection;
import java.util.Set;
import java.util.function.Consumer;

import org.jetbrains.annotations.Nullable;

import io.crate.analyze.OrderBy;
import io.crate.common.collections.Lists;
import io.crate.data.Row;
import io.crate.execution.dsl.phases.ExecutionPhases;
import io.crate.execution.dsl.projection.EvalProjection;
import io.crate.execution.dsl.projection.LimitAndOffsetProjection;
import io.crate.execution.dsl.projection.LimitDistinctProjection;
import io.crate.execution.dsl.projection.builder.InputColumns;
import io.crate.execution.dsl.projection.builder.ProjectionBuilder;
import io.crate.execution.engine.pipeline.LimitAndOffset;
import io.crate.expression.symbol.InputColumn;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.Symbols;
import io.crate.metadata.RowGranularity;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.ExecutionPlan;
import io.crate.planner.Merge;
import io.crate.planner.PlannerContext;
import io.crate.types.DataTypes;

public final class LimitDistinct extends ForwardingLogicalPlan {

    private final Symbol limit;
    private final List<Symbol> outputs;
    private final Symbol offset;

    public LimitDistinct(LogicalPlan source, Symbol limit, Symbol offset, List<Symbol> outputs) {
        super(source);
        this.limit = limit;
        this.offset = offset;
        this.outputs = outputs;
    }

    @Override
    public List<Symbol> outputs() {
        return outputs;
    }

    public Symbol limit() {
        return this.limit;
    }

    @Override
    public ExecutionPlan build(DependencyCarrier executor,
                               PlannerContext plannerContext,
                               Set<PlanHint> planHints,
                               ProjectionBuilder projectionBuilder,
                               int limitHint,
                               int offsetHint,
                               @Nullable OrderBy order,
                               @Nullable Integer pageSizeHint,
                               Row params,
                               SubQueryResults subQueryResults) {
        var executionPlan = source.build(
            executor,
            plannerContext,
            planHints,
            projectionBuilder,
            LimitAndOffset.NO_LIMIT,
            LimitAndOffset.NO_OFFSET,
            null,
            null,
            params,
            subQueryResults
        );
        if (executionPlan.resultDescription().hasRemainingLimitOrOffset()) {
            executionPlan = Merge.ensureOnHandler(executionPlan, plannerContext);
        }
        if (!source.outputs().equals(outputs)) {
            EvalProjection evalProjection = new EvalProjection(
                InputColumns.create(outputs, new InputColumns.SourceSymbols(source.outputs()))
            );
            executionPlan.addProjection(evalProjection);
        }
        int limit = DataTypes.INTEGER.sanitizeValue(
            evaluate(
                plannerContext.transactionContext(),
                plannerContext.nodeContext(),
                this.limit,
                params,
                subQueryResults
            )
        );
        int offset = DataTypes.INTEGER.sanitizeValue(
            evaluate(
                plannerContext.transactionContext(),
                plannerContext.nodeContext(),
                this.offset,
                params,
                subQueryResults
            )
        );
        var inputColOutputs = InputColumn.mapToInputColumns(outputs);
        executionPlan.addProjection(
            new LimitDistinctProjection(
                limit + offset,
                inputColOutputs,
                source.preferShardProjections() ? RowGranularity.SHARD : RowGranularity.CLUSTER
            )
        );
        boolean onHandler = ExecutionPhases.executesOnHandler(
            plannerContext.handlerNode(), executionPlan.resultDescription().nodeIds());
        if (!onHandler || source.preferShardProjections()) {
            if (!onHandler) {
                executionPlan = Merge.ensureOnHandler(executionPlan, plannerContext);
            }
            LimitDistinctProjection limitDistinct = new LimitDistinctProjection(
                limit + offset,
                inputColOutputs,
                RowGranularity.CLUSTER
            );
            executionPlan.addProjection(limitDistinct);
        }
        if (offset > 0) {
            // LimitDistinctProjection outputs a distinct result-set,
            // That allows us to use the LimitAndOffsetProjection to apply the offset
            executionPlan.addProjection(
                new LimitAndOffsetProjection(limit, offset, Symbols.typeView(inputColOutputs))
            );
        }
        return executionPlan;
    }

    @Override
    public LogicalPlan pruneOutputsExcept(SequencedCollection<Symbol> outputsToKeep) {
        LinkedHashSet<Symbol> toKeep = new LinkedHashSet<>();
        Consumer<Symbol> keep = toKeep::add;
        // Pruning unused outputs would change semantics. Need to keep all in any case
        for (var output : outputs) {
            Symbols.intersection(output, source.outputs(), keep);
        }
        LogicalPlan prunedSource = source.pruneOutputsExcept(toKeep);
        if (prunedSource == source) {
            return this;
        }
        return new LimitDistinct(prunedSource, limit, offset, outputs);
    }

    @Override
    public LogicalPlan replaceSources(List<LogicalPlan> sources) {
        var source = Lists.getOnlyElement(sources);
        return new LimitDistinct(source, limit, offset, outputs);
    }

    @Override
    public <C, R> R accept(LogicalPlanVisitor<C, R> visitor, C context) {
        return visitor.visitLimitDistinct(this, context);
    }

    @Override
    public void print(PrintContext printContext) {
        printContext
            .text("LimitDistinct[")
            .text(limit.toString())
            .text(";")
            .text(offset.toString())
            .text(" | [")
            .text(Lists.joinOn(", ", outputs, Symbol::toString))
            .text("]]");
        printStats(printContext);
        printContext.nest(source::print);
    }
}
