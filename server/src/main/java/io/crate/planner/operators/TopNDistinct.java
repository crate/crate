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
import io.crate.execution.dsl.projection.EvalProjection;
import io.crate.execution.dsl.projection.TopNDistinctProjection;
import io.crate.execution.dsl.projection.TopNProjection;
import io.crate.execution.dsl.projection.builder.InputColumns;
import io.crate.execution.dsl.projection.builder.ProjectionBuilder;
import io.crate.execution.engine.pipeline.TopN;
import io.crate.expression.symbol.InputColumn;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.Symbols;
import io.crate.metadata.RowGranularity;
import io.crate.planner.ExecutionPlan;
import io.crate.planner.Merge;
import io.crate.planner.PlannerContext;
import io.crate.types.DataTypes;

import javax.annotation.Nullable;
import java.util.List;

import static io.crate.analyze.SymbolEvaluator.evaluate;

public final class TopNDistinct extends ForwardingLogicalPlan {

    private final Symbol limit;
    private final List<Symbol> outputs;
    private final Symbol offset;

    public TopNDistinct(LogicalPlan source, Symbol limit, Symbol offset, List<Symbol> outputs) {
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
    public ExecutionPlan build(PlannerContext plannerContext,
                               ProjectionBuilder projectionBuilder,
                               int limitHint,
                               int offsetHint,
                               @Nullable OrderBy order,
                               @Nullable Integer pageSizeHint,
                               Row params,
                               SubQueryResults subQueryResults) {
        var executionPlan = source.build(
            plannerContext,
            projectionBuilder,
            TopN.NO_LIMIT,
            TopN.NO_OFFSET,
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
        int limit = DataTypes.INTEGER.value(
            evaluate(
                plannerContext.transactionContext(),
                plannerContext.functions(),
                this.limit,
                params,
                subQueryResults
            )
        );
        int offset = DataTypes.INTEGER.value(
            evaluate(
                plannerContext.transactionContext(),
                plannerContext.functions(),
                this.offset,
                params,
                subQueryResults
            )
        );
        var inputColOutputs = InputColumn.mapToInputColumns(outputs);
        executionPlan.addProjection(
            new TopNDistinctProjection(
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
            TopNDistinctProjection topNDistinct = new TopNDistinctProjection(
                limit + offset,
                inputColOutputs,
                RowGranularity.CLUSTER
            );
            executionPlan.addProjection(topNDistinct);
        }
        if (offset > 0) {
            // TopNDistinctProjection outputs a distinct result-set,
            // That allows us to use the TopNProjection to apply the offset
            executionPlan.addProjection(
                new TopNProjection(limit, offset, Symbols.typeView(inputColOutputs))
            );
        }
        return executionPlan;
    }

    @Override
    public LogicalPlan replaceSources(List<LogicalPlan> sources) {
        var source = Lists2.getOnlyElement(sources);
        return new TopNDistinct(source, limit, offset, outputs);
    }

    @Override
    public <C, R> R accept(LogicalPlanVisitor<C, R> visitor, C context) {
        return visitor.visitTopNDistinct(this, context);
    }

    @Override
    public void print(PrintContext printContext) {
        printContext
            .text("TopNDistinct[")
            .text(limit.toString())
            .text(";")
            .text(offset.toString())
            .text(" | [")
            .text(Lists2.joinOn(", ", outputs, Symbol::toString))
            .text("]]")
            .nest(source::print);
    }
}
