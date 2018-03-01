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
import io.crate.data.Row;
import io.crate.execution.dsl.phases.ExecutionPhases;
import io.crate.execution.dsl.phases.MergePhase;
import io.crate.execution.dsl.projection.AggregationProjection;
import io.crate.execution.dsl.projection.builder.ProjectionBuilder;
import io.crate.execution.engine.aggregation.impl.CountAggregation;
import io.crate.expression.symbol.AggregateMode;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.SelectSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.SymbolVisitor;
import io.crate.expression.symbol.format.SymbolFormatter;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Reference;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.planner.ExecutionPlan;
import io.crate.planner.Merge;
import io.crate.planner.PlannerContext;
import io.crate.planner.distribution.DistributionInfo;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class HashAggregate extends OneInputPlan {

    private static final String MERGE_PHASE_NAME = "mergeOnHandler";
    final List<Function> aggregates;

    HashAggregate(LogicalPlan source, List<Function> aggregates) {
        super(source);
        this.aggregates = aggregates;
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
        AggregationOutputValidator.validateOutputs(aggregates);
        ExecutionPlan executionPlan = source.build(
            plannerContext, projectionBuilder, LogicalPlanner.NO_LIMIT, 0, null, null, params, subQueryValues);

        List<Symbol> sourceOutputs = source.outputs();
        if (executionPlan.resultDescription().hasRemainingLimitOrOffset()) {
            executionPlan = Merge.ensureOnHandler(executionPlan, plannerContext);
        }
        if (ExecutionPhases.executesOnHandler(plannerContext.handlerNode(), executionPlan.resultDescription().nodeIds())) {
            if (source.preferShardProjections()) {
                executionPlan.addProjection(projectionBuilder.aggregationProjection(
                    sourceOutputs, aggregates, AggregateMode.ITER_PARTIAL, RowGranularity.SHARD));
                executionPlan.addProjection(projectionBuilder.aggregationProjection(
                    aggregates, aggregates, AggregateMode.PARTIAL_FINAL, RowGranularity.CLUSTER));
                return executionPlan;
            }
            AggregationProjection fullAggregation = projectionBuilder.aggregationProjection(
                sourceOutputs, aggregates, AggregateMode.ITER_FINAL, RowGranularity.CLUSTER);
            executionPlan.addProjection(fullAggregation);
            return executionPlan;
        }
        AggregationProjection toPartial = projectionBuilder.aggregationProjection(
            sourceOutputs,
            aggregates,
            AggregateMode.ITER_PARTIAL,
            source.preferShardProjections() ? RowGranularity.SHARD : RowGranularity.NODE
        );
        executionPlan.addProjection(toPartial);

        AggregationProjection toFinal = projectionBuilder.aggregationProjection(
            aggregates,
            aggregates,
            AggregateMode.PARTIAL_FINAL,
            RowGranularity.CLUSTER
        );
        return new Merge(
            executionPlan,
            new MergePhase(
                plannerContext.jobId(),
                plannerContext.nextExecutionPhaseId(),
                MERGE_PHASE_NAME,
                executionPlan.resultDescription().nodeIds().size(),
                1,
                Collections.singletonList(plannerContext.handlerNode()),
                executionPlan.resultDescription().streamOutputs(),
                Collections.singletonList(toFinal),
                DistributionInfo.DEFAULT_BROADCAST,
                null
            ),
            LogicalPlanner.NO_LIMIT,
            0,
            aggregates.size(),
            1,
            null
        );
    }

    @Override
    public LogicalPlan tryOptimize(@Nullable LogicalPlan pushDown, SymbolMapper mapper) {
        if (pushDown != null) {
            // can't push down anything
            return null;
        }
        LogicalPlan collapsed = source.tryOptimize(null, mapper);
        if (collapsed instanceof Collect &&
            ((Collect) collapsed).tableInfo instanceof DocTableInfo &&
            aggregates.size() == 1 &&
            aggregates.get(0).info().equals(CountAggregation.COUNT_STAR_FUNCTION)) {

            Collect collect = (Collect) collapsed;
            return new Count(aggregates.get(0), collect.relation.tableRelation(), collect.where);
        }
        if (collapsed == source) {
            return this;
        }
        return updateSource(collapsed, mapper);
    }

    @Override
    public List<Symbol> outputs() {
        return new ArrayList<>(aggregates);
    }

    @Override
    protected LogicalPlan updateSource(LogicalPlan newSource, SymbolMapper mapper) {
        return new HashAggregate(newSource, aggregates);
    }

    @Override
    public long numExpectedRows() {
        return 1L;
    }

    @Override
    public <C, R> R accept(LogicalPlanVisitor<C, R> visitor, C context) {
        return visitor.visitHashAggregate(this, context);
    }

    private static class OutputValidatorContext {
        private boolean insideAggregation = false;
    }

    public static class AggregationOutputValidator extends SymbolVisitor<OutputValidatorContext, Void> {

        private static final AggregationOutputValidator INSTANCE = new AggregationOutputValidator();

        public static void validateOutputs(Collection<? extends Symbol> outputs) {
            OutputValidatorContext ctx = new OutputValidatorContext();
            for (Symbol output : outputs) {
                ctx.insideAggregation = false;
                INSTANCE.process(output, ctx);
            }
        }

        @Override
        public Void visitFunction(Function symbol, OutputValidatorContext context) {
            context.insideAggregation =
                context.insideAggregation || symbol.info().type().equals(FunctionInfo.Type.AGGREGATE);
            for (Symbol argument : symbol.arguments()) {
                process(argument, context);
            }
            context.insideAggregation = false;
            return null;
        }

        @Override
        public Void visitReference(Reference symbol, OutputValidatorContext context) {
            if (context.insideAggregation) {
                Reference.IndexType indexType = symbol.indexType();
                if (indexType == Reference.IndexType.ANALYZED) {
                    throw new IllegalArgumentException(SymbolFormatter.format(
                        "Cannot select analyzed column '%s' within grouping or aggregations", symbol));
                }
            }
            return null;
        }

        @Override
        protected Void visitSymbol(Symbol symbol, OutputValidatorContext context) {
            return null;
        }
    }
}
