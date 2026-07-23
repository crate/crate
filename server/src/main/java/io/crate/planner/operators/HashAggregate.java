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

import static io.crate.analyze.expressions.ExpressionAnalyzer.allocateBuiltinOrUdfFunction;
import static io.crate.analyze.expressions.ExpressionAnalyzer.allocateFunction;
import static io.crate.execution.engine.pipeline.LimitAndOffset.NO_LIMIT;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.SequencedCollection;
import java.util.Set;

import org.jspecify.annotations.Nullable;

import io.crate.analyze.OrderBy;
import io.crate.analyze.WindowDefinition;
import io.crate.analyze.expressions.ExpressionAnalysisContext;
import io.crate.common.collections.Lists;
import io.crate.data.Row;
import io.crate.execution.dsl.phases.ExecutionPhases;
import io.crate.execution.dsl.phases.MergePhase;
import io.crate.execution.dsl.projection.AggregationProjection;
import io.crate.execution.dsl.projection.builder.ProjectionBuilder;
import io.crate.execution.engine.aggregation.impl.CollectSetAggregation;
import io.crate.expression.symbol.AggregateMode;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.SymbolVisitor;
import io.crate.expression.symbol.Symbols;
import io.crate.expression.symbol.WindowFunction;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.FunctionType;
import io.crate.metadata.IndexType;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Reference;
import io.crate.metadata.RowGranularity;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.ExecutionPlan;
import io.crate.planner.Merge;
import io.crate.planner.PlannerContext;
import io.crate.planner.ResultDescription;
import io.crate.planner.distribution.DistributionInfo;

public class HashAggregate extends ForwardingLogicalPlan {

    private static final String MERGE_PHASE_NAME = "mergeOnHandler";
    final List<Function> aggregates;

    HashAggregate(LogicalPlan source, List<Function> aggregates) {
        super(source);
        this.aggregates = aggregates;
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
        // Avoid source look-ups for performance reasons. Global aggregations are a pipeline breaker using all data.
        // So use column store instead, because it is likely more efficient.
        if (planHints.contains(PlanHint.PREFER_SOURCE_LOOKUP)) {
            planHints = new HashSet<>(planHints);
            planHints.remove(PlanHint.PREFER_SOURCE_LOOKUP);
        }
        ExecutionPlan executionPlan = source.build(
            executor, plannerContext, planHints, projectionBuilder, NO_LIMIT, 0, null, null, params, subQueryResults);

        var aggregatesRewritten = rewriteDistinct(plannerContext, aggregates);
        AggregationOutputValidator.validateOutputs(aggregatesRewritten);
        var paramBinder = new SubQueryAndParamBinder(params, subQueryResults);

        var sourceOutputs = source.outputs();
        if (executionPlan.resultDescription().hasRemainingLimitOrOffset()) {
            executionPlan = Merge.ensureOnHandler(executionPlan, plannerContext);
        }
        if (ExecutionPhases.executesOnHandler(plannerContext.handlerNode(), executionPlan.resultDescription().nodeIds())) {
            if (source.preferShardProjections()) {
                executionPlan.addProjection(
                    projectionBuilder.aggregationProjection(
                        sourceOutputs,
                        aggregatesRewritten,
                        paramBinder,
                        AggregateMode.ITER_PARTIAL,
                        RowGranularity.SHARD
                    )
                );
                executionPlan.addProjection(
                    projectionBuilder.aggregationProjection(
                        aggregatesRewritten,
                        aggregatesRewritten,
                        paramBinder,
                        AggregateMode.PARTIAL_FINAL,
                        RowGranularity.CLUSTER
                    )
                );

                executionPlan = addEvalProj(plannerContext, params, subQueryResults, executionPlan, aggregatesRewritten);
                return executionPlan;
            }

            AggregationProjection fullAggregation = projectionBuilder.aggregationProjection(
                sourceOutputs,
                aggregatesRewritten,
                paramBinder,
                AggregateMode.ITER_FINAL,
                RowGranularity.CLUSTER
            );
            executionPlan.addProjection(fullAggregation);

            executionPlan = addEvalProj(plannerContext, params, subQueryResults, executionPlan, aggregatesRewritten);
            return executionPlan;
        }
        AggregationProjection toPartial = projectionBuilder.aggregationProjection(
            sourceOutputs,
            aggregatesRewritten,
            paramBinder,
            AggregateMode.ITER_PARTIAL,
            source.preferShardProjections() ? RowGranularity.SHARD : RowGranularity.NODE
        );
        executionPlan.addProjection(toPartial);

        AggregationProjection toFinal = projectionBuilder.aggregationProjection(
            aggregatesRewritten,
            aggregatesRewritten,
            paramBinder,
            AggregateMode.PARTIAL_FINAL,
            RowGranularity.CLUSTER
        );
        ResultDescription resultDescription = executionPlan.resultDescription();

        executionPlan = addEvalProj(plannerContext, params, subQueryResults, executionPlan, aggregatesRewritten);

        return new Merge(
            executionPlan,
            new MergePhase(
                plannerContext.jobId(),
                plannerContext.nextExecutionPhaseId(),
                MERGE_PHASE_NAME,
                resultDescription.nodeIds().size(),
                1,
                Collections.singletonList(plannerContext.handlerNode()),
                resultDescription.streamOutputs(),
                Collections.singletonList(toFinal),
                resultDescription.nodeIds(),
                DistributionInfo.DEFAULT_BROADCAST,
                null
            ),
            NO_LIMIT,
            0,
            aggregatesRewritten.size(),
            1,
            null
        );
    }

    private ExecutionPlan addEvalProj(PlannerContext plannerContext,
                                      Row params,
                                      SubQueryResults subQueryResults,
                                      ExecutionPlan executionPlan,
                                      List<Function> aggregatesRewritten) {
        if (Eval.hasDistinctFunctions(this)) {
            executionPlan = Eval.addEvalProjection(
                plannerContext,
                executionPlan,
                params,
                subQueryResults,
                new ArrayList<>(outputs()),
                new ArrayList<>(aggregatesRewritten)
            );
        }
        return executionPlan;
    }

    private List<Function> rewriteDistinct(PlannerContext plannerContext, List<Function> functions) {
        return functions.stream()
            .map((Function fn) -> rewriteDistinctFunction(plannerContext.transactionContext(), plannerContext.nodeContext(), fn))
            .toList();
    }


    private static Function rewriteDistinctFunction(CoordinatorTxnCtx coordinatorTxnCtx, NodeContext nodeCtx, Function fn) {
        if (fn.distinct()) {
            return wrapWithCollectSet(fn, coordinatorTxnCtx, nodeCtx);
        }

        return fn;
    }

    private static Function wrapWithCollectSet(Function original, CoordinatorTxnCtx coordinatorTxnCtx, NodeContext nodeCtx) {
        var arguments = original.arguments();
        var filter = original.filter();
        ExpressionAnalysisContext context = null;
        String name = original.name();
        WindowDefinition windowDefinition = (original instanceof WindowFunction wf) ? wf.windowDefinition() : null;
        Boolean ignoreNulls = (original instanceof WindowFunction wf) ? wf.ignoreNulls() : null;
        String schema = original.signature().getName().schema();

        Function collectSetFunction = allocateFunction(
            CollectSetAggregation.NAME,
            arguments,
            filter,
            context,
            coordinatorTxnCtx,
            nodeCtx
        );

        if (true) {
            return collectSetFunction;
        }

        // define the outer function which contains the inner function as argument.
        String nodeName = "collection_" + name;
        List<Symbol> outerArguments = List.of(collectSetFunction);
        try {
            return allocateBuiltinOrUdfFunction(
                schema, nodeName, outerArguments, null, ignoreNulls, context, true, windowDefinition, coordinatorTxnCtx, nodeCtx);
        } catch (UnsupportedOperationException ex) {
            throw new UnsupportedOperationException(String.format(Locale.ENGLISH,
                "unknown function %s(DISTINCT %s)", name, arguments.get(0).valueType()), ex);
        }

    }

    public List<Function> aggregates() {
        return aggregates;
    }

    @Override
    public List<Symbol> outputs() {
        return new ArrayList<>(aggregates);
    }

    @Override
    public LogicalPlan replaceSources(List<LogicalPlan> sources) {
        return new HashAggregate(Lists.getOnlyElement(sources), aggregates);
    }

    @Override
    public LogicalPlan pruneOutputsExcept(SequencedCollection<Symbol> outputsToKeep) {
        ArrayList<Function> newAggregates = new ArrayList<>();
        for (Symbol outputToKeep : outputsToKeep) {
            Symbols.intersection(outputToKeep, aggregates, newAggregates::add);
        }
        LinkedHashSet<Symbol> toKeep = new LinkedHashSet<>();
        for (Function newAggregate : newAggregates) {
            Symbols.intersection(newAggregate, source.outputs(), toKeep::add);
        }
        LogicalPlan newSource = source.pruneOutputsExcept(toKeep);
        if (source == newSource && newAggregates == aggregates) {
            return this;
        }
        return new HashAggregate(newSource, newAggregates);
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
                output.accept(INSTANCE, ctx);
            }
        }

        @Override
        public Void visitFunction(Function symbol, OutputValidatorContext context) {
            context.insideAggregation =
                context.insideAggregation || symbol.signature().getType().equals(FunctionType.AGGREGATE);
            for (Symbol argument : symbol.arguments()) {
                argument.accept(this, context);
            }
            context.insideAggregation = false;
            return null;
        }

        @Override
        public Void visitReference(Reference symbol, OutputValidatorContext context) {
            if (context.insideAggregation) {
                IndexType indexType = symbol.indexType();
                if (indexType == IndexType.FULLTEXT) {
                    throw new IllegalArgumentException(Symbols.format(
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
