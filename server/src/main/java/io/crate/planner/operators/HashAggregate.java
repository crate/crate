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

import static io.crate.execution.engine.pipeline.LimitAndOffset.NO_LIMIT;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.SequencedCollection;
import java.util.Set;

import org.jspecify.annotations.Nullable;

import io.crate.analyze.OrderBy;
import io.crate.common.collections.Lists;
import io.crate.data.Row;
import io.crate.execution.dsl.phases.ExecutionPhases;
import io.crate.execution.dsl.phases.MergePhase;
import io.crate.execution.dsl.projection.AggregationProjection;
import io.crate.execution.dsl.projection.builder.ProjectionBuilder;
import io.crate.expression.symbol.AggregateMode;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.SymbolVisitor;
import io.crate.expression.symbol.Symbols;
import io.crate.metadata.FunctionType;
import io.crate.metadata.IndexType;
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

    /// How the `distinct` flag in `aggregates` is implemented in the **execution** plan.
    /// The `aggregates`/`outputs()` are unaffected by this in every case.
    public enum DistinctMode {
        /// Default: `build()` rewrites `agg(DISTINCT x)` to `collection_agg(collect_set(x))`.
        COLLECT_SET,
        /// `source` is a [GroupHashAggregate] that deduplicates the distinct argument.
        /// There are also non-distinct aggregates, for which we calculate a partial value (per group),
        /// and then merge the partial values into the final value. This is done in `build()`.
        /// See: [io.crate.planner.optimizer.rule.RewriteMixedDistinctAggToGroupBy].
        SPLIT_AND_MERGE,
        /// `source` already deduplicates the distinct argument.
        /// See: [io.crate.planner.optimizer.rule.RewriteDistinctAggToGroupBy].
        NONE
    }

    private final DistinctMode distinctMode;

    public HashAggregate(LogicalPlan source, List<Function> aggregates) {
        this(source, aggregates, DistinctMode.COLLECT_SET);
    }

    public HashAggregate(LogicalPlan source, List<Function> aggregates, DistinctMode distinctMode) {
        super(source);
        this.aggregates = aggregates;
        this.distinctMode = distinctMode;
    }

    public DistinctMode distinctMode() {
        return distinctMode;
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

        AggregationOutputValidator.validateOutputs(aggregates);
        var paramBinder = new SubQueryAndParamBinder(params, subQueryResults);

        DistinctRewriter.Result rewritten = rewriteAggregates(plannerContext, paramBinder);

        var sourceOutputs = source.outputs();
        if (executionPlan.resultDescription().hasRemainingLimitOrOffset()) {
            executionPlan = Merge.ensureOnHandler(executionPlan, plannerContext);
        }
        if (ExecutionPhases.executesOnHandler(plannerContext.handlerNode(), executionPlan.resultDescription().nodeIds())) {
            if (source.preferShardProjections()) {
                executionPlan.addProjection(
                    projectionBuilder.aggregationProjection(
                        sourceOutputs,
                        rewritten.aggregates(),
                        paramBinder,
                        AggregateMode.ITER_PARTIAL,
                        RowGranularity.SHARD
                    )
                );
                executionPlan.addProjection(
                    projectionBuilder.aggregationProjection(
                        rewritten.aggregates(),
                        rewritten.aggregates(),
                        paramBinder,
                        AggregateMode.PARTIAL_FINAL,
                        RowGranularity.CLUSTER
                    )
                );
                if (rewritten.evalProjection() != null) {
                    executionPlan.addProjection(rewritten.evalProjection());
                }
                return executionPlan;
            }
            AggregationProjection fullAggregation = projectionBuilder.aggregationProjection(
                sourceOutputs,
                rewritten.aggregates(),
                paramBinder,
                AggregateMode.ITER_FINAL,
                RowGranularity.CLUSTER
            );
            executionPlan.addProjection(fullAggregation);
            if (rewritten.evalProjection() != null) {
                executionPlan.addProjection(rewritten.evalProjection());
            }
            return executionPlan;
        }
        AggregationProjection toPartial = projectionBuilder.aggregationProjection(
            sourceOutputs,
            rewritten.aggregates(),
            paramBinder,
            AggregateMode.ITER_PARTIAL,
            source.preferShardProjections() ? RowGranularity.SHARD : RowGranularity.NODE
        );
        executionPlan.addProjection(toPartial);

        AggregationProjection toFinal = projectionBuilder.aggregationProjection(
            rewritten.aggregates(),
            rewritten.aggregates(),
            paramBinder,
            AggregateMode.PARTIAL_FINAL,
            RowGranularity.CLUSTER
        );
        ResultDescription resultDescription = executionPlan.resultDescription();
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
                rewritten.evalProjection() == null ? List.of(toFinal) : List.of(toFinal, rewritten.evalProjection()),
                resultDescription.nodeIds(),
                DistributionInfo.DEFAULT_BROADCAST,
                null
            ),
            NO_LIMIT,
            0,
            aggregates.size(),
            1,
            null
        );
    }

    private DistinctRewriter.Result rewriteAggregates(PlannerContext plannerContext, SubQueryAndParamBinder paramBinder) {
        return switch (distinctMode) {
            case COLLECT_SET -> DistinctRewriter.rewrite(
                aggregates,
                aggregates,
                paramBinder,
                plannerContext.transactionContext(),
                plannerContext.nodeContext());
            case SPLIT_AND_MERGE -> SplitDistinctAggregate.rewriteForGroupBy(
                aggregates,
                paramBinder,
                plannerContext.transactionContext(),
                plannerContext.nodeContext());
            case NONE -> DistinctRewriter.noop(aggregates);
        };
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
        return new HashAggregate(Lists.getOnlyElement(sources), aggregates, distinctMode);
    }

    @Override
    public LogicalPlan pruneOutputsExcept(SequencedCollection<Symbol> outputsToKeep) {
        // Collecting pruned outputs, but they can be out of order.
        ArrayList<Function> newAggregates = new ArrayList<>();
        for (Symbol outputToKeep : outputsToKeep) {
            Symbols.intersection(outputToKeep, aggregates, newAggregates::add);
        }
        if (distinctMode == DistinctMode.SPLIT_AND_MERGE) {
            // `source` is a `GroupHashAggregate` producing the partials this operator recombines.
           // `avg(x)`'s argument `x` doesn't appear in `source.outputs()` any more, only
            // the `sum(x)`/`count(x)` partials do.
            // So leave `source` untouched instead of pruning it.
            if (newAggregates.size() == aggregates.size()) {
                return this;
            }
            List<Function> prunedOutputs = Lists.intersection(aggregates, newAggregates);
            HashAggregate newPlan = new HashAggregate(source, prunedOutputs, distinctMode);
            validateOutputsOrder(newPlan.outputs());
            return newPlan;
        }
        // Trying to prune source with a narrower list of outputs:
        // outputsToKeep ∩ aggregates ∩ source.outputs()
        LinkedHashSet<Symbol> toKeep = new LinkedHashSet<>();
        for (Function newAggregate : newAggregates) {
            Symbols.intersection(newAggregate, source.outputs(), toKeep::add);
        }
        LogicalPlan newSource = source.pruneOutputsExcept(toKeep);
        if (source == newSource && newAggregates == aggregates) {
            return this;
        }
        List<Function> prunedOutputs = Lists.intersection(aggregates, newAggregates);
        HashAggregate newPlan = new HashAggregate(newSource, prunedOutputs, distinctMode);
        validateOutputsOrder(newPlan.outputs());
        return newPlan;
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
