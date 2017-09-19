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
import io.crate.analyze.symbol.AggregateMode;
import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.symbol.SymbolVisitor;
import io.crate.analyze.symbol.format.SymbolFormatter;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Reference;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.operation.aggregation.impl.CountAggregation;
import io.crate.planner.Merge;
import io.crate.planner.Plan;
import io.crate.planner.Planner;
import io.crate.planner.distribution.DistributionInfo;
import io.crate.planner.node.ExecutionPhases;
import io.crate.planner.node.dql.MergePhase;
import io.crate.planner.projection.AggregationProjection;
import io.crate.planner.projection.builder.ProjectionBuilder;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class HashAggregate implements LogicalPlan {

    private static final String MERGE_PHASE_NAME = "mergeOnHandler";
    final LogicalPlan source;
    final List<Function> aggregates;

    HashAggregate(LogicalPlan source, List<Function> aggregates) {
        this.source = source;
        this.aggregates = aggregates;
    }

    @Override
    public Plan build(Planner.Context plannerContext,
                      ProjectionBuilder projectionBuilder,
                      int limit,
                      int offset,
                      @Nullable OrderBy order,
                      @Nullable Integer pageSizeHint) {
        AggregationOutputValidator.validateOutputs(aggregates);
        Plan plan = source.build(plannerContext, projectionBuilder, LogicalPlanner.NO_LIMIT, 0, null, null);

        if (plan.resultDescription().hasRemainingLimitOrOffset()) {
            plan = Merge.ensureOnHandler(plan, plannerContext);
        }
        if (ExecutionPhases.executesOnHandler(plannerContext.handlerNode(), plan.resultDescription().nodeIds())) {
            AggregationProjection fullAggregation = projectionBuilder.aggregationProjection(
                source.outputs(),
                aggregates,
                AggregateMode.ITER_FINAL,
                RowGranularity.CLUSTER
            );
            plan.addProjection(fullAggregation);
            return plan;
        }
        AggregationProjection toPartial = projectionBuilder.aggregationProjection(
            source.outputs(),
            aggregates,
            AggregateMode.ITER_PARTIAL,
            // dataGranularity == shard -> 1 row per shard
            // it would be unnecessary overhead to run 1 projection per shard if there is only 1 row
            source.dataGranularity() == RowGranularity.SHARD ? RowGranularity.NODE : RowGranularity.SHARD
        );
        plan.addProjection(toPartial);

        AggregationProjection toFinal = projectionBuilder.aggregationProjection(
            aggregates,
            aggregates,
            AggregateMode.PARTIAL_FINAL,
            RowGranularity.CLUSTER
        );
        return new Merge(
            plan,
            new MergePhase(
                plannerContext.jobId(),
                plannerContext.nextExecutionPhaseId(),
                MERGE_PHASE_NAME,
                plan.resultDescription().nodeIds().size(),
                Collections.singletonList(plannerContext.handlerNode()),
                plan.resultDescription().streamOutputs(),
                Collections.singletonList(toFinal),
                DistributionInfo.DEFAULT_SAME_NODE,
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
    public LogicalPlan tryCollapse() {
        LogicalPlan collapsed = source.tryCollapse();
        if (collapsed instanceof Collect &&
            ((Collect) collapsed).tableInfo instanceof DocTableInfo &&
            aggregates.size() == 1 &&
            aggregates.get(0).info().equals(CountAggregation.COUNT_STAR_FUNCTION)) {

            Collect collect = (Collect) collapsed;
            return new Count(aggregates.get(0), collect.relation.tableRelation().tableInfo(), collect.where);
        }
        if (collapsed == source) {
            return this;
        }
        return new HashAggregate(collapsed, aggregates);
    }

    @Override
    public List<Symbol> outputs() {
        return new ArrayList<>(aggregates);
    }

    @Override
    public List<TableInfo> baseTables() {
        return source.baseTables();
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
                } else if (indexType == Reference.IndexType.NO) {
                    throw new IllegalArgumentException(SymbolFormatter.format(
                        "Cannot select non-indexed column '%s' within grouping or aggregations", symbol));
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
