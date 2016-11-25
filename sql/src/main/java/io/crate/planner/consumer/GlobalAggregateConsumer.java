/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.planner.consumer;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import io.crate.analyze.*;
import io.crate.analyze.relations.AbstractTableRelation;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.QueriedDocTable;
import io.crate.analyze.symbol.*;
import io.crate.analyze.symbol.format.SymbolFormatter;
import io.crate.exceptions.VersionInvalidException;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Functions;
import io.crate.metadata.Reference;
import io.crate.metadata.RowGranularity;
import io.crate.operation.projectors.TopN;
import io.crate.planner.*;
import io.crate.planner.distribution.DistributionInfo;
import io.crate.planner.node.ExecutionPhases;
import io.crate.planner.node.dql.Collect;
import io.crate.planner.node.dql.MergePhase;
import io.crate.planner.node.dql.RoutedCollectPhase;
import io.crate.planner.projection.AggregationProjection;
import io.crate.planner.projection.FilterProjection;
import io.crate.planner.projection.Projection;
import io.crate.planner.projection.builder.ProjectionBuilder;
import io.crate.planner.projection.builder.SplitPoints;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;


class GlobalAggregateConsumer implements Consumer {

    private static final AggregationOutputValidator AGGREGATION_OUTPUT_VALIDATOR = new AggregationOutputValidator();
    private final RelationPlanningVisitor visitor;

    GlobalAggregateConsumer(Functions functions) {
        visitor = new Visitor(functions);
    }

    @Override
    public Plan consume(AnalyzedRelation rootRelation, ConsumerContext context) {
        return visitor.process(rootRelation, context);
    }

    /**
     * Add aggregation/having/topN projections to the plan.
     * Either directly or by wrapping it into a Merge plan.
     */
    static Plan addAggregations(QuerySpec qs,
                                ProjectionBuilder projectionBuilder,
                                SplitPoints splitPoints,
                                Planner.Context plannerContext,
                                Plan plan) {
        ResultDescription resultDescription = plan.resultDescription();
        if (resultDescription.limit() > TopN.NO_LIMIT || resultDescription.orderBy() != null) {
            plan = Merge.ensureOnHandler(plan, plannerContext);
            resultDescription = plan.resultDescription();
        }
        WhereClause where = qs.where();
        if (where != WhereClause.MATCH_ALL) {
            FilterProjection whereFilter = ProjectionBuilder.filterProjection(splitPoints.toCollect(), where);
            plan.addProjection(whereFilter, null, null, null, null);
        }
        List<Projection> postAggregationProjections = createPostAggregationProjections(qs, splitPoints, plannerContext);
        if (ExecutionPhases.executesOnHandler(plannerContext.handlerNode(), resultDescription.nodeIds())) {
            AggregationProjection finalAggregation = projectionBuilder.aggregationProjection(
                splitPoints.toCollect(),
                splitPoints.aggregates(),
                Aggregation.Step.ITER,
                Aggregation.Step.FINAL,
                RowGranularity.CLUSTER
            );
            plan.addProjection(finalAggregation, null, null, splitPoints.aggregates().size(), null);
            for (Projection postAggregationProjection : postAggregationProjections) {
                plan.addProjection(postAggregationProjection, null, null, null, null);
            }
            return plan;
        } else {
            AggregationProjection partialAggregation = projectionBuilder.aggregationProjection(
                splitPoints.toCollect(),
                splitPoints.aggregates(),
                Aggregation.Step.ITER,
                Aggregation.Step.PARTIAL,
                RowGranularity.CLUSTER
            );
            plan.addProjection(partialAggregation, null, null, splitPoints.aggregates().size(), null);

            AggregationProjection finalAggregation = projectionBuilder.aggregationProjection(
                splitPoints.aggregates(),
                splitPoints.aggregates(),
                Aggregation.Step.PARTIAL,
                Aggregation.Step.FINAL,
                RowGranularity.CLUSTER
            );
            postAggregationProjections.add(0, finalAggregation);
        }
        return createMerge(plan, plannerContext, postAggregationProjections);
    }

    private static List<Projection> createPostAggregationProjections(QuerySpec qs,
                                                                     SplitPoints splitPoints,
                                                                     Planner.Context plannerContext) {
        List<Projection> postAggregationProjections = new ArrayList<>();
        Optional<HavingClause> having = qs.having();
        if (having.isPresent()) {
            postAggregationProjections.add(ProjectionBuilder.filterProjection(splitPoints.aggregates(), having.get()));
        }
        Limits limits = plannerContext.getLimits(qs);
        // topN is used even if there is no limit because outptus might contain scalars which need to be executed
        postAggregationProjections.add(ProjectionBuilder.topNProjection(
            splitPoints.aggregates(), null, limits.offset(), limits.finalLimit(), qs.outputs()));
        return postAggregationProjections;
    }

    private static Plan createMerge(Plan plan, Planner.Context plannerContext, List<Projection> mergeProjections) {
        ResultDescription resultDescription = plan.resultDescription();
        MergePhase mergePhase = new MergePhase(
            plannerContext.jobId(),
            plannerContext.nextExecutionPhaseId(),
            "mergeOnHandler",
            resultDescription.nodeIds().size(),
            Collections.singletonList(plannerContext.handlerNode()),
            resultDescription.streamOutputs(),
            mergeProjections,
            DistributionInfo.DEFAULT_SAME_NODE,
            null
        );
        Projection lastProjection = mergeProjections.get(mergeProjections.size() - 1);
        return new Merge(
            plan, mergePhase, TopN.NO_LIMIT, 0, lastProjection.outputs().size(), 1, null);
    }

    /**
     * Create a Merge(Collect) plan.
     *
     * iter->partial aggregations on use {@code projectionGranularity} granularity
     */
    private static Plan globalAggregates(Functions functions,
                                         QueriedTableRelation table,
                                         ConsumerContext context,
                                         RowGranularity projectionGranularity) {
        QuerySpec querySpec = table.querySpec();
        if (querySpec.groupBy().isPresent() || !querySpec.hasAggregates()) {
            return null;
        }
        // global aggregate: collect and partial aggregate on C and final agg on H
        Planner.Context plannerContext = context.plannerContext();
        validateAggregationOutputs(table.tableRelation(), querySpec.outputs());
        ProjectionBuilder projectionBuilder = new ProjectionBuilder(functions, querySpec);
        SplitPoints splitPoints = projectionBuilder.getSplitPoints();

        AggregationProjection ap = projectionBuilder.aggregationProjection(
            splitPoints.leaves(),
            splitPoints.aggregates(),
            Aggregation.Step.ITER,
            Aggregation.Step.PARTIAL,
            projectionGranularity
        );
        RoutedCollectPhase collectPhase = RoutedCollectPhase.forQueriedTable(
            plannerContext,
            table,
            splitPoints.leaves(),
            ImmutableList.of(ap),
            table.relationId()
        );
        Collect collect = new Collect(collectPhase, TopN.NO_LIMIT, 0, ap.outputs().size(), 1, null);

        AggregationProjection aggregationProjection = projectionBuilder.aggregationProjection(
            splitPoints.aggregates(),
            splitPoints.aggregates(),
            Aggregation.Step.PARTIAL,
            Aggregation.Step.FINAL,
            RowGranularity.CLUSTER);
        List<Projection> postAggregationProjections = createPostAggregationProjections(querySpec, splitPoints, plannerContext);
        postAggregationProjections.add(0, aggregationProjection);

        return createMerge(collect, plannerContext, postAggregationProjections);
    }

    private static void validateAggregationOutputs(AbstractTableRelation tableRelation, Collection<? extends Symbol> outputSymbols) {
        OutputValidatorContext context = new OutputValidatorContext(tableRelation);
        for (Symbol outputSymbol : outputSymbols) {
            context.insideAggregation = false;
            AGGREGATION_OUTPUT_VALIDATOR.process(outputSymbol, context);
        }
    }

    private static class OutputValidatorContext {
        private final AbstractTableRelation tableRelation;
        private boolean insideAggregation = false;

        OutputValidatorContext(AbstractTableRelation tableRelation) {
            this.tableRelation = tableRelation;
        }
    }

    private static class Visitor extends RelationPlanningVisitor {

        private final Functions functions;

        public Visitor(Functions functions) {
            this.functions = functions;
        }

        @Override
        public Plan visitQueriedDocTable(QueriedDocTable table, ConsumerContext context) {
            if (table.querySpec().where().hasVersions()) {
                context.validationException(new VersionInvalidException());
                return null;
            }
            return globalAggregates(functions, table, context, RowGranularity.SHARD);
        }

        @Override
        public Plan visitQueriedTable(QueriedTable table, ConsumerContext context) {
            return globalAggregates(functions, table, context, RowGranularity.CLUSTER);
        }

        @Override
        public Plan visitQueriedSelectRelation(QueriedSelectRelation relation, ConsumerContext context) {
            QuerySpec qs = relation.querySpec();
            if (qs.groupBy().isPresent() || !qs.hasAggregates()) {
                return null;
            }
            Planner.Context plannerContext = context.plannerContext();
            Plan subPlan = plannerContext.planSubRelation(relation.subRelation(), context);
            if (subPlan == null) {
                return null;
            }
            ProjectionBuilder projectionBuilder = new ProjectionBuilder(functions, qs);
            return addAggregations(qs, projectionBuilder, projectionBuilder.getSplitPoints(), plannerContext, subPlan);
        }
    }

    private static class AggregationOutputValidator extends SymbolVisitor<OutputValidatorContext, Void> {

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
        public Void visitField(Field field, OutputValidatorContext context) {
            return process(context.tableRelation.resolveField(field), context);
        }

        @Override
        protected Void visitSymbol(Symbol symbol, OutputValidatorContext context) {
            return null;
        }
    }
}
