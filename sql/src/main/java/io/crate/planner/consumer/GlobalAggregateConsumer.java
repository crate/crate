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

import com.google.common.collect.ImmutableList;
import io.crate.analyze.HavingClause;
import io.crate.analyze.QueriedSelectRelation;
import io.crate.analyze.QueriedTable;
import io.crate.analyze.QueriedTableRelation;
import io.crate.analyze.QuerySpec;
import io.crate.analyze.WhereClause;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.QueriedDocTable;
import io.crate.analyze.symbol.AggregateMode;
import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.Symbol;
import io.crate.exceptions.VersionInvalidException;
import io.crate.metadata.RowGranularity;
import io.crate.operation.projectors.TopN;
import io.crate.planner.Limits;
import io.crate.planner.Merge;
import io.crate.planner.Plan;
import io.crate.planner.Planner;
import io.crate.planner.ResultDescription;
import io.crate.planner.distribution.DistributionInfo;
import io.crate.planner.node.ExecutionPhases;
import io.crate.planner.node.dql.Collect;
import io.crate.planner.node.dql.MergePhase;
import io.crate.planner.node.dql.RoutedCollectPhase;
import io.crate.planner.operators.HashAggregate;
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

    private final RelationPlanningVisitor visitor;

    GlobalAggregateConsumer(ProjectionBuilder projectionBuilder) {
        visitor = new Visitor(projectionBuilder);
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
                                Collection<? extends Symbol> subRelationOutputs,
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
        if (where.hasQuery() || where.noMatch()) {
            FilterProjection whereFilter = ProjectionBuilder.filterProjection(subRelationOutputs, where);
            plan.addProjection(whereFilter);
        }
        List<Projection> postAggregationProjections =
            createPostAggregationProjections(qs, splitPoints.aggregates(), plannerContext);
        if (ExecutionPhases.executesOnHandler(plannerContext.handlerNode(), resultDescription.nodeIds())) {
            AggregationProjection finalAggregation = projectionBuilder.aggregationProjection(
                subRelationOutputs,
                splitPoints.aggregates(),
                AggregateMode.ITER_FINAL,
                RowGranularity.CLUSTER
            );
            plan.addProjection(finalAggregation);
            for (Projection postAggregationProjection : postAggregationProjections) {
                plan.addProjection(postAggregationProjection);
            }
            return plan;
        } else {
            AggregationProjection partialAggregation = projectionBuilder.aggregationProjection(
                subRelationOutputs,
                splitPoints.aggregates(),
                AggregateMode.ITER_PARTIAL,
                RowGranularity.CLUSTER
            );
            plan.addProjection(partialAggregation);

            AggregationProjection finalAggregation = projectionBuilder.aggregationProjection(
                splitPoints.aggregates(),
                splitPoints.aggregates(),
                AggregateMode.PARTIAL_FINAL,
                RowGranularity.CLUSTER
            );
            postAggregationProjections.add(0, finalAggregation);
        }
        return createMerge(plan, plannerContext, postAggregationProjections);
    }

    private static List<Projection> createPostAggregationProjections(QuerySpec qs,
                                                                     List<Function> aggregates,
                                                                     Planner.Context plannerContext) {
        List<Projection> postAggregationProjections = new ArrayList<>();
        HavingClause having = qs.having();
        if (having != null) {
            postAggregationProjections.add(ProjectionBuilder.filterProjection(aggregates, having));
        }
        Limits limits = plannerContext.getLimits(qs);
        // topN is used even if there is no limit because outputs might contain scalars which need to be executed
        postAggregationProjections.add(ProjectionBuilder.topNOrEval(
            aggregates, null, limits.offset(), limits.finalLimit(), qs.outputs()));
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
    private static Plan globalAggregates(ProjectionBuilder projectionBuilder,
                                         QueriedTableRelation table,
                                         ConsumerContext context,
                                         RowGranularity projectionGranularity) {
        QuerySpec querySpec = table.querySpec();
        if (!querySpec.groupBy().isEmpty() || !querySpec.hasAggregates()) {
            return null;
        }
        // global aggregate: collect and partial aggregate on C and final agg on H
        Planner.Context plannerContext = context.plannerContext();
        validateAggregationOutputs(querySpec.outputs());
        SplitPoints splitPoints = SplitPoints.create(querySpec);

        AggregationProjection ap = projectionBuilder.aggregationProjection(
            splitPoints.toCollect(),
            splitPoints.aggregates(),
            AggregateMode.ITER_PARTIAL,
            projectionGranularity
        );
        RoutedCollectPhase collectPhase = RoutedCollectPhase.forQueriedTable(
            plannerContext,
            table,
            splitPoints.toCollect(),
            ImmutableList.of(ap)
        );
        Collect collect = new Collect(collectPhase, TopN.NO_LIMIT, 0, ap.outputs().size(), 1, null);

        AggregationProjection aggregationProjection = projectionBuilder.aggregationProjection(
            splitPoints.aggregates(),
            splitPoints.aggregates(),
            AggregateMode.PARTIAL_FINAL,
            RowGranularity.CLUSTER);
        List<Projection> postAggregationProjections =
            createPostAggregationProjections(querySpec, splitPoints.aggregates(), plannerContext);
        postAggregationProjections.add(0, aggregationProjection);

        return createMerge(collect, plannerContext, postAggregationProjections);
    }

    private static void validateAggregationOutputs(Collection<? extends Symbol> outputSymbols) {
        HashAggregate.AggregationOutputValidator.validateOutputs(outputSymbols);
    }

    private static class Visitor extends RelationPlanningVisitor {

        private final ProjectionBuilder projectionBuilder;

        public Visitor(ProjectionBuilder projectionBuilder) {
            this.projectionBuilder = projectionBuilder;
        }

        @Override
        public Plan visitQueriedDocTable(QueriedDocTable table, ConsumerContext context) {
            if (table.querySpec().where().hasVersions()) {
                context.validationException(new VersionInvalidException());
                return null;
            }
            return globalAggregates(projectionBuilder, table, context, RowGranularity.SHARD);
        }

        @Override
        public Plan visitQueriedTable(QueriedTable table, ConsumerContext context) {
            return globalAggregates(projectionBuilder, table, context, RowGranularity.CLUSTER);
        }

        @Override
        public Plan visitQueriedSelectRelation(QueriedSelectRelation relation, ConsumerContext context) {
            QuerySpec qs = relation.querySpec();
            if (!qs.groupBy().isEmpty() || !qs.hasAggregates()) {
                return null;
            }
            Planner.Context plannerContext = context.plannerContext();
            Plan subPlan = plannerContext.planSubRelation(relation.subRelation(), context);
            if (subPlan == null) {
                return null;
            }
            return addAggregations(
                qs,
                relation.subRelation().fields(),
                projectionBuilder,
                SplitPoints.create(qs),
                plannerContext,
                subPlan
            );
        }
    }
}
