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

package io.crate.planner.consumer;

import com.google.common.base.Optional;
import io.crate.analyze.MultiSourceSelect;
import io.crate.analyze.QuerySpec;
import io.crate.analyze.RelationSource;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.symbol.*;
import io.crate.metadata.Functions;
import io.crate.metadata.RowGranularity;
import io.crate.operation.projectors.TopN;
import io.crate.planner.*;
import io.crate.planner.distribution.DistributionInfo;
import io.crate.planner.node.dql.MergePhase;
import io.crate.planner.projection.AggregationProjection;
import io.crate.planner.projection.Projection;
import io.crate.planner.projection.builder.ProjectionBuilder;
import io.crate.planner.projection.builder.SplitPoints;

import java.util.*;

class MultiSourceAggregationConsumer implements Consumer {

    private final Visitor visitor;

    MultiSourceAggregationConsumer(Functions functions) {
        visitor = new Visitor(functions);
    }

    @Override
    public Plan consume(AnalyzedRelation relation, ConsumerContext context) {
        return visitor.process(relation, context);
    }

    private static class Visitor extends RelationPlanningVisitor {

        private final Functions functions;

        public Visitor(Functions functions) {
            this.functions = functions;
        }

        @Override
        public Plan visitMultiSourceSelect(MultiSourceSelect multiSourceSelect, ConsumerContext context) {
            QuerySpec qs = multiSourceSelect.querySpec();
            if (!qs.hasAggregates() || qs.groupBy().isPresent()) {
                return null;
            }
            qs = qs.copyAndReplace(i -> i); // copy because MSS planning mutates symbols
            ProjectionBuilder projectionBuilder = new ProjectionBuilder(functions, qs);
            SplitPoints splitPoints = projectionBuilder.getSplitPoints();
            removeAggregationsAndLimitsFromMSS(multiSourceSelect, splitPoints);
            Planner.Context plannerContext = context.plannerContext();
            Plan plan = plannerContext.planSubRelation(multiSourceSelect, context);

            return addAggregations(qs, projectionBuilder, splitPoints, plannerContext, plan);
        }
    }

    private static Merge addAggregations(QuerySpec qs,
                                         ProjectionBuilder projectionBuilder,
                                         SplitPoints splitPoints,
                                         Planner.Context plannerContext,
                                         Plan plan) {
        ResultDescription resultDescription = plan.resultDescription();
        AggregationProjection partialAggregation = projectionBuilder.aggregationProjection(
            splitPoints.toCollect(),
            splitPoints.aggregates(),
            Aggregation.Step.ITER,
            Aggregation.Step.PARTIAL,
            RowGranularity.CLUSTER
        );
        plan.addProjection(partialAggregation, null, null, splitPoints.aggregates().size(), null);

        List<Projection> handlerProjections = new ArrayList<>();
        AggregationProjection finalAggregation = projectionBuilder.aggregationProjection(
            splitPoints.aggregates(),
            splitPoints.aggregates(),
            Aggregation.Step.PARTIAL,
            Aggregation.Step.FINAL,
            RowGranularity.CLUSTER
        );
        handlerProjections.add(finalAggregation);
        if (qs.having().isPresent()) {
            handlerProjections.add(ProjectionBuilder.filterProjection(splitPoints.aggregates(), qs.having().get()));
        }
        Limits limits = plannerContext.getLimits(qs);
        handlerProjections.add(ProjectionBuilder.topNProjection(
            splitPoints.aggregates(), null, limits.offset(), limits.finalLimit(), qs.outputs()));
        MergePhase mergePhase = new MergePhase(
            plannerContext.jobId(),
            plannerContext.nextExecutionPhaseId(),
            "mergeOnHandler",
            resultDescription.nodeIds().size(),
            Collections.singletonList(plannerContext.handlerNode()),
            resultDescription.streamOutputs(),
            handlerProjections,
            DistributionInfo.DEFAULT_SAME_NODE,
            null
        );
        return new Merge(
            plan, mergePhase, TopN.NO_LIMIT, 0, splitPoints.aggregates().size(), 1, null);
    }

    private static void removeAggregationsAndLimitsFromMSS(MultiSourceSelect mss, SplitPoints splitPoints) {
        QuerySpec querySpec = mss.querySpec();
        querySpec.outputs(new ArrayList<>(splitPoints.toCollect()));
        querySpec.hasAggregates(false);
        // Limit & offset must be applied after the aggregation, so remove it from mss and sources.
        // OrderBy can be ignored because it's also applied after aggregation but there is always only 1 row so it
        // wouldn't have any effect.
        removeLimitOffsetAndOrder(querySpec);
        for (RelationSource relationSource : mss.sources().values()) {
            removeLimitOffsetAndOrder(relationSource.querySpec());
        }

        // need to change the types on the fields of the MSS to match the new outputs
        ListIterator<Field> fieldsIt = mss.fields().listIterator();
        Iterator<Function> outputsIt = splitPoints.aggregates().iterator();
        while (fieldsIt.hasNext()) {
            Field field = fieldsIt.next();
            Symbol output = outputsIt.next();
            fieldsIt.set(new Field(field.relation(), field.path(), output.valueType()));
        }
    }

    private static void removeLimitOffsetAndOrder(QuerySpec querySpec) {
        if (querySpec.limit().isPresent()) {
            querySpec.limit(Optional.absent());
        }
        if (querySpec.offset().isPresent()) {
            querySpec.offset(Optional.absent());
        }
        if (querySpec.orderBy().isPresent()) {
            querySpec.orderBy(null);
        }
    }
}
