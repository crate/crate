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

package io.crate.planner;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import io.crate.analyze.*;
import io.crate.analyze.relations.*;
import io.crate.exceptions.ValidationException;
import io.crate.exceptions.VersionInvalidException;
import io.crate.operation.projectors.TopN;
import io.crate.planner.consumer.ConsumerContext;
import io.crate.planner.consumer.ConsumingPlanner;
import io.crate.planner.consumer.ESGetStatementPlanner;
import io.crate.planner.consumer.SimpleSelect;
import io.crate.planner.fetch.FetchPushDown;
import io.crate.planner.node.NoopPlannedAnalyzedRelation;
import io.crate.planner.node.dql.CollectAndMerge;
import io.crate.planner.node.dql.MergePhase;
import io.crate.planner.node.dql.QueryThenFetch;
import io.crate.planner.node.dql.RoutedCollectPhase;
import io.crate.planner.projection.TopNProjection;
import io.crate.planner.projection.builder.ProjectionBuilder;

import java.util.ArrayList;
import java.util.List;

class SelectStatementPlanner {

    private final Visitor visitor;

    SelectStatementPlanner(ConsumingPlanner consumingPlanner) {
        visitor = new Visitor(consumingPlanner);
    }

    public Plan plan(SelectAnalyzedStatement statement, Planner.Context context) {
        return visitor.process(statement.relation(), context);
    }

    private static PlannedAnalyzedRelation subPlan(AnalyzedRelation rel, Planner.Context context) {
        ConsumerContext consumerContext = new ConsumerContext(rel, context);
        PlannedAnalyzedRelation subPlan = context.planSubRelation(rel, consumerContext);
        assert subPlan != null;
        ValidationException validationException = consumerContext.validationException();
        if (validationException != null) {
            throw validationException;
        }
        return subPlan;
    }

    private static class Visitor extends AnalyzedRelationVisitor<Planner.Context, Plan> {

        private final ConsumingPlanner consumingPlanner;

        public Visitor(ConsumingPlanner consumingPlanner) {
            this.consumingPlanner = consumingPlanner;
        }

        @Override
        protected Plan visitAnalyzedRelation(AnalyzedRelation relation, Planner.Context context) {
            return consumingPlanner.plan(relation, context);
        }

        @Override
        public Plan visitQueriedDocTable(QueriedDocTable table, Planner.Context context) {
            QuerySpec querySpec = table.querySpec();
            if (querySpec.hasAggregates() || querySpec.groupBy().isPresent()) {
                return consumingPlanner.plan(table, context);
            }
            if (querySpec.where().docKeys().isPresent() && !table.tableRelation().tableInfo().isAlias()) {
                return ESGetStatementPlanner.convert(table, context);
            }
            if (querySpec.where().hasVersions()) {
                throw new VersionInvalidException();
            }
            Limits limits = context.getLimits(true, querySpec);
            if (querySpec.where().noMatch() || (querySpec.limit().isPresent() && limits.finalLimit() == 0)) {
                return new NoopPlan(context.jobId());
            }
            table.tableRelation().validateOrderBy(querySpec.orderBy());

            FetchPushDown.Builder fetchPhaseBuilder = FetchPushDown.pushDown(table);
            if (fetchPhaseBuilder == null) {
                // no fetch required
                return consumingPlanner.plan(table, context);
            }
            AnalyzedRelation subRelation = fetchPhaseBuilder.replacedRelation();
            PlannedAnalyzedRelation plannedSubQuery = subPlan(subRelation, context);
            assert plannedSubQuery != null : "consumingPlanner should have created a subPlan";

            // fetch phase and projection can only be build after the sub-plan was processed (shards/readers allocated)
            FetchPushDown.PhaseAndProjection fetchPhaseAndProjection = fetchPhaseBuilder.build(context);

            CollectAndMerge qaf = (CollectAndMerge) plannedSubQuery;
            RoutedCollectPhase collectPhase = ((RoutedCollectPhase) qaf.collectPhase());

            if (collectPhase.nodePageSizeHint() == null && limits.limitAndOffset > TopN.NO_LIMIT) {
                collectPhase.nodePageSizeHint(limits.limitAndOffset);
            }

            MergePhase localMergePhase;
            assert qaf.localMerge() == null : "subRelation shouldn't plan localMerge";

            TopNProjection topN = ProjectionBuilder.topNProjection(
                collectPhase.toCollect(),
                null, // orderBy = null because stuff is pre-sorted in collectPhase and sortedLocalMerge is used
                limits.offset(),
                limits.finalLimit,
                null
            );
            Optional<OrderBy> orderBy = querySpec.orderBy();
            if (!orderBy.isPresent()) {
                localMergePhase = MergePhase.localMerge(
                    context.jobId(),
                    context.nextExecutionPhaseId(),
                    ImmutableList.of(topN, fetchPhaseAndProjection.projection),
                    collectPhase.executionNodes().size(),
                    collectPhase.outputTypes()
                );
            } else {
                localMergePhase = MergePhase.sortedMerge(
                    context.jobId(),
                    context.nextExecutionPhaseId(),
                    orderBy.get(),
                    collectPhase.toCollect(),
                    null,
                    ImmutableList.of(topN, fetchPhaseAndProjection.projection),
                    collectPhase.executionNodes().size(),
                    collectPhase.outputTypes()
                );
            }
            SimpleSelect.enablePagingIfApplicable(
                collectPhase, localMergePhase, limits.finalLimit(), limits.offset(),
                context.clusterService().localNode().id());
            return new QueryThenFetch(plannedSubQuery.plan(), fetchPhaseAndProjection.phase, localMergePhase, context.jobId());
        }

        @Override
        public Plan visitMultiSourceSelect(MultiSourceSelect mss, Planner.Context context) {
            if (mss.querySpec().where().noMatch() && !mss.querySpec().hasAggregates()) {
                return new NoopPlan(context.jobId());
            }
            if (mss.canBeFetched().isEmpty()) {
                return consumingPlanner.plan(mss, context);
            }
            FetchPushDown.Builder fetchPhaseBuilder = FetchPushDown.pushDown(mss);
            ConsumerContext consumerContext = new ConsumerContext(mss, context);
            // plan sub relation as if root so that it adds a mergePhase
            PlannedAnalyzedRelation plannedSubQuery = consumingPlanner.plan(mss, consumerContext);
            // NestedLoopConsumer can return NoopPlannedAnalyzedRelation if its left or right plan
            // is noop. E.g. it is the case with creating NestedLoopConsumer for empty partitioned tables.
            if (plannedSubQuery instanceof NoopPlannedAnalyzedRelation) {
                return new NoopPlan(context.jobId());
            }
            assert plannedSubQuery != null : "consumingPlanner should have created a subPlan";
            assert !plannedSubQuery.resultIsDistributed() : "subQuery must not have a distributed result";

            assert fetchPhaseBuilder != null : "expecting fetchPhaseBuilder not to be null";
            FetchPushDown.PhaseAndProjection fetchPhaseAndProjection = fetchPhaseBuilder.build(context);

            plannedSubQuery.addProjection(fetchPhaseAndProjection.projection);
            return new QueryThenFetch(plannedSubQuery.plan(), fetchPhaseAndProjection.phase, null, context.jobId());
        }

        @Override
        public Plan visitTwoRelationsUnion(TwoRelationsUnion twoRelationsUnion, Planner.Context context) {
            // Currently we only support UNION ALL so it's ok to flatten the union pairs
            UnionFlatteningVisitorContext visitorContext = new UnionFlatteningVisitorContext();
            UnionFlatteningVisitor unionFlatteningVisitor = new UnionFlatteningVisitor();
            unionFlatteningVisitor.process(twoRelationsUnion, visitorContext);
            UnionSelect unionSelect = new UnionSelect(visitorContext.relations, twoRelationsUnion.querySpec());

            FetchPushDown.Builder fetchPhaseBuilder = FetchPushDown.pushDown(unionSelect);
            ConsumerContext consumerContext = new ConsumerContext(unionSelect, context);
            // plan sub relation as if root so that it adds a mergePhase
            PlannedAnalyzedRelation plannedSubQuery = consumingPlanner.plan(unionSelect, consumerContext);
            if (plannedSubQuery instanceof NoopPlannedAnalyzedRelation) {
                return new NoopPlan(context.jobId());
            }
            assert plannedSubQuery != null : "consumingPlanner should have created a subPlan";
            assert !plannedSubQuery.resultIsDistributed() : "subQuery must not have a distributed result";

            if (fetchPhaseBuilder == null) {
                // no fetch required
                return plannedSubQuery.plan();
            }

            FetchPushDown.PhaseAndProjection fetchPhaseAndProjection = fetchPhaseBuilder.build(context);

            plannedSubQuery.addProjection(fetchPhaseAndProjection.projection);
            return new QueryThenFetch(plannedSubQuery.plan(), fetchPhaseAndProjection.phase, null, context.jobId());
        }

        @Override
        public Plan visitQueriedSelectRelation(QueriedSelectRelation relation, Planner.Context context) {
            throw new UnsupportedOperationException("complex sub selects are not supported");
        }
    }

    private static class UnionFlatteningVisitor extends AnalyzedRelationVisitor<UnionFlatteningVisitorContext, Void> {

        @Override
        public Void visitQueriedTable(QueriedTable relation, UnionFlatteningVisitorContext context) {
            context.relations.add(0, relation);
            return null;
        }

        @Override
        public Void visitQueriedDocTable(QueriedDocTable relation, UnionFlatteningVisitorContext context) {
            context.relations.add(0, relation);
            return null;
        }

        @Override
        public Void visitMultiSourceSelect(MultiSourceSelect relation, UnionFlatteningVisitorContext context) {
            context.relations.add(0, relation);
            return null;
        }

        @Override
        public Void visitQueriedSelectRelation(QueriedSelectRelation relation, UnionFlatteningVisitorContext context) {
            context.relations.add(0, relation);
            return null;
        }

        @Override
        public Void visitTwoRelationsUnion(TwoRelationsUnion twoRelationsUnion, UnionFlatteningVisitorContext context) {
            process(twoRelationsUnion.first(), context);
            process(twoRelationsUnion.second(), context);
            return null;
        }
    }

    private static class UnionFlatteningVisitorContext {
        private final List<QueriedRelation> relations = new ArrayList<>();
    }
}
