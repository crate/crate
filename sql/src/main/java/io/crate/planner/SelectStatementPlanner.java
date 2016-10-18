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

import io.crate.analyze.*;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.AnalyzedRelationVisitor;
import io.crate.analyze.relations.QueriedDocTable;
import io.crate.analyze.relations.QueriedRelation;
import io.crate.analyze.symbol.SelectSymbol;
import io.crate.exceptions.ValidationException;
import io.crate.exceptions.VersionInvalidException;
import io.crate.planner.consumer.ConsumerContext;
import io.crate.planner.consumer.ConsumingPlanner;
import io.crate.planner.consumer.ESGetStatementPlanner;
import io.crate.planner.fetch.FetchPushDown;
import io.crate.planner.node.ExecutionPhases;
import io.crate.planner.node.dql.QueryThenFetch;

import java.util.List;
import java.util.Map;

class SelectStatementPlanner {

    private final Visitor visitor;

    SelectStatementPlanner(ConsumingPlanner consumingPlanner) {
        visitor = new Visitor(consumingPlanner);
    }

    public Plan plan(SelectAnalyzedStatement statement, Planner.Context context) {
        return visitor.process(statement.relation(), context);
    }

    private static Plan subPlan(AnalyzedRelation rel, Planner.Context context) {
        ConsumerContext consumerContext = new ConsumerContext(rel, context);
        Plan subPlan = context.planSubRelation(rel, consumerContext);
        assert subPlan != null : "plan must not be null";
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

        private Plan invokeConsumingPlanner(AnalyzedRelation relation, Planner.Context context) {
            Plan plan = consumingPlanner.plan(relation, context);
            if (plan == null) {
                throw new UnsupportedOperationException("Cannot create plan for: " + relation);
            }
            return Merge.ensureOnHandler(plan, context);
        }

        @Override
        protected Plan visitAnalyzedRelation(AnalyzedRelation relation, Planner.Context context) {
            return invokeConsumingPlanner(relation, context);
        }

        @Override
        public Plan visitQueriedTable(QueriedTable table, Planner.Context context) {
            context.applySoftLimit(table.querySpec());
            return super.visitQueriedTable(table, context);
        }

        @Override
        public Plan visitQueriedDocTable(QueriedDocTable table, Planner.Context context) {
            QuerySpec querySpec = table.querySpec();
            context.applySoftLimit(querySpec);
            if (querySpec.hasAggregates() || querySpec.groupBy().isPresent()) {
                return invokeConsumingPlanner(table, context);
            }
            if (querySpec.where().docKeys().isPresent() && !table.tableRelation().tableInfo().isAlias()) {
                SubqueryPlanner subqueryPlanner = new SubqueryPlanner(context);
                Map<Plan, SelectSymbol> subQueries = subqueryPlanner.planSubQueries(table.querySpec());
                return MultiPhasePlan.createIfNeeded(ESGetStatementPlanner.convert(table, context), subQueries);
            }
            if (querySpec.where().hasVersions()) {
                throw new VersionInvalidException();
            }
            Limits limits = context.getLimits(querySpec);
            if (querySpec.where().noMatch() || (querySpec.limit().isPresent() && limits.finalLimit() == 0)) {
                return new NoopPlan(context.jobId());
            }

            FetchPushDown.Builder fetchPhaseBuilder = FetchPushDown.pushDown(table);
            if (fetchPhaseBuilder == null) {
                // no fetch required
                return invokeConsumingPlanner(table, context);
            }
            AnalyzedRelation subRelation = fetchPhaseBuilder.replacedRelation();
            Plan plannedSubQuery = subPlan(subRelation, context);
            assert plannedSubQuery != null : "consumingPlanner should have created a subPlan";
            plannedSubQuery = Merge.ensureOnHandler(plannedSubQuery, context);

            // fetch phase and projection can only be build after the sub-plan was processed (shards/readers allocated)
            FetchPushDown.PhaseAndProjection fetchPhaseAndProjection = fetchPhaseBuilder.build(context);
            plannedSubQuery.addProjection(
                fetchPhaseAndProjection.projection,
                null,
                null,
                fetchPhaseAndProjection.projection.outputs().size(),
                null);

            QueryThenFetch qtf = new QueryThenFetch(plannedSubQuery, fetchPhaseAndProjection.phase);
            SubqueryPlanner subqueryPlanner = new SubqueryPlanner(context);
            Map<Plan, SelectSymbol> subqueries = subqueryPlanner.planSubQueries(querySpec);
            return MultiPhasePlan.createIfNeeded(qtf, subqueries);
        }

        @Override
        public Plan visitMultiSourceSelect(MultiSourceSelect mss, Planner.Context context) {
            QuerySpec querySpec = mss.querySpec();
            context.applySoftLimit(querySpec);
            if (querySpec.hasAggregates() || querySpec.groupBy().isPresent()) {
                return invokeConsumingPlanner(mss, context);
            }
            if (querySpec.where().noMatch() && !querySpec.hasAggregates()) {
                return new NoopPlan(context.jobId());
            }
            if (mss.canBeFetched().isEmpty()) {
                return invokeConsumingPlanner(mss, context);
            }
            FetchPushDown.Builder fetchPhaseBuilder = FetchPushDown.pushDown(mss);
            assert fetchPhaseBuilder != null : "expecting fetchPhaseBuilder not to be null";

            // plan sub relation as if root so that it adds a mergePhase
            Plan plannedSubQuery = invokeConsumingPlanner(mss, context);
            assert plannedSubQuery != null : "consumingPlanner should have created a subPlan";
            // NestedLoopConsumer can return NoopPlannedAnalyzedRelation if its left or right plan
            // is noop. E.g. it is the case with creating NestedLoopConsumer for empty partitioned tables.
            if (plannedSubQuery instanceof NoopPlan) {
                return plannedSubQuery;
            }

            FetchPushDown.PhaseAndProjection fetchPhaseAndProjection = fetchPhaseBuilder.build(context);
            plannedSubQuery.addProjection(
                fetchPhaseAndProjection.projection,
                null,
                null,
                fetchPhaseAndProjection.projection.outputs().size(),
                null);
            return new QueryThenFetch(plannedSubQuery, fetchPhaseAndProjection.phase);
        }

        @Override
        public Plan visitTwoRelationsUnion(TwoRelationsUnion twoRelationsUnion, Planner.Context context) {
            // Currently we only support UNION ALL so it's ok to flatten the union pairs
            List<QueriedRelation> subRelations = UnionRewriter.flatten(twoRelationsUnion);
            UnionSelect unionSelect = new UnionSelect(subRelations, twoRelationsUnion.querySpec());

            FetchPushDown.Builder fetchPhaseBuilder = FetchPushDown.pushDown(unionSelect);
            ConsumerContext consumerContext = new ConsumerContext(unionSelect, context);
            Plan plannedSubQuery = consumingPlanner.plan(unionSelect, consumerContext);
            if (plannedSubQuery instanceof NoopPlan) {
                return plannedSubQuery;
            }
            assert plannedSubQuery != null : "consumingPlanner should have created a subPlan";
            assert ExecutionPhases.executesOnHandler(context.handlerNode(), plannedSubQuery.resultDescription().nodeIds())
                : "subQuery must not have a distributed result";

            if (fetchPhaseBuilder == null) {
                // no fetch required
                return plannedSubQuery;
            }

            FetchPushDown.PhaseAndProjection fetchPhaseAndProjection = fetchPhaseBuilder.build(context);
            plannedSubQuery.addProjection(fetchPhaseAndProjection.projection, null, null, null, null);
            return new QueryThenFetch(plannedSubQuery, fetchPhaseAndProjection.phase);
        }
    }
}
