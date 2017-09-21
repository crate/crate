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

import io.crate.analyze.HavingClause;
import io.crate.analyze.OrderBy;
import io.crate.analyze.QueriedSelectRelation;
import io.crate.analyze.QuerySpec;
import io.crate.analyze.WhereClause;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.symbol.AggregateMode;
import io.crate.analyze.symbol.Symbol;
import io.crate.metadata.RowGranularity;
import io.crate.operation.projectors.TopN;
import io.crate.planner.Limits;
import io.crate.planner.Merge;
import io.crate.planner.Plan;
import io.crate.planner.PositionalOrderBy;
import io.crate.planner.distribution.DistributionInfo;
import io.crate.planner.node.ExecutionPhases;
import io.crate.planner.node.dql.MergePhase;
import io.crate.planner.projection.FilterProjection;
import io.crate.planner.projection.GroupProjection;
import io.crate.planner.projection.Projection;
import io.crate.planner.projection.builder.ProjectionBuilder;
import io.crate.planner.projection.builder.SplitPoints;

import java.util.ArrayList;
import java.util.List;

/**
 * This class handles subselects in combination with 'group by' by creating
 * an appropriate execution plan for a distributed and non distributed
 * case. I.e.:
 *
 * select x, count(*) from
 *   (select x from t limit 1) as tt
 * group by x;
 */
public class GroupingSubselectConsumer implements Consumer {

    private final Visitor visitor;

    GroupingSubselectConsumer(ProjectionBuilder projectionBuilder) {
        visitor = new Visitor(projectionBuilder);
    }

    @Override
    public Plan consume(AnalyzedRelation relation, ConsumerContext context) {
        return visitor.process(relation, context);
    }

    private static class Visitor extends RelationPlanningVisitor {

        private final ProjectionBuilder projectionBuilder;

        public Visitor(ProjectionBuilder projectionBuilder) {
            this.projectionBuilder = projectionBuilder;
        }

        @Override
        public Plan visitQueriedSelectRelation(QueriedSelectRelation relation, ConsumerContext context) {
            QuerySpec querySpec = relation.querySpec();

            if (querySpec.groupBy().isEmpty()) {
                return null;
            }

            Plan plan = context.plannerContext().planSubRelation(relation.subRelation(), context);

            addWhereFilterProjectionIfNecessary(plan, querySpec, relation);

            return createPlan(
                plan,
                context,
                SplitPoints.create(querySpec),
                relation.subRelation().fields(),
                querySpec.groupBy(),
                querySpec.outputs(),
                querySpec,
                projectionBuilder
            );
        }
    }

    public static Plan createPlan(Plan plan,
                                  ConsumerContext context,
                                  SplitPoints splitPoints,
                                  List<? extends Symbol> subRelationOutputs,
                                  List<Symbol> groupKeys,
                                  List<Symbol> outputs,
                                  QuerySpec querySpec,
                                  ProjectionBuilder projectionBuilder) {
        boolean isExecutedOnHandler = ExecutionPhases.executesOnHandler(
            context.plannerContext().handlerNode(),
            plan.resultDescription().nodeIds()
        );
        if (isExecutedOnHandler) {
            addNonDistributedGroupProjection(plan, splitPoints, subRelationOutputs, groupKeys, projectionBuilder);
            addFilterProjection(plan, splitPoints, groupKeys, querySpec);
            addTopN(plan, splitPoints, groupKeys, querySpec, context, outputs);
        } else {
            addDistributedGroupProjection(plan, splitPoints, subRelationOutputs, groupKeys, projectionBuilder);
            plan = createReduceMerge(plan, splitPoints, groupKeys, querySpec, context, outputs, projectionBuilder);
        }

        return plan;
    }

    /**
     * Adds the where filter projection if necessary.
     */
    private static void addWhereFilterProjectionIfNecessary(Plan plan,
                                                            QuerySpec querySpec,
                                                            QueriedSelectRelation relation) {
        WhereClause where = querySpec.where();
        if (where.hasQuery() || where.noMatch()) {
            FilterProjection filterProjection = ProjectionBuilder.filterProjection(
                relation.subRelation().fields(),
                where
            );
            plan.addProjection(filterProjection);
        }
    }

    /**
     * Adds the group projection to the given plan in order to handle the groupBy.
     */
    private static void addNonDistributedGroupProjection(Plan plan,
                                                         SplitPoints splitPoints,
                                                         List<? extends Symbol> subRelationOutputs,
                                                         List<Symbol> groupKeys,
                                                         ProjectionBuilder projectionBuilder) {
        GroupProjection groupProjection = projectionBuilder.groupProjection(
            subRelationOutputs,
            groupKeys,
            splitPoints.aggregates(),
            AggregateMode.ITER_FINAL,
            RowGranularity.CLUSTER
        );
        plan.addProjection(groupProjection);
    }

    /**
     * Adds the filter projection to the given plan in order to handle the `having` clause.
     */
    private static void addFilterProjection(Plan plan,
                                            SplitPoints splitPoints,
                                            List<Symbol> groupKeys,
                                            QuerySpec querySpec) {
        HavingClause havingClause = querySpec.having();
        if (havingClause != null) {
            List<Symbol> postGroupingOutputs = new ArrayList<>(groupKeys);
            postGroupingOutputs.addAll(splitPoints.aggregates());
            FilterProjection filterProjection = ProjectionBuilder.filterProjection(postGroupingOutputs, havingClause);
            plan.addProjection(filterProjection);
        }
    }

    /**
     * Add topN and re-order the column after messed up by groupBy.
     */
    private static void addTopN(Plan plan,
                                SplitPoints splitPoints,
                                List<Symbol> groupKeys,
                                QuerySpec querySpec,
                                ConsumerContext context,
                                List<Symbol> outputs) {
        OrderBy orderBy = querySpec.orderBy();
        Limits limits = context.plannerContext().getLimits(querySpec);
        ArrayList<Symbol> groupProjectionOutputs = new ArrayList<>(groupKeys);
        groupProjectionOutputs.addAll(splitPoints.aggregates());
        Projection postAggregationProjection = ProjectionBuilder.topNOrEval(
            groupProjectionOutputs,
            orderBy,
            limits.offset(),
            limits.finalLimit(),
            outputs
        );
        plan.addProjection(postAggregationProjection, TopN.NO_LIMIT, 0, null);
    }

    /**
     * Adds the group projection to the given plan in order to handle the groupBy.
     */
    private static void addDistributedGroupProjection(Plan plan,
                                                      SplitPoints splitPoints,
                                                      List<? extends Symbol> subRelationOutputs,
                                                      List<Symbol> groupKeys,
                                                      ProjectionBuilder projectionBuilder) {
        GroupProjection groupProjection = projectionBuilder.groupProjection(
            subRelationOutputs,
            groupKeys,
            splitPoints.aggregates(),
            AggregateMode.ITER_PARTIAL,
            RowGranularity.SHARD
        );
        plan.setDistributionInfo(DistributionInfo.DEFAULT_MODULO);
        plan.addProjection(groupProjection);
    }

    /**
     * Creates and returns the merge plan.
     */
    private static Merge createReduceMerge(Plan plan,
                                           SplitPoints splitPoints,
                                           List<Symbol> groupKeys,
                                           QuerySpec querySpec,
                                           ConsumerContext context,
                                           List<Symbol> outputs,
                                           ProjectionBuilder projectionBuilder) {
        ArrayList<Symbol> groupProjectionOutputs = new ArrayList<>(groupKeys);
        groupProjectionOutputs.addAll(splitPoints.aggregates());

        List<Projection> reducerProjections = new ArrayList<>();
        reducerProjections.add(projectionBuilder.groupProjection(
            groupProjectionOutputs,
            groupKeys,
            splitPoints.aggregates(),
            AggregateMode.PARTIAL_FINAL,
            RowGranularity.CLUSTER)
        );

        addFilterProjectionIfNecessary(reducerProjections, querySpec, groupProjectionOutputs);

        Limits limits = context.plannerContext().getLimits(querySpec);
        OrderBy orderBy = querySpec.orderBy();

        PositionalOrderBy positionalOrderBy = PositionalOrderBy.of(orderBy, outputs);
        reducerProjections.add(ProjectionBuilder.topNOrEval(
            groupProjectionOutputs,
            orderBy,
            0, // No offset since this is distributed.
            limits.limitAndOffset(),
            outputs)
        );

        return new Merge(
            plan,
            createMergePhase(context, plan, reducerProjections),
            limits.finalLimit(),
            limits.offset(),
            outputs.size(),
            limits.limitAndOffset(),
            positionalOrderBy
        );
    }

    /**
     * Add filter projection to reducer projections.
     */
    private static void addFilterProjectionIfNecessary(List<Projection> reducerProjections,
                                                       QuerySpec querySpec,
                                                       List<Symbol> collectOutputs) {
        HavingClause havingClause = querySpec.having();
        if (havingClause != null) {
            reducerProjections.add(ProjectionBuilder.filterProjection(collectOutputs, havingClause));
        }
    }

    /**
     * Creates and returns the merge phase.
     */
    private static MergePhase createMergePhase(ConsumerContext context,
                                               Plan plan,
                                               List<Projection> reducerProjections) {
        return new MergePhase(
            context.plannerContext().jobId(),
            context.plannerContext().nextExecutionPhaseId(),
            "distributed merge",
            plan.resultDescription().nodeIds().size(),
            plan.resultDescription().nodeIds(),
            plan.resultDescription().streamOutputs(),
            reducerProjections,
            DistributionInfo.DEFAULT_BROADCAST,
            null
        );
    }
}
