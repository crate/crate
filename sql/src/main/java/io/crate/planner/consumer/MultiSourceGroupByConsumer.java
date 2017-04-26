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

import io.crate.analyze.*;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.JoinPairs;
import io.crate.analyze.symbol.AggregateMode;
import io.crate.analyze.symbol.Field;
import io.crate.analyze.symbol.FieldsVisitor;
import io.crate.analyze.symbol.Symbol;
import io.crate.collections.Lists2;
import io.crate.metadata.ReplaceMode;
import io.crate.metadata.ReplacingSymbolVisitor;
import io.crate.metadata.RowGranularity;
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
import io.crate.sql.tree.QualifiedName;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class MultiSourceGroupByConsumer implements Consumer {

    private final Visitor visitor;
    private static final ReplacingSymbolVisitor<Void> DEEP_COPY = new ReplacingSymbolVisitor<>(ReplaceMode.COPY);

    MultiSourceGroupByConsumer(ProjectionBuilder projectionBuilder) {
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
        public Plan visitMultiSourceSelect(MultiSourceSelect multiSourceSelect, ConsumerContext context) {
            QuerySpec querySpec = multiSourceSelect.querySpec();

            // Check if group by is present - if not skip and continue with next consumer in chain.
            if (!querySpec.groupBy().isPresent()) {
                return null;
            }

            List<Symbol> groupKeys = querySpec.groupBy().get();
            List<Symbol> outputs = querySpec.outputs();

            // Planning the multiSourceSelect as subRelation mutates the querySpec.
            querySpec = querySpec.copyAndReplace(s -> DEEP_COPY.process(s, null));

            SplitPoints splitPoints = SplitPoints.create(querySpec);
            querySpec.hasAggregates(false);

            querySpec.outputs(splitPoints.toCollect());

            // splitPoints.toCollect can contain new fields (if only used in having for example)
            // need to update the outputs of the source relations to include them.
            updateSourceOutputs(multiSourceSelect.sources(), splitPoints.toCollect());

            removePostGroupingActionsFromQuerySpec(multiSourceSelect, splitPoints);

            context.setFetchMode(FetchMode.NEVER);
            Plan plan = context.plannerContext().planSubRelation(multiSourceSelect, context);

            if (isExecutedOnHandler(context, plan)) {
                addNonDistributedGroupProjection(plan, splitPoints, groupKeys);
                addFilterProjection(plan, splitPoints, groupKeys, querySpec);
                addTopN(plan, splitPoints, groupKeys, querySpec, context, outputs);
            } else {
                addDistributedGroupProjection(plan, splitPoints, groupKeys);
                plan = createReduceMerge(plan, splitPoints, groupKeys, querySpec, context, outputs);
            }

            return plan;
        }

        /**
         * Returns true for non distributed and false for distributed execution.
         */
        private static boolean isExecutedOnHandler(ConsumerContext context, Plan plan) {
            return ExecutionPhases.executesOnHandler(
                context.plannerContext().handlerNode(),
                plan.resultDescription().nodeIds()
            );
        }

        /**
         * Creates and returns the merge plan.
         */
        private Merge createReduceMerge(Plan plan,
                                        SplitPoints splitPoints,
                                        List<Symbol> groupKeys,
                                        QuerySpec querySpec,
                                        ConsumerContext context,
                                        List<Symbol> outputs) {
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
            Optional<OrderBy> orderBy = querySpec.orderBy();

            PositionalOrderBy positionalOrderBy = PositionalOrderBy.of(orderBy.orElse(null), outputs);
            reducerProjections.add(ProjectionBuilder.topNOrEval(
                groupProjectionOutputs,
                orderBy.orElse(null),
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

        /**
         * Add filter projection to reducer projections.
         */
        private static void addFilterProjectionIfNecessary(List<Projection> reducerProjections,
                                                           QuerySpec querySpec,
                                                           List<Symbol> collectOutputs) {
            Optional<HavingClause> havingClause = querySpec.having();
            if (havingClause.isPresent()) {
                HavingClause having = havingClause.get();
                reducerProjections.add(ProjectionBuilder.filterProjection(collectOutputs, having));
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
            OrderBy orderBy = querySpec.orderBy().orElse(null);
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
            plan.addProjection(postAggregationProjection, null, null, null);
        }

        /**
         * Adds the group projection to the given plan in order to handle the groupBy.
         */
        private void addNonDistributedGroupProjection(Plan plan,
                                                      SplitPoints splitPoints,
                                                      List<Symbol> groupKeys) {
            GroupProjection groupProjection = projectionBuilder.groupProjection(
                splitPoints.toCollect(),
                groupKeys,
                splitPoints.aggregates(),
                AggregateMode.ITER_FINAL,
                RowGranularity.CLUSTER
            );
            plan.addProjection(groupProjection, null, null, null);
        }

        /**
         * Adds the group projection to the given plan in order to handle the groupBy.
         */
        private void addDistributedGroupProjection(Plan plan,
                                                   SplitPoints splitPoints,
                                                   List<Symbol> groupKeys) {
            GroupProjection groupProjection = projectionBuilder.groupProjection(
                splitPoints.toCollect(),
                groupKeys,
                splitPoints.aggregates(),
                AggregateMode.ITER_PARTIAL,
                RowGranularity.SHARD
            );
            plan.setDistributionInfo(DistributionInfo.DEFAULT_MODULO);
            plan.addProjection(groupProjection, null, null, null);
        }

        /**
         * Adds the filter projection to the given plan in order to handle the `having` clause.
         */
        private static void addFilterProjection(Plan plan,
                                                SplitPoints splitPoints,
                                                List<Symbol> groupKeys,
                                                QuerySpec querySpec) {
            Optional<HavingClause> havingClause = querySpec.having();
            if (havingClause.isPresent()) {
                List<Symbol> postGroupingOutputs = new ArrayList<>(groupKeys);
                postGroupingOutputs.addAll(splitPoints.aggregates());
                HavingClause having = havingClause.get();
                FilterProjection filterProjection = ProjectionBuilder.filterProjection(postGroupingOutputs, having);
                plan.addProjection(filterProjection, null, null, null);
            }
        }

        /**
         * Remove limit, offset, order by and group by from RelationSource and MultiSourceSelect QuerySpec.
         */
        private static void removePostGroupingActionsFromQuerySpec(MultiSourceSelect mss, SplitPoints splitPoints) {
            QuerySpec querySpec = mss.querySpec();
            List<Symbol> outputs = Lists2.concatUnique(
                splitPoints.toCollect(),
                JoinPairs.extractFieldsFromJoinConditions(mss.joinPairs())
            );
            querySpec.outputs(outputs);
            querySpec.hasAggregates(false);
            removePostGroupingActions(querySpec);

            for (RelationSource relationSource : mss.sources().values()) {
                removePostGroupingActions(relationSource.querySpec());
            }
        }

        /**
         *  Remove limit, offset, orderBy and groupBy from given querySpec.
         */
        private static void removePostGroupingActions(QuerySpec querySpec) {
            if (querySpec.limit().isPresent()) {
                querySpec.limit(Optional.empty());
            }
            if (querySpec.offset().isPresent()) {
                querySpec.offset(Optional.empty());
            }
            if (querySpec.orderBy().isPresent()) {
                querySpec.orderBy(null);
            }
            if (querySpec.groupBy().isPresent()) {
                querySpec.groupBy(null);
            }
        }
    }

    /**
     * Update the source outputs with potential additional fields.
     */
    private static void updateSourceOutputs(Map<QualifiedName, RelationSource> sources, ArrayList<Symbol> newOutputs) {
        java.util.function.Consumer<Field> updateConsumer = field -> {
            RelationSource relationSource = sources.get(field.relation().getQualifiedName());
            List<Symbol> currentOutputs = relationSource.querySpec().outputs();
            if (!currentOutputs.contains(field)) {
                currentOutputs.add(field);
            }
        };
        for (Symbol newOutput : newOutputs) {
            FieldsVisitor.visitFields(newOutput, updateConsumer);
        }
    }
}
