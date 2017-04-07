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
import io.crate.analyze.symbol.*;
import io.crate.collections.Lists2;
import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.metadata.Functions;
import io.crate.metadata.RowGranularity;
import io.crate.planner.Limits;
import io.crate.planner.Plan;
import io.crate.planner.node.ExecutionPhases;
import io.crate.planner.projection.FilterProjection;
import io.crate.planner.projection.GroupProjection;
import io.crate.planner.projection.Projection;
import io.crate.planner.projection.builder.ProjectionBuilder;
import io.crate.planner.projection.builder.SplitPoints;

import java.util.*;
import java.util.function.Function;

public class MultiSourceGroupByConsumer implements Consumer {

    private final Visitor visitor;

    MultiSourceGroupByConsumer(Functions functions) {
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
            QuerySpec querySpec = multiSourceSelect.querySpec();

            // Check if group by is present - if not skip and continue with next consumer in chain.
            if (!querySpec.groupBy().isPresent()) {
                return null;
            }

            // Copy because MSS planning mutates symbols.
            querySpec = querySpec.copyAndReplace(Function.identity());

            List<Symbol> groupKeys = querySpec.groupBy().get();
            SplitPoints splitPoints = SplitPoints.create(querySpec);

            List<Symbol> outputs = querySpec.outputs();
            if (querySpec.hasAggregates() == true) {
                querySpec.hasAggregates(false);
                querySpec.outputs(splitPoints.toCollect());
                // TODO: Alter the fields on multiSourceSelect because the fields doesn't match the outputs anymore.
                // This is currently not necessary since there is no parent relation.
            }

            removePostGroupingActionsFromQuerySpec(multiSourceSelect, splitPoints);
            Plan plan = createSubPlan(context, multiSourceSelect);
            addGroupProtection(plan, splitPoints, groupKeys);
            addFilterProtection(plan, groupKeys, splitPoints, querySpec);
            addTopN(plan, context, splitPoints, groupKeys, querySpec, outputs);

            return plan;
        };

        /**
         * Create and return the execution plan depending on distributed or non distributed execution.
         */
        private static Plan createSubPlan(ConsumerContext context, MultiSourceSelect multiSourceSelect) {
            Plan plan = context.plannerContext().planSubRelation(multiSourceSelect, context);
            boolean executesOnHandler = ExecutionPhases.executesOnHandler(
                context.plannerContext().handlerNode(),
                plan.resultDescription().nodeIds()
            );
            if (!executesOnHandler) {
                // TODO: support distributed execution.
                throw new UnsupportedFeatureException("Distributed execution is not supported");
            }

            return plan;
        }

        /**
         * Add topN and re-order the column after messed up by groupBy.
         */
        private static void addTopN(Plan plan,
                                    ConsumerContext context,
                                    SplitPoints splitPoints,
                                    List<Symbol> groupKeys,
                                    QuerySpec querySpec,
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
            plan.addProjection(postAggregationProjection, null, null, outputs.size(), null);
        }

        /**
         * Adds the group protection to the given plan in order to handle the groupBy.
         */
        private void addGroupProtection(Plan plan,
                                        SplitPoints splitPoints,
                                        List<Symbol> groupKeys) {
            ProjectionBuilder projectionBuilder = new ProjectionBuilder(functions);

            GroupProjection groupProjection = projectionBuilder.groupProjection(
                splitPoints.toCollect(),
                groupKeys,
                splitPoints.aggregates(),
                AggregateMode.ITER_FINAL,
                RowGranularity.CLUSTER
            );
            plan.addProjection(groupProjection, null, null, groupProjection.outputs().size(), null);
        }

        /**
         * Adds the filter protection to the given plan in order to handle the `having` clause.
         */
        private static void addFilterProtection(Plan plan,
                                                List<Symbol> groupKeys,
                                                SplitPoints splitPoints,
                                                QuerySpec querySpec) {
            Optional<HavingClause> havingClause = querySpec.having();

            if (havingClause.isPresent()) {
                List<Symbol> postGroupingOutputs = new ArrayList<>(
                    groupKeys.size() +
                    splitPoints.aggregates().size());
                postGroupingOutputs.addAll(groupKeys);
                postGroupingOutputs.addAll(splitPoints.aggregates());
                HavingClause having = havingClause.get();
                FilterProjection filterProjection = ProjectionBuilder.filterProjection(postGroupingOutputs, having);
                plan.addProjection(filterProjection, null, null, null, null);
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
}
