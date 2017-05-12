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

import io.crate.analyze.MultiSourceSelect;
import io.crate.analyze.QuerySpec;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.JoinPairs;
import io.crate.analyze.relations.QueriedRelation;
import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.symbol.Symbols;
import io.crate.collections.Lists2;
import io.crate.planner.Plan;
import io.crate.planner.projection.builder.ProjectionBuilder;
import io.crate.planner.projection.builder.SplitPoints;

import java.util.List;
import java.util.Optional;

public class MultiSourceGroupByConsumer implements Consumer {

    private final Visitor visitor;

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

            if (!querySpec.groupBy().isPresent()) {
                return null;
            }

            List<Symbol> groupKeys = querySpec.groupBy().get();
            List<Symbol> outputs = querySpec.outputs();

            // Planning the multiSourceSelect as subRelation mutates the querySpec.
            querySpec = querySpec.copyAndReplace(Symbols.DEEP_COPY);

            SplitPoints splitPoints = SplitPoints.create(querySpec);
            querySpec.hasAggregates(false);

            querySpec.outputs(splitPoints.toCollect());
            removePostGroupingActionsFromQuerySpec(multiSourceSelect, splitPoints);

            context.setFetchMode(FetchMode.NEVER);

            Plan plan = context.plannerContext().planSubRelation(multiSourceSelect, context);

            return GroupingSubselectConsumer.createPlan(
                plan,
                context,
                splitPoints,
                splitPoints.toCollect(),
                groupKeys,
                outputs,
                querySpec,
                projectionBuilder
            );
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

            for (AnalyzedRelation relation : mss.sources().values()) {
                removePostGroupingActions(((QueriedRelation) relation).querySpec());
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
