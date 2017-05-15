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
import io.crate.analyze.QueriedTable;
import io.crate.analyze.QueriedTableRelation;
import io.crate.analyze.QuerySpec;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.QueriedDocTable;
import io.crate.analyze.symbol.AggregateMode;
import io.crate.analyze.symbol.Symbol;
import io.crate.collections.Lists2;
import io.crate.exceptions.VersionInvalidException;
import io.crate.metadata.Routing;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.operation.projectors.TopN;
import io.crate.planner.Limits;
import io.crate.planner.Merge;
import io.crate.planner.Plan;
import io.crate.planner.Planner;
import io.crate.planner.distribution.DistributionInfo;
import io.crate.planner.node.dql.Collect;
import io.crate.planner.node.dql.GroupByConsumer;
import io.crate.planner.node.dql.MergePhase;
import io.crate.planner.node.dql.RoutedCollectPhase;
import io.crate.planner.projection.GroupProjection;
import io.crate.planner.projection.Projection;
import io.crate.planner.projection.builder.ProjectionBuilder;
import io.crate.planner.projection.builder.SplitPoints;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

class NonDistributedGroupByConsumer implements Consumer {

    private final Visitor visitor;

    NonDistributedGroupByConsumer(ProjectionBuilder projectionBuilder) {
        this.visitor = new Visitor(projectionBuilder);
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
        public Plan visitQueriedDocTable(QueriedDocTable table, ConsumerContext context) {
            if (!table.querySpec().groupBy().isPresent()) {
                return null;
            }
            DocTableInfo tableInfo = table.tableRelation().tableInfo();

            if (table.querySpec().where().hasVersions()) {
                context.validationException(new VersionInvalidException());
                return null;
            }

            Routing routing = context.plannerContext().allocateRouting(tableInfo, table.querySpec().where(), null);
            if (routing.hasLocations() && routing.locations().size() > 1) {
                return null;
            }
            GroupByConsumer.validateGroupBySymbols(table.querySpec().groupBy().get());
            return nonDistributedGroupBy(table, context, RowGranularity.SHARD);
        }

        @Override
        public Plan visitQueriedTable(QueriedTable table, ConsumerContext context) {
            if (!table.querySpec().groupBy().isPresent()) {
                return null;
            }
            return nonDistributedGroupBy(table, context, RowGranularity.CLUSTER);
        }

        /**
         * Group by on System Tables (never needs distribution)
         * or Group by on user tables (RowGranulariy.DOC) with only one node.
         * <p>
         * produces:
         * <p>
         * SELECT:
         * Collect ( GroupProjection ITER -> PARTIAL )
         * LocalMerge ( GroupProjection PARTIAL -> FINAL, [FilterProjection], TopN )
         */
        private Plan nonDistributedGroupBy(QueriedTableRelation table,
                                           ConsumerContext context,
                                           RowGranularity groupProjectionGranularity) {
            Planner.Context plannerContext = context.plannerContext();
            QuerySpec querySpec = table.querySpec();
            List<Symbol> groupKeys = querySpec.groupBy().get();

            SplitPoints splitPoints = SplitPoints.create(querySpec);

            // mapper / collect
            GroupProjection groupProjection = projectionBuilder.groupProjection(
                splitPoints.toCollect(),
                groupKeys,
                splitPoints.aggregates(),
                AggregateMode.ITER_PARTIAL,
                groupProjectionGranularity);

            RoutedCollectPhase collectPhase = RoutedCollectPhase.forQueriedTable(
                plannerContext,
                table,
                splitPoints.toCollect(),
                ImmutableList.of(groupProjection));
            Collect collect = new Collect(
                collectPhase,
                TopN.NO_LIMIT,
                0,
                groupProjection.outputs().size(),
                -1,
                null);

            // handler
            List<Symbol> collectOutputs = Lists2.concat(groupKeys, splitPoints.aggregates());

            List<Projection> mergeProjections = new ArrayList<>();
            mergeProjections.add(projectionBuilder.groupProjection(
                collectOutputs,
                groupKeys,
                splitPoints.aggregates(),
                AggregateMode.PARTIAL_FINAL,
                RowGranularity.CLUSTER
            ));

            Optional<HavingClause> havingClause = querySpec.having();
            if (havingClause.isPresent()) {
                HavingClause having = havingClause.get();
                mergeProjections.add(ProjectionBuilder.filterProjection(collectOutputs, having));
            }
            Limits limits = plannerContext.getLimits(querySpec);
            List<Symbol> qsOutputs = querySpec.outputs();
            mergeProjections.add(ProjectionBuilder.topNOrEval(
                collectOutputs,
                querySpec.orderBy().orElse(null),
                limits.offset(),
                limits.finalLimit(),
                qsOutputs
            ));

            MergePhase mergePhase = new MergePhase(
                plannerContext.jobId(),
                plannerContext.nextExecutionPhaseId(),
                "mergeOnHandler",
                collect.resultDescription().nodeIds().size(),
                Collections.singletonList(plannerContext.handlerNode()),
                collect.resultDescription().streamOutputs(),
                mergeProjections,
                DistributionInfo.DEFAULT_SAME_NODE,
                null
            );
            return new Merge(collect, mergePhase, TopN.NO_LIMIT, 0, qsOutputs.size(), limits.finalLimit(), null);
        }
    }
}
