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
import io.crate.analyze.HavingClause;
import io.crate.analyze.OrderBy;
import io.crate.analyze.QuerySpec;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.QueriedDocTable;
import io.crate.analyze.symbol.Aggregation;
import io.crate.analyze.symbol.Symbol;
import io.crate.collections.Lists2;
import io.crate.exceptions.VersionInvalidException;
import io.crate.metadata.Functions;
import io.crate.metadata.Routing;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.planner.Limits;
import io.crate.planner.Plan;
import io.crate.planner.Planner;
import io.crate.planner.PositionalOrderBy;
import io.crate.planner.distribution.DistributionInfo;
import io.crate.planner.node.dql.DistributedGroupBy;
import io.crate.planner.node.dql.GroupByConsumer;
import io.crate.planner.node.dql.MergePhase;
import io.crate.planner.node.dql.RoutedCollectPhase;
import io.crate.planner.projection.GroupProjection;
import io.crate.planner.projection.Projection;
import io.crate.planner.projection.builder.ProjectionBuilder;
import io.crate.planner.projection.builder.SplitPoints;
import org.elasticsearch.common.inject.Singleton;

import java.util.ArrayList;
import java.util.List;

@Singleton
class DistributedGroupByConsumer implements Consumer {

    private final Visitor visitor;

    DistributedGroupByConsumer(Functions functions) {
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
        public Plan visitQueriedDocTable(QueriedDocTable table, ConsumerContext context) {
            if (!table.querySpec().groupBy().isPresent()) {
                return null;
            }
            QuerySpec querySpec = table.querySpec();
            List<Symbol> groupBy = querySpec.groupBy().get();
            DocTableInfo tableInfo = table.tableRelation().tableInfo();
            if (querySpec.where().hasVersions()) {
                context.validationException(new VersionInvalidException());
                return null;
            }

            GroupByConsumer.validateGroupBySymbols(table.tableRelation(), groupBy);
            ProjectionBuilder projectionBuilder = new ProjectionBuilder(functions, querySpec);
            SplitPoints splitPoints = projectionBuilder.getSplitPoints();

            // start: Map/Collect side
            GroupProjection groupProjection = projectionBuilder.groupProjection(
                splitPoints.leaves(),
                groupBy,
                splitPoints.aggregates(),
                Aggregation.Step.ITER,
                Aggregation.Step.PARTIAL,
                RowGranularity.SHARD);

            Planner.Context plannerContext = context.plannerContext();
            Routing routing = plannerContext.allocateRouting(tableInfo, querySpec.where(), null);
            RoutedCollectPhase collectPhase = new RoutedCollectPhase(
                plannerContext.jobId(),
                plannerContext.nextExecutionPhaseId(),
                "distributing collect",
                routing,
                tableInfo.rowGranularity(),
                splitPoints.leaves(),
                ImmutableList.<Projection>of(groupProjection),
                querySpec.where(),
                DistributionInfo.DEFAULT_MODULO,
                table.relationId()
            );
            // end: Map/Collect side

            // start: Reducer
            List<Symbol> collectOutputs = new ArrayList<>(
                groupBy.size() +
                splitPoints.aggregates().size());
            collectOutputs.addAll(groupBy);
            collectOutputs.addAll(splitPoints.aggregates());

            List<Projection> reducerProjections = new ArrayList<>();
            reducerProjections.add(projectionBuilder.groupProjection(
                collectOutputs,
                groupBy,
                splitPoints.aggregates(),
                Aggregation.Step.PARTIAL,
                Aggregation.Step.FINAL,
                RowGranularity.CLUSTER)
            );

            table.tableRelation().validateOrderBy(querySpec.orderBy());

            Optional<HavingClause> havingClause = querySpec.having();
            if (havingClause.isPresent()) {
                HavingClause having = havingClause.get();
                reducerProjections.add(ProjectionBuilder.filterProjection(collectOutputs, having));
            }
            Limits limits = plannerContext.getLimits(querySpec);
            Optional<OrderBy> optOrderBy = querySpec.orderBy();
            List<Symbol> topNOutputs;
            if (optOrderBy.isPresent()) {
                topNOutputs = Lists2.concatUnique(querySpec.outputs(), optOrderBy.get().orderBySymbols());
            } else {
                topNOutputs = querySpec.outputs();
            }
            reducerProjections.add(ProjectionBuilder.topNProjection(
                collectOutputs,
                optOrderBy.orNull(),
                0,
                limits.limitAndOffset(),
                topNOutputs));
            MergePhase reducerMerge = new MergePhase(
                plannerContext.jobId(),
                plannerContext.nextExecutionPhaseId(),
                "distributed merge",
                collectPhase.nodeIds().size(),
                collectPhase.nodeIds(),
                collectPhase.outputTypes(),
                reducerProjections,
                DistributionInfo.DEFAULT_BROADCAST,
                null
            );
            // end: Reducer

            return new DistributedGroupBy(
                collectPhase,
                reducerMerge,
                limits.finalLimit(),
                limits.offset(),
                querySpec.outputs().size(),
                limits.limitAndOffset(),
                PositionalOrderBy.of(optOrderBy.orNull(), topNOutputs)
            );
        }
    }
}
