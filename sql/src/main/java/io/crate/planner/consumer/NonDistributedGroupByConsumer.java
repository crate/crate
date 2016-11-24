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
import io.crate.analyze.QueriedTable;
import io.crate.analyze.QueriedTableRelation;
import io.crate.analyze.QuerySpec;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.QueriedDocTable;
import io.crate.analyze.symbol.Aggregation;
import io.crate.analyze.symbol.Symbol;
import io.crate.exceptions.VersionInvalidException;
import io.crate.metadata.Functions;
import io.crate.metadata.Routing;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.operation.projectors.TopN;
import io.crate.planner.Limits;
import io.crate.planner.Merge;
import io.crate.planner.Plan;
import io.crate.planner.node.dql.Collect;
import io.crate.planner.node.dql.GroupByConsumer;
import io.crate.planner.node.dql.RoutedCollectPhase;
import io.crate.planner.projection.GroupProjection;
import io.crate.planner.projection.Projection;
import io.crate.planner.projection.builder.ProjectionBuilder;
import io.crate.planner.projection.builder.SplitPoints;

import java.util.ArrayList;
import java.util.List;

class NonDistributedGroupByConsumer implements Consumer {

    private final Visitor visitor;

    NonDistributedGroupByConsumer(Functions functions) {
        this.visitor = new Visitor(functions);
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
            DocTableInfo tableInfo = table.tableRelation().tableInfo();

            if (table.querySpec().where().hasVersions()) {
                context.validationException(new VersionInvalidException());
                return null;
            }

            Routing routing = context.plannerContext().allocateRouting(tableInfo, table.querySpec().where(), null);
            if (routing.hasLocations() && routing.locations().size() > 1) {
                return null;
            }
            GroupByConsumer.validateGroupBySymbols(table.tableRelation(), table.querySpec().groupBy().get());
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
            QuerySpec querySpec = table.querySpec();
            List<Symbol> groupKeys = querySpec.groupBy().get();

            ProjectionBuilder projectionBuilder = new ProjectionBuilder(functions, querySpec);
            SplitPoints splitPoints = projectionBuilder.getSplitPoints();

            // mapper / collect
            GroupProjection groupProjection = projectionBuilder.groupProjection(
                splitPoints.leaves(),
                groupKeys,
                splitPoints.aggregates(),
                Aggregation.Step.ITER,
                Aggregation.Step.PARTIAL,
                groupProjectionGranularity);

            RoutedCollectPhase collectPhase = RoutedCollectPhase.forQueriedTable(
                context.plannerContext(),
                table,
                splitPoints.leaves(),
                ImmutableList.<Projection>of(groupProjection),
                table.relationId()
            );
            Collect collect = new Collect(collectPhase, TopN.NO_LIMIT, 0, groupProjection.outputs().size(), -1, null);

            // handler
            List<Symbol> collectOutputs = new ArrayList<>(
                groupKeys.size() +
                splitPoints.aggregates().size());
            collectOutputs.addAll(groupKeys);
            collectOutputs.addAll(splitPoints.aggregates());

            table.tableRelation().validateOrderBy(querySpec.orderBy());

            List<Projection> mergeProjections = new ArrayList<>();
            mergeProjections.add(projectionBuilder.groupProjection(
                collectOutputs,
                groupKeys,
                splitPoints.aggregates(),
                Aggregation.Step.PARTIAL,
                Aggregation.Step.FINAL,
                RowGranularity.CLUSTER
            ));

            Optional<HavingClause> havingClause = querySpec.having();
            if (havingClause.isPresent()) {
                HavingClause having = havingClause.get();
                mergeProjections.add(ProjectionBuilder.filterProjection(collectOutputs, having));
            }
            Limits limits = context.plannerContext().getLimits(querySpec);
            List<Symbol> qsOutputs = querySpec.outputs();
            mergeProjections.add(ProjectionBuilder.topNProjection(
                collectOutputs,
                querySpec.orderBy().orNull(),
                limits.offset(),
                limits.finalLimit(),
                qsOutputs
            ));
            return Merge.create(
                collect,
                context.plannerContext(),
                mergeProjections,
                TopN.NO_LIMIT,
                0,
                qsOutputs.size(),
                limits.finalLimit()
            );
        }
    }
}
