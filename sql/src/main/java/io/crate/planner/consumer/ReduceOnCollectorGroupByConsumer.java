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
import io.crate.analyze.QuerySpec;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.DocTableRelation;
import io.crate.analyze.relations.QueriedDocTable;
import io.crate.analyze.symbol.Aggregation;
import io.crate.analyze.symbol.Symbol;
import io.crate.exceptions.VersionInvalidException;
import io.crate.metadata.Functions;
import io.crate.metadata.RowGranularity;
import io.crate.operation.projectors.TopN;
import io.crate.planner.Limits;
import io.crate.planner.NoopPlan;
import io.crate.planner.Plan;
import io.crate.planner.node.dql.CollectAndMerge;
import io.crate.planner.node.dql.GroupByConsumer;
import io.crate.planner.node.dql.MergePhase;
import io.crate.planner.node.dql.RoutedCollectPhase;
import io.crate.planner.projection.FilterProjection;
import io.crate.planner.projection.GroupProjection;
import io.crate.planner.projection.Projection;
import io.crate.planner.projection.builder.ProjectionBuilder;
import io.crate.planner.projection.builder.SplitPoints;

import java.util.ArrayList;
import java.util.List;

class ReduceOnCollectorGroupByConsumer implements Consumer {

    private final Visitor visitor;

    ReduceOnCollectorGroupByConsumer(Functions functions) {
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
            DocTableRelation tableRelation = table.tableRelation();
            if (!GroupByConsumer.groupedByClusteredColumnOrPrimaryKeys(
                tableRelation, table.querySpec().where(), table.querySpec().groupBy().get())) {
                return null;
            }

            if (table.querySpec().where().hasVersions()) {
                context.validationException(new VersionInvalidException());
                return null;
            }
            return optimizedReduceOnCollectorGroupBy(table, tableRelation, context);
        }

        /**
         * grouping on doc tables by clustered column or primary keys, no distribution needed
         * only one aggregation step as the mappers (shards) have row-authority
         * <p>
         * produces:
         * <p>
         * SELECT:
         * CollectNode ( GroupProjection, [FilterProjection], [TopN] )
         * LocalMergeNode ( TopN )
         */
        private Plan optimizedReduceOnCollectorGroupBy(QueriedDocTable table, DocTableRelation tableRelation, ConsumerContext context) {
            QuerySpec querySpec = table.querySpec();
            assert GroupByConsumer.groupedByClusteredColumnOrPrimaryKeys(
                tableRelation, querySpec.where(), querySpec.groupBy().get()) : "not grouped by clustered column or primary keys";
            GroupByConsumer.validateGroupBySymbols(tableRelation, querySpec.groupBy().get());
            List<Symbol> groupBy = querySpec.groupBy().get();

            Limits limits = context.plannerContext().getLimits(querySpec);
            boolean ignoreSorting = context.rootRelation() != table
                                    && !limits.hasLimit()
                                    && limits.offset() == TopN.NO_OFFSET;


            ProjectionBuilder projectionBuilder = new ProjectionBuilder(functions, querySpec);
            SplitPoints splitPoints = projectionBuilder.getSplitPoints();

            // mapper / collect
            List<Symbol> collectOutputs = new ArrayList<>(
                groupBy.size() +
                splitPoints.aggregates().size());
            collectOutputs.addAll(groupBy);
            collectOutputs.addAll(splitPoints.aggregates());

            table.tableRelation().validateOrderBy(querySpec.orderBy());

            List<Projection> projections = new ArrayList<>();
            GroupProjection groupProjection = projectionBuilder.groupProjection(
                splitPoints.leaves(),
                querySpec.groupBy().get(),
                splitPoints.aggregates(),
                Aggregation.Step.ITER,
                Aggregation.Step.FINAL
            );
            groupProjection.setRequiredGranularity(RowGranularity.SHARD);
            projections.add(groupProjection);

            Optional<HavingClause> havingClause = querySpec.having();
            if (havingClause.isPresent()) {
                if (havingClause.get().noMatch()) {
                    return new NoopPlan(context.plannerContext().jobId());
                } else if (havingClause.get().hasQuery()) {
                    FilterProjection fp = ProjectionBuilder.filterProjection(
                        collectOutputs,
                        havingClause.get().query()
                    );
                    fp.requiredGranularity(RowGranularity.SHARD);
                    projections.add(fp);
                }
            }
            // mapper / collect
            // use topN on collector if needed
            boolean outputsMatch = querySpec.outputs().size() == collectOutputs.size() &&
                                   collectOutputs.containsAll(querySpec.outputs());

            boolean collectorTopN = limits.hasLimit() || limits.offset() > 0 || !outputsMatch;

            if (collectorTopN) {
                projections.add(ProjectionBuilder.topNProjection(
                    collectOutputs,
                    querySpec.orderBy().orNull(),
                    0, // no offset
                    limits.limitAndOffset(),
                    querySpec.outputs()
                ));
            }

            RoutedCollectPhase collectPhase = RoutedCollectPhase.forQueriedTable(
                context.plannerContext(),
                table,
                splitPoints.leaves(),
                ImmutableList.copyOf(projections)
            );

            // handler
            List<Projection> handlerProjections = new ArrayList<>();
            MergePhase localMerge;
            if (!ignoreSorting && collectorTopN && querySpec.orderBy().isPresent()) {
                // handler receives sorted results from collect nodes
                // we can do the sorting with a sorting bucket merger
                handlerProjections.add(
                    ProjectionBuilder.topNProjection(
                        querySpec.outputs(),
                        null, // omit order by
                        limits.offset(),
                        limits.finalLimit(),
                        querySpec.outputs()
                    )
                );
                localMerge = MergePhase.sortedMerge(
                    context.plannerContext().jobId(),
                    context.plannerContext().nextExecutionPhaseId(),
                    querySpec.orderBy().get(),
                    querySpec.outputs(),
                    null,
                    handlerProjections,
                    collectPhase.nodeIds().size(),
                    collectPhase.outputTypes()
                );
            } else {
                handlerProjections.add(
                    ProjectionBuilder.topNProjection(
                        collectorTopN ? querySpec.outputs() : collectOutputs,
                        querySpec.orderBy().orNull(),
                        limits.offset(),
                        limits.finalLimit(),
                        querySpec.outputs()
                    )
                );
                // fallback - unsorted local merge
                localMerge = MergePhase.localMerge(
                    context.plannerContext().jobId(),
                    context.plannerContext().nextExecutionPhaseId(),
                    handlerProjections,
                    collectPhase.nodeIds().size(),
                    collectPhase.outputTypes()
                );
            }
            return new CollectAndMerge(collectPhase, localMerge);
        }


    }
}
