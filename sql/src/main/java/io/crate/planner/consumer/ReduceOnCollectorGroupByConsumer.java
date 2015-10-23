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
import io.crate.Constants;
import io.crate.analyze.HavingClause;
import io.crate.analyze.OrderBy;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.DocTableRelation;
import io.crate.analyze.relations.PlannedAnalyzedRelation;
import io.crate.analyze.relations.QueriedDocTable;
import io.crate.analyze.symbol.Aggregation;
import io.crate.analyze.symbol.Symbol;
import io.crate.exceptions.VersionInvalidException;
import io.crate.metadata.Functions;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.table.TableInfo;
import io.crate.operation.projectors.TopN;
import io.crate.planner.node.NoopPlannedAnalyzedRelation;
import io.crate.planner.node.dql.CollectPhase;
import io.crate.planner.node.dql.GroupByConsumer;
import io.crate.planner.node.dql.MergePhase;
import io.crate.planner.node.dql.NonDistributedGroupBy;
import io.crate.planner.projection.FilterProjection;
import io.crate.planner.projection.GroupProjection;
import io.crate.planner.projection.Projection;
import io.crate.planner.projection.builder.ProjectionBuilder;
import io.crate.planner.projection.builder.SplitPoints;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.MoreObjects.firstNonNull;

@Singleton
public class ReduceOnCollectorGroupByConsumer implements Consumer {

    private final Visitor visitor;

    @Inject
    public ReduceOnCollectorGroupByConsumer(Functions functions) {
        visitor = new Visitor(functions);
    }

    @Override
    public PlannedAnalyzedRelation consume(AnalyzedRelation relation, ConsumerContext context) {
        return visitor.process(relation, context);
    }

    private static class Visitor extends RelationPlanningVisitor {

        private final Functions functions;

        public Visitor(Functions functions) {
            this.functions = functions;
        }

        @Override
        public PlannedAnalyzedRelation visitQueriedDocTable(QueriedDocTable table, ConsumerContext context) {
            if (table.querySpec().groupBy() == null) {
                return null;
            }
            DocTableRelation tableRelation = table.tableRelation();
            if (!GroupByConsumer.groupedByClusteredColumnOrPrimaryKeys(
                    tableRelation, table.querySpec().where(), table.querySpec().groupBy())) {
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
         *
         * produces:
         *
         * SELECT:
         * CollectNode ( GroupProjection, [FilterProjection], [TopN] )
         * LocalMergeNode ( TopN )
         */
        private PlannedAnalyzedRelation optimizedReduceOnCollectorGroupBy(QueriedDocTable table, DocTableRelation tableRelation, ConsumerContext context) {
            assert GroupByConsumer.groupedByClusteredColumnOrPrimaryKeys(
                    tableRelation, table.querySpec().where(), table.querySpec().groupBy()) : "not grouped by clustered column or primary keys";
            TableInfo tableInfo = tableRelation.tableInfo();
            GroupByConsumer.validateGroupBySymbols(tableRelation, table.querySpec().groupBy());
            List<Symbol> groupBy = table.querySpec().groupBy();

            boolean ignoreSorting = context.rootRelation() != table
                    && table.querySpec().limit() == null
                    && table.querySpec().offset() == TopN.NO_OFFSET;

            ProjectionBuilder projectionBuilder = new ProjectionBuilder(functions, table.querySpec());
            SplitPoints splitPoints = projectionBuilder.getSplitPoints();

            // mapper / collect
            List<Symbol> collectOutputs = new ArrayList<>(
                    groupBy.size() +
                            splitPoints.aggregates().size());
            collectOutputs.addAll(groupBy);
            collectOutputs.addAll(splitPoints.aggregates());

            OrderBy orderBy = table.querySpec().orderBy();
            if (orderBy != null) {
                table.tableRelation().validateOrderBy(orderBy);
            }

            List<Projection> projections = new ArrayList<>();
            GroupProjection groupProjection = projectionBuilder.groupProjection(
                    splitPoints.leaves(),
                    table.querySpec().groupBy(),
                    splitPoints.aggregates(),
                    Aggregation.Step.ITER,
                    Aggregation.Step.FINAL
            );
            groupProjection.setRequiredGranularity(RowGranularity.SHARD);
            projections.add(groupProjection);

            HavingClause havingClause = table.querySpec().having();
            if (havingClause != null) {
                if (havingClause.noMatch()) {
                    return new NoopPlannedAnalyzedRelation(table, context.plannerContext().jobId());
                } else if (havingClause.hasQuery()) {
                    FilterProjection fp = projectionBuilder.filterProjection(
                            collectOutputs,
                            havingClause.query()
                    );
                    fp.requiredGranularity(RowGranularity.SHARD);
                    projections.add(fp);
                }
            }
            // mapper / collect
            // use topN on collector if needed
            boolean outputsMatch = table.querySpec().outputs().size() == collectOutputs.size() &&
                    collectOutputs.containsAll(table.querySpec().outputs());
            boolean collectorTopN = table.querySpec().limit() != null || table.querySpec().offset() > 0 || !outputsMatch;

            if (collectorTopN) {
                projections.add(projectionBuilder.topNProjection(
                        collectOutputs,
                        orderBy,
                        0, // no offset
                        firstNonNull(table.querySpec().limit(), Constants.DEFAULT_SELECT_LIMIT) + table.querySpec().offset(),
                        table.querySpec().outputs()
                ));
            }

            CollectPhase collectPhase = CollectPhase.forQueriedTable(
                    context.plannerContext(),
                    table,
                    splitPoints.leaves(),
                    ImmutableList.copyOf(projections)
            );

            // handler
            List<Projection> handlerProjections = new ArrayList<>();
            MergePhase localMerge;
            if (!ignoreSorting && collectorTopN && orderBy != null) {
                // handler receives sorted results from collect nodes
                // we can do the sorting with a sorting bucket merger
                handlerProjections.add(
                        projectionBuilder.topNProjection(
                                table.querySpec().outputs(),
                                null, // omit order by
                                table.querySpec().offset(),
                                firstNonNull(table.querySpec().limit(), Constants.DEFAULT_SELECT_LIMIT),
                                table.querySpec().outputs()
                        )
                );
                localMerge = MergePhase.sortedMerge(
                        context.plannerContext().jobId(),
                        context.plannerContext().nextExecutionPhaseId(),
                        orderBy,
                        table.querySpec().outputs(),
                        null,
                        handlerProjections,
                        collectPhase.executionNodes().size(),
                        collectPhase.outputTypes()
                );
            } else {
                handlerProjections.add(
                        projectionBuilder.topNProjection(
                                collectorTopN ? table.querySpec().outputs() : collectOutputs,
                                orderBy,
                                table.querySpec().offset(),
                                firstNonNull(table.querySpec().limit(), Constants.DEFAULT_SELECT_LIMIT),
                                table.querySpec().outputs()
                        )
                );
                // fallback - unsorted local merge
                localMerge = MergePhase.localMerge(
                        context.plannerContext().jobId(),
                        context.plannerContext().nextExecutionPhaseId(),
                        handlerProjections,
                        collectPhase.executionNodes().size(),
                        collectPhase.outputTypes()
                );
            }
            return new NonDistributedGroupBy(collectPhase, localMerge, context.plannerContext().jobId());
        }


    }
}
