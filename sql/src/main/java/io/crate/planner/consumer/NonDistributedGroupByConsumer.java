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
import io.crate.analyze.OrderBy;
import io.crate.analyze.QueriedTable;
import io.crate.analyze.QueriedTableRelation;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.AnalyzedRelationVisitor;
import io.crate.analyze.relations.PlannedAnalyzedRelation;
import io.crate.analyze.relations.QueriedDocTable;
import io.crate.exceptions.VersionInvalidException;
import io.crate.metadata.Functions;
import io.crate.metadata.Routing;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.planner.node.NoopPlannedAnalyzedRelation;
import io.crate.planner.node.dql.CollectPhase;
import io.crate.planner.node.dql.GroupByConsumer;
import io.crate.planner.node.dql.MergePhase;
import io.crate.planner.node.dql.NonDistributedGroupBy;
import io.crate.planner.projection.GroupProjection;
import io.crate.planner.projection.Projection;
import io.crate.planner.projection.builder.ProjectionBuilder;
import io.crate.planner.projection.builder.SplitPoints;
import io.crate.planner.symbol.Aggregation;
import io.crate.planner.symbol.Symbol;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;

import java.util.ArrayList;
import java.util.List;

@Singleton
public class NonDistributedGroupByConsumer implements Consumer {

    private final Visitor visitor;

    @Inject
    public NonDistributedGroupByConsumer(Functions functions) {
        this.visitor = new Visitor(functions);
    }

    @Override
    public PlannedAnalyzedRelation consume(AnalyzedRelation relation, ConsumerContext context) {
        return visitor.process(relation, context);
    }

    private static class Visitor extends AnalyzedRelationVisitor<ConsumerContext, PlannedAnalyzedRelation> {

        private final Functions functions;

        public Visitor(Functions functions) {
            this.functions = functions;
        }

        @Override
        public PlannedAnalyzedRelation visitQueriedDocTable(QueriedDocTable table, ConsumerContext context) {
            if (table.querySpec().groupBy() == null) {
                return null;
            }
            DocTableInfo tableInfo = table.tableRelation().tableInfo();

            if (table.querySpec().where().hasVersions()) {
                context.validationException(new VersionInvalidException());
                return null;
            }

            Routing routing = context.plannerContext().allocateRouting(tableInfo, table.querySpec().where(), null);
            if (routing.hasLocations() && routing.locations().size()>1) {
                return null;
            }
            GroupByConsumer.validateGroupBySymbols(table.tableRelation(), table.querySpec().groupBy());
            return nonDistributedGroupBy(table, routing, context);
        }

        @Override
        public PlannedAnalyzedRelation visitQueriedTable(QueriedTable table, ConsumerContext context) {
            if (table.querySpec().groupBy() == null) {
                return null;
            }
            Routing routing = context.plannerContext().allocateRouting(table.tableRelation().tableInfo(), table.querySpec().where(), null);
            return nonDistributedGroupBy(table, routing, context);
        }

        @Override
        protected PlannedAnalyzedRelation visitAnalyzedRelation(AnalyzedRelation relation, ConsumerContext context) {
            return null;
        }

        /**
         * Group by on System Tables (never needs distribution)
         * or Group by on user tables (RowGranulariy.DOC) with only one node.
         *
         * produces:
         *
         * SELECT:
         * Collect ( GroupProjection ITER -> PARTIAL )
         * LocalMerge ( GroupProjection PARTIAL -> FINAL, [FilterProjection], TopN )
         *
         */
        private PlannedAnalyzedRelation nonDistributedGroupBy(QueriedTableRelation table, Routing routing, ConsumerContext context) {
            List<Symbol> groupBy = table.querySpec().groupBy();

            ProjectionBuilder projectionBuilder = new ProjectionBuilder(functions, table.querySpec());
            SplitPoints splitPoints = projectionBuilder.getSplitPoints();

            // mapper / collect
            GroupProjection groupProjection = projectionBuilder.groupProjection(
                    splitPoints.leaves(),
                    table.querySpec().groupBy(),
                    splitPoints.aggregates(),
                    Aggregation.Step.ITER,
                    Aggregation.Step.PARTIAL);

            CollectPhase collectPhase = CollectPhase.forQueriedTable(
                    context.plannerContext(),
                    table,
                    splitPoints.leaves(),
                    ImmutableList.<Projection>of(groupProjection)
            );

            // handler
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
            projections.add(projectionBuilder.groupProjection(
                    collectOutputs,
                    table.querySpec().groupBy(),
                    splitPoints.aggregates(),
                    Aggregation.Step.PARTIAL,
                    Aggregation.Step.FINAL
            ));

            HavingClause havingClause = table.querySpec().having();
            if (havingClause != null) {
                if (havingClause.noMatch()) {
                    return new NoopPlannedAnalyzedRelation(table, context.plannerContext().jobId());
                } else if (havingClause.hasQuery()){
                    projections.add(projectionBuilder.filterProjection(
                            collectOutputs,
                            havingClause.query()
                    ));
                }
            }

            /**
             * If this is not the rootRelation this is a subquery (e.g. Insert by Query),
             * so ordering and limiting is done by the rootRelation if required.
             *
             * If the querySpec outputs don't match the collectOutputs the query contains
             * aggregations or scalar functions which can only be resolved by a TopNProjection,
             * so a TopNProjection must be added.
             */
            boolean outputsMatch = table.querySpec().outputs().size() == collectOutputs.size() &&
                                    collectOutputs.containsAll(table.querySpec().outputs());
            if (context.rootRelation() == table || !outputsMatch){
                projections.add(projectionBuilder.topNProjection(
                        collectOutputs,
                        orderBy,
                        table.querySpec().offset(),
                        table.querySpec().limit(),
                        table.querySpec().outputs()
                ));
            }
            MergePhase localMergeNode = MergePhase.localMerge(
                    context.plannerContext().jobId(),
                    context.plannerContext().nextExecutionPhaseId(),
                    projections,
                    collectPhase);
            return new NonDistributedGroupBy(collectPhase, localMergeNode, context.plannerContext().jobId());
        }
    }

}
