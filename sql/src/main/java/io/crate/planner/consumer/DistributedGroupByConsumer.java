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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import io.crate.Constants;
import io.crate.analyze.HavingClause;
import io.crate.analyze.QuerySpec;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.PlannedAnalyzedRelation;
import io.crate.analyze.relations.QueriedDocTable;
import io.crate.analyze.symbol.Aggregation;
import io.crate.analyze.symbol.Symbol;
import io.crate.exceptions.VersionInvalidException;
import io.crate.metadata.Functions;
import io.crate.metadata.Routing;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.planner.Planner;
import io.crate.planner.distribution.DistributionInfo;
import io.crate.planner.node.NoopPlannedAnalyzedRelation;
import io.crate.planner.node.dql.CollectPhase;
import io.crate.planner.node.dql.DistributedGroupBy;
import io.crate.planner.node.dql.GroupByConsumer;
import io.crate.planner.node.dql.MergePhase;
import io.crate.planner.projection.GroupProjection;
import io.crate.planner.projection.Projection;
import io.crate.planner.projection.TopNProjection;
import io.crate.planner.projection.builder.ProjectionBuilder;
import io.crate.planner.projection.builder.SplitPoints;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

@Singleton
public class DistributedGroupByConsumer implements Consumer {

    private final Visitor visitor;

    @Inject
    public DistributedGroupByConsumer(Functions functions) {
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
            if (!table.querySpec().groupBy().isPresent()) {
                return null;
            }
            QuerySpec querySpec = table.querySpec();
            List<Symbol> groupBy = querySpec.groupBy().get();
            DocTableInfo tableInfo = table.tableRelation().tableInfo();
            if(querySpec.where().hasVersions()){
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
                    Aggregation.Step.PARTIAL);

            Planner.Context plannerContext = context.plannerContext();
            Routing routing = plannerContext.allocateRouting(tableInfo, querySpec.where(), null);
            CollectPhase collectNode = new CollectPhase(
                    plannerContext.jobId(),
                    plannerContext.nextExecutionPhaseId(),
                    "distributing collect",
                    routing,
                    tableInfo.rowGranularity(),
                    splitPoints.leaves(),
                    ImmutableList.<Projection>of(groupProjection),
                    querySpec.where(),
                    DistributionInfo.DEFAULT_MODULO
            );
            // end: Map/Collect side

            // start: Reducer
            List<Symbol> collectOutputs = new ArrayList<>(
                    groupBy.size() +
                            splitPoints.aggregates().size());
            collectOutputs.addAll(groupBy);
            collectOutputs.addAll(splitPoints.aggregates());

            List<Projection> reducerProjections = new LinkedList<>();
            reducerProjections.add(projectionBuilder.groupProjection(
                    collectOutputs,
                    groupBy,
                    splitPoints.aggregates(),
                    Aggregation.Step.PARTIAL,
                    Aggregation.Step.FINAL)
            );

            table.tableRelation().validateOrderBy(querySpec.orderBy());

            Optional<HavingClause> havingClause = querySpec.having();
            if (havingClause.isPresent()) {
                if (havingClause.get().noMatch()) {
                    return new NoopPlannedAnalyzedRelation(table, plannerContext.jobId());
                } else if (havingClause.get().hasQuery()) {
                    reducerProjections.add(projectionBuilder.filterProjection(
                            collectOutputs,
                            havingClause.get().query()
                    ));
                }
            }

            boolean isRootRelation = context.rootRelation() == table;
            if (isRootRelation) {
                reducerProjections.add(projectionBuilder.topNProjection(
                        collectOutputs,
                        querySpec.orderBy().orNull(),
                        0,
                        querySpec.limit().or(Constants.DEFAULT_SELECT_LIMIT) + querySpec.offset(),
                        querySpec.outputs()));
            }

            MergePhase mergePhase = new MergePhase(
                    plannerContext.jobId(),
                    plannerContext.nextExecutionPhaseId(),
                    "distributed merge",
                    collectNode.executionNodes().size(),
                    collectNode.outputTypes(),
                    reducerProjections,
                    DistributionInfo.DEFAULT_BROADCAST
            );
            mergePhase.executionNodes(ImmutableSet.copyOf(collectNode.executionNodes()));
            // end: Reducer

            MergePhase localMergeNode = null;
            String localNodeId = plannerContext.clusterService().state().nodes().localNodeId();
            if(isRootRelation) {
                TopNProjection topN = projectionBuilder.topNProjection(
                        querySpec.outputs(),
                        querySpec.orderBy().orNull(),
                        querySpec.offset(),
                        querySpec.limit().orNull(),
                        null);
                localMergeNode = MergePhase.localMerge(
                        plannerContext.jobId(),
                        plannerContext.nextExecutionPhaseId(),
                        ImmutableList.<Projection>of(topN),
                        mergePhase.executionNodes().size(),
                        mergePhase.outputTypes());
                localMergeNode.executionNodes(Sets.newHashSet(localNodeId));
            }

            return new DistributedGroupBy(
                    collectNode,
                    mergePhase,
                    localMergeNode,
                    plannerContext.jobId()
            );
        }
    }
}
