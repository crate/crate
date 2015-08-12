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
import com.google.common.collect.Sets;
import io.crate.Constants;
import io.crate.analyze.OrderBy;
import io.crate.analyze.QuerySpec;
import io.crate.analyze.relations.*;
import io.crate.exceptions.VersionInvalidException;
import io.crate.metadata.DocReferenceConverter;
import io.crate.metadata.Functions;
import io.crate.metadata.OutputName;
import io.crate.operation.Paging;
import io.crate.planner.Planner;
import io.crate.planner.fetch.FetchPushDown;
import io.crate.planner.node.NoopPlannedAnalyzedRelation;
import io.crate.planner.node.dql.CollectAndMerge;
import io.crate.planner.node.dql.CollectPhase;
import io.crate.planner.node.dql.MergePhase;
import io.crate.planner.node.dql.QueryThenFetch;
import io.crate.planner.node.fetch.FetchPhase;
import io.crate.planner.projection.FetchProjection;
import io.crate.planner.projection.TopNProjection;
import io.crate.planner.projection.builder.ProjectionBuilder;
import io.crate.planner.symbol.InputColumn;
import io.crate.planner.symbol.Symbol;
import io.crate.planner.symbol.SymbolFormatter;
import io.crate.types.DataTypes;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;

import java.util.ArrayList;
import java.util.List;

@Singleton
public class QueryThenFetchConsumer implements Consumer {

    private static final InputColumn DEFAULT_DOC_ID_INPUT_COLUMN = new InputColumn(0, DataTypes.STRING);
    private final Visitor visitor;

    @Inject
    public QueryThenFetchConsumer(Functions functions) {
        visitor = new Visitor(functions);
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
            if (context.rootRelation() != table) {
                return null;
            }
            QuerySpec querySpec = table.querySpec();
            if (querySpec.hasAggregates() || querySpec.groupBy()!=null) {
                return null;
            }
            if(querySpec.where().hasVersions()){
                context.validationException(new VersionInvalidException());
                return null;
            }

            Planner.Context plannerContext = context.plannerContext();
            if (querySpec.where().noMatch()) {
                return new NoopPlannedAnalyzedRelation(table, plannerContext.jobId());
            }

            OrderBy orderBy = querySpec.orderBy();
            if (orderBy != null) {
                table.tableRelation().validateOrderBy(orderBy);
            }

            QuerySpec pushedDownSpec = FetchPushDown.pushDown(querySpec, table.tableRelation().tableInfo().ident());
            if (pushedDownSpec == null) {
                return null;
            }
            List<OutputName> outputNames = new ArrayList<>(pushedDownSpec.outputs().size());
            for (Symbol symbol : pushedDownSpec.outputs()) {
                outputNames.add(new OutputName(SymbolFormatter.format(symbol)));
            }
            QueriedDocTable subRelation = new QueriedDocTable(
                    new DocTableRelation(table.tableRelation().tableInfo()),
                    outputNames,
                    pushedDownSpec
            );
            PlannedAnalyzedRelation plannedSubQuery = plannerContext.planSubRelation(subRelation, context);
            if (plannedSubQuery == null) {
                return null;
            }

            ProjectionBuilder projectionBuilder = new ProjectionBuilder(functions, querySpec);
            List<Symbol> outputs = new ArrayList<>();
            for (Symbol symbol : querySpec.outputs()) {
                outputs.add(DocReferenceConverter.convertIfPossible(symbol, table.tableRelation().tableInfo()));
            }
            CollectAndMerge qaf = (CollectAndMerge) plannedSubQuery;
            CollectPhase collectPhase = qaf.collectPhase();
            if (collectPhase.limit() == null) {
                collectPhase.limit(Constants.DEFAULT_SELECT_LIMIT + querySpec.offset());
            }

            FetchPhase fetchPhase = new FetchPhase(
                    context.plannerContext().jobId(),
                    context.plannerContext().nextExecutionPhaseId(),
                    ImmutableList.of(collectPhase.executionPhaseId()),
                    collectPhase.executionNodes()
            );
            FetchProjection fp = new FetchProjection(
                    fetchPhase.executionPhaseId(),
                    DEFAULT_DOC_ID_INPUT_COLUMN,
                    collectPhase.toCollect(),
                    outputs,
                    table.tableRelation().tableInfo().partitionedByColumns(),
                    collectPhase.executionNodes(),
                    plannerContext.jobSearchContextIdToNode(),
                    plannerContext.jobSearchContextIdToShard()
            );

            MergePhase localMergePhase;
            assert qaf.localMerge() == null : "subRelation shouldn't plan localMerge";

            TopNProjection topN = projectionBuilder.topNProjection(
                    collectPhase.toCollect(),
                    null, // orderBy = null because stuff is pre-sorted in collectPhase and sortedLocalMerge is used
                    querySpec.offset(),
                    querySpec.limit(),
                    null
            );
            if (orderBy == null || !orderBy.isSorted()) {
                localMergePhase = MergePhase.localMerge(
                        plannerContext.jobId(),
                        plannerContext.nextExecutionPhaseId(),
                        ImmutableList.of(topN, fp),
                        collectPhase
                );
            } else {
                localMergePhase = MergePhase.sortedMerge(
                        plannerContext.jobId(),
                        plannerContext.nextExecutionPhaseId(),
                        orderBy,
                        collectPhase.toCollect(),
                        null,
                        ImmutableList.of(topN, fp),
                        collectPhase
                );
            }

            Integer limit = querySpec.limit();
            if (limit != null && limit + querySpec.offset() > Paging.PAGE_SIZE) {
                localMergePhase.executionNodes(Sets.newHashSet(plannerContext.clusterService().localNode().id()));
            }
            return new QueryThenFetch(collectPhase, fetchPhase, localMergePhase, context.plannerContext().jobId());
        }

        @Override
        protected PlannedAnalyzedRelation visitAnalyzedRelation(AnalyzedRelation relation, ConsumerContext context) {
            return null;
        }
    }
}
