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

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.crate.Constants;
import io.crate.analyze.*;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.AnalyzedRelationVisitor;
import io.crate.exceptions.VersionInvalidException;
import io.crate.metadata.Routing;
import io.crate.metadata.table.TableInfo;
import io.crate.planner.PlanNodeBuilder;
import io.crate.planner.node.NoopPlannedAnalyzedRelation;
import io.crate.planner.node.dql.CollectNode;
import io.crate.planner.node.dql.DistributedGroupBy;
import io.crate.planner.node.dql.GroupByConsumer;
import io.crate.planner.node.dql.MergeNode;
import io.crate.planner.projection.GroupProjection;
import io.crate.planner.projection.Projection;
import io.crate.planner.projection.TopNProjection;
import io.crate.planner.projection.builder.ProjectionBuilder;
import io.crate.planner.projection.builder.SplitPoints;
import io.crate.planner.symbol.Aggregation;
import io.crate.planner.symbol.Symbol;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class DistributedGroupByConsumer implements Consumer {

    private static final Visitor VISITOR = new Visitor();

    @Override
    public boolean consume(AnalyzedRelation rootRelation, ConsumerContext context) {
        Context ctx = new Context(context);
        context.rootRelation(VISITOR.process(context.rootRelation(), ctx));
        return ctx.result;
    }

    private static class Context {
        ConsumerContext consumerContext;
        boolean result = false;

        public Context(ConsumerContext context) {
            this.consumerContext = context;
        }
    }

    private static class Visitor extends AnalyzedRelationVisitor<Context, AnalyzedRelation> {

        @Override
        public AnalyzedRelation visitQueriedTable(QueriedTable table, Context context) {
            List<Symbol> groupBy = table.querySpec().groupBy();
            if (groupBy == null) {
                return table;
            }

            TableInfo tableInfo = table.tableRelation().tableInfo();
            if(table.querySpec().where().hasVersions()){
                context.consumerContext.validationException(new VersionInvalidException());
                return table;
            }

            Routing routing = tableInfo.getRouting(table.querySpec().where(), null);

            GroupByConsumer.validateGroupBySymbols(table.tableRelation(), table.querySpec().groupBy());

            ProjectionBuilder projectionBuilder = new ProjectionBuilder(table.querySpec());

            SplitPoints splitPoints = projectionBuilder.getSplitPoints();

            // start: Map/Collect side
            GroupProjection groupProjection = projectionBuilder.groupProjection(
                    splitPoints.leaves(),
                    table.querySpec().groupBy(),
                    splitPoints.aggregates(),
                    Aggregation.Step.ITER,
                    Aggregation.Step.PARTIAL);

            CollectNode collectNode = PlanNodeBuilder.distributingCollect(
                    tableInfo,
                    context.consumerContext.plannerContext(),
                    table.querySpec().where(),
                    splitPoints.leaves(),
                    Lists.newArrayList(routing.nodes()),
                    ImmutableList.<Projection>of(groupProjection)
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
                    table.querySpec().groupBy(),
                    splitPoints.aggregates(),
                    Aggregation.Step.PARTIAL,
                    Aggregation.Step.FINAL));


            OrderBy orderBy = table.querySpec().orderBy();
            if (orderBy != null) {
                table.tableRelation().validateOrderBy(orderBy);
            }


            HavingClause havingClause = table.querySpec().having();
            if (havingClause != null) {
                if (havingClause.noMatch()) {
                    return new NoopPlannedAnalyzedRelation(table);
                } else if (havingClause.hasQuery()) {
                    reducerProjections.add(projectionBuilder.filterProjection(
                            collectOutputs,
                            havingClause.query()
                    ));
                }
            }

            boolean isRootRelation = context.consumerContext.rootRelation() == table;
            if (isRootRelation) {
                reducerProjections.add(projectionBuilder.topNProjection(
                        collectOutputs,
                        orderBy,
                        0,
                        MoreObjects.firstNonNull(table.querySpec().limit(),
                                Constants.DEFAULT_SELECT_LIMIT) + table.querySpec().offset(),
                        table.querySpec().outputs()));
            }
            MergeNode mergeNode = PlanNodeBuilder.distributedMerge(
                    collectNode,
                    reducerProjections);
            // end: Reducer

            MergeNode localMergeNode = null;
            if(isRootRelation) {
                TopNProjection topN = projectionBuilder.topNProjection(
                        table.querySpec().outputs(),
                        orderBy,
                        table.querySpec().offset(),
                        table.querySpec().limit(),
                        null);
                localMergeNode = PlanNodeBuilder.localMerge(ImmutableList.<Projection>of(topN), mergeNode);
            }
            context.result = true;
            return new DistributedGroupBy(
                    collectNode,
                    mergeNode,
                    localMergeNode
            );
        }

        @Override
        public AnalyzedRelation visitInsertFromQuery(InsertFromSubQueryAnalyzedStatement insertFromSubQueryAnalyzedStatement, Context context) {
            InsertFromSubQueryConsumer.planInnerRelation(insertFromSubQueryAnalyzedStatement, context, this);
            return insertFromSubQueryAnalyzedStatement;
        }

        @Override
        protected AnalyzedRelation visitAnalyzedRelation(AnalyzedRelation relation, Context context) {
            return relation;
        }

    }
}
