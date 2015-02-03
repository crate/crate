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
import com.google.common.collect.Lists;
import io.crate.Constants;
import io.crate.analyze.*;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.AnalyzedRelationVisitor;
import io.crate.analyze.where.WhereClauseAnalyzer;
import io.crate.analyze.where.WhereClauseContext;
import io.crate.exceptions.VersionInvalidException;
import io.crate.metadata.Routing;
import io.crate.metadata.table.TableInfo;
import io.crate.planner.PlanNodeBuilder;
import io.crate.planner.PlannerContextBuilder;
import io.crate.planner.node.NoopPlannedAnalyzedRelation;
import io.crate.planner.node.dql.CollectNode;
import io.crate.planner.node.dql.DistributedGroupBy;
import io.crate.planner.node.dql.GroupByConsumer;
import io.crate.planner.node.dql.MergeNode;
import io.crate.planner.projection.FilterProjection;
import io.crate.planner.projection.GroupProjection;
import io.crate.planner.projection.Projection;
import io.crate.planner.projection.TopNProjection;
import io.crate.planner.symbol.Symbol;

import javax.annotation.Nullable;
import java.util.List;

import static com.google.common.base.MoreObjects.firstNonNull;

public class DistributedGroupByConsumer implements Consumer {

    private final Visitor visitor;

    public DistributedGroupByConsumer(AnalysisMetaData analysisMetaData) {
        visitor = new Visitor(analysisMetaData);
    }

    @Override
    public boolean consume(AnalyzedRelation rootRelation, ConsumerContext context) {
        Context ctx = new Context(context);
        context.rootRelation(visitor.process(context.rootRelation(), ctx));
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

        private final AnalysisMetaData analysisMetaData;

        public Visitor(AnalysisMetaData analysisMetaData) {
            this.analysisMetaData = analysisMetaData;
        }

        @Override
        public AnalyzedRelation visitQueriedTable(QueriedTable table, Context context) {
            // Test if statement is root relation because the rootRelation will be replaced with the returned plan
            if (context.consumerContext.rootRelation() != table) {
                return table;
            }
            if (table.querySpec().groupBy()==null) {
                return table;
            }

            TableInfo tableInfo = table.tableRelation().tableInfo();
            WhereClauseAnalyzer whereClauseAnalyzer = new WhereClauseAnalyzer(analysisMetaData, table.tableRelation());
            WhereClauseContext whereClauseContext = whereClauseAnalyzer.analyze(table.querySpec().where());
            WhereClause whereClause = whereClauseContext.whereClause();
            if(whereClause.version().isPresent()){
                context.consumerContext.validationException(new VersionInvalidException());
                return table;
            }

            Routing routing = tableInfo.getRouting(whereClause, null);

            if (!GroupByConsumer.requiresDistribution(tableInfo, routing)) {
                return table;
            }

            GroupByConsumer.validateGroupBySymbols(table.tableRelation(), table.querySpec().groupBy());
            PlannerContextBuilder contextBuilder = new PlannerContextBuilder(2,
                    table.querySpec().groupBy())
                    .output(table.querySpec().outputs());

            OrderBy orderBy = table.querySpec().orderBy();
            if (orderBy != null){
                table.tableRelation().validateOrderBy(orderBy);
                contextBuilder.orderBy(orderBy.orderBySymbols());
            }

            HavingClause havingClause = table.querySpec().having();
            Symbol havingQuery = null;
            int numSymbolsWithoutHaving = contextBuilder.aggregations().size() + contextBuilder.groupBy().size();
            if (havingClause != null){
                if (havingClause.noMatch()) {
                    return new NoopPlannedAnalyzedRelation(table);
                } else if (havingClause.hasQuery()){
                    havingQuery = contextBuilder.having(havingClause.query());
                }
            }
            contextBuilder.addProjection(new GroupProjection(
                    contextBuilder.groupBy(), contextBuilder.aggregations()));
            CollectNode collectNode = PlanNodeBuilder.distributingCollect(
                    tableInfo,
                    whereClauseContext.whereClause(),
                    contextBuilder.toCollect(),
                    Lists.newArrayList(routing.nodes()),
                    contextBuilder.getAndClearProjections()
            );

            contextBuilder.nextStep();

            // mergeNode for reducer
            contextBuilder.addProjection(new GroupProjection(
                    contextBuilder.groupBy(),
                    contextBuilder.aggregations()));


            if (havingQuery != null) {
                FilterProjection fp = new FilterProjection(havingQuery);
                /**
                 * Pass through outputs from previous group by projection as-is.
                 * In case group by has more outputs than the select statement strip those outputs away.
                 *
                 * E.g.
                 *      select count(*), name from t having avg(y) > 10
                 *
                 * output from group by projection:
                 *      name, count(*), avg(y)
                 *
                 * outputs from fp:
                 *      name, count(*)
                 *
                 * Any additional aggregations in the having clause that are not part of the selectList must come
                 * AFTER the selectList aggregations
                 */
                fp.outputs(contextBuilder.genInputColumns(collectNode.finalProjection().get().outputs(), numSymbolsWithoutHaving));
                contextBuilder.addProjection(fp);
            }
            TopNProjection topNForReducer = getTopNForReducer(table.querySpec(), contextBuilder, contextBuilder.outputs());
            if (topNForReducer != null) {
                contextBuilder.addProjection(topNForReducer);
            }

            MergeNode mergeNode = PlanNodeBuilder.distributedMerge(
                    collectNode,
                    contextBuilder.getAndClearProjections());

            List<Symbol> outputs;
            List<Symbol> orderBySymbols;
            if (topNForReducer == null) {
                orderBySymbols = contextBuilder.orderBy();
                outputs = contextBuilder.outputs();
            } else {
                orderBySymbols = contextBuilder.passThroughOrderBy();
                outputs = contextBuilder.passThroughOutputs();
            }
            // mergeNode handler
            int limit = firstNonNull(table.querySpec().limit(), Constants.DEFAULT_SELECT_LIMIT);
            TopNProjection topN;
            if (orderBy == null){
                topN = new TopNProjection(limit, table.querySpec().offset());
            } else {
                topN = new TopNProjection(limit, table.querySpec().offset(),
                        orderBySymbols,
                        orderBy.reverseFlags(),
                        orderBy.nullsFirst()
                );
            }
            topN.outputs(outputs);
            MergeNode localMergeNode = PlanNodeBuilder.localMerge(ImmutableList.<Projection>of(topN), mergeNode);

            context.result = true;
            return new DistributedGroupBy(
                    collectNode,
                    mergeNode,
                    localMergeNode
            );
        }

        @Override
        protected AnalyzedRelation visitAnalyzedRelation(AnalyzedRelation relation, Context context) {
            return relation;
        }

        /**
         * returns a topNProjection intended for the reducer in a group by query.
         *
         * result will be null if topN on reducer is not needed or possible.
         *
         * the limit given to the topN projection will be limit + offset because there will be another
         * @param outputs list of outputs to add to the topNProjection if applicable.
         */
        @Nullable
        private TopNProjection getTopNForReducer(QuerySpec querySpec,
                                                 PlannerContextBuilder contextBuilder,
                                                 List<Symbol> outputs) {
            if (requireLimitOnReducer(querySpec, contextBuilder.aggregationsWrappedInScalar)) {
                OrderBy orderBy = querySpec.orderBy();
                int limit = firstNonNull(querySpec.limit(), Constants.DEFAULT_SELECT_LIMIT) + querySpec.offset();
                TopNProjection topN;
                if (orderBy==null){
                    topN = new TopNProjection(limit, 0);
                } else {
                    topN = new TopNProjection(limit, 0,
                            contextBuilder.orderBy(),
                            orderBy.reverseFlags(),
                            orderBy.nullsFirst());
                }
                topN.outputs(outputs);
                return topN;
            }
            return null;
        }

        private boolean requireLimitOnReducer(QuerySpec querySpec, boolean aggregationsWrappedInScalar) {
            return (querySpec.limit() != null
                    || querySpec.offset() > 0
                    || aggregationsWrappedInScalar);
        }
    }
}
