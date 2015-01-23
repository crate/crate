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
import io.crate.analyze.AnalysisMetaData;
import io.crate.analyze.OrderBy;
import io.crate.analyze.SelectAnalyzedStatement;
import io.crate.analyze.WhereClause;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.AnalyzedRelationVisitor;
import io.crate.analyze.relations.TableRelation;
import io.crate.analyze.where.WhereClauseAnalyzer;
import io.crate.analyze.where.WhereClauseContext;
import io.crate.exceptions.VersionInvalidException;
import io.crate.metadata.Routing;
import io.crate.metadata.table.TableInfo;
import io.crate.planner.PlanNodeBuilder;
import io.crate.planner.PlannerContextBuilder;
import io.crate.planner.node.dql.CollectNode;
import io.crate.planner.node.dql.DistributedGroupBy;
import io.crate.planner.node.dql.GroupByConsumer;
import io.crate.planner.node.dql.MergeNode;
import io.crate.planner.projection.FilterProjection;
import io.crate.planner.projection.GroupProjection;
import io.crate.planner.projection.Projection;
import io.crate.planner.projection.TopNProjection;
import io.crate.planner.symbol.Function;
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
        public AnalyzedRelation visitSelectAnalyzedStatement(SelectAnalyzedStatement statement, Context context) {
            // Test if statement is root relation because the rootRelation will be replaced with the returned plan
            if (context.consumerContext.rootRelation() != statement) {
                return statement;
            }
            if (statement.querySpec().groupBy()==null) {
                return statement;
            }
            TableRelation tableRelation = ConsumingPlanner.getSingleTableRelation(statement.sources());
            if (tableRelation == null) {
                return statement;
            }
            TableInfo tableInfo = tableRelation.tableInfo();
            WhereClauseAnalyzer whereClauseAnalyzer = new WhereClauseAnalyzer(analysisMetaData, tableRelation);
            WhereClauseContext whereClauseContext = whereClauseAnalyzer.analyze(statement.querySpec().where());
            WhereClause whereClause = whereClauseContext.whereClause();
            if(whereClause.version().isPresent()){
                context.consumerContext.validationException(new VersionInvalidException());
                return statement;
            }

            Routing routing = tableInfo.getRouting(whereClause, null);

            if (!GroupByConsumer.requiresDistribution(tableInfo, routing)) {
                return statement;
            }

            GroupByConsumer.validateGroupBySymbols(tableRelation, statement.querySpec().groupBy());
            PlannerContextBuilder contextBuilder = new PlannerContextBuilder(2, tableRelation.resolve(statement.querySpec().groupBy()))
                    .output(tableRelation.resolve(statement.querySpec().outputs())
                    );

            OrderBy orderBy = statement.querySpec().orderBy();
            if (orderBy != null){
                contextBuilder.orderBy(tableRelation.resolveAndValidateOrderBy(orderBy));
            }

            Symbol havingClause = null;
            if(statement.querySpec().having() != null){
                havingClause = tableRelation.resolveHaving(statement.querySpec().having());
            }
            if (havingClause != null && havingClause instanceof Function) {
                // replace aggregation symbols with input columns from previous projection
                havingClause = contextBuilder.having(havingClause);
            }

            // collector

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


            if (havingClause != null) {
                FilterProjection fp = new FilterProjection((Function)havingClause);
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
                fp.outputs(contextBuilder.genInputColumns(collectNode.finalProjection().get().outputs(), statement.querySpec().outputs().size()));
                contextBuilder.addProjection(fp);
            }
            TopNProjection topNForReducer = getTopNForReducer(statement, contextBuilder, contextBuilder.outputs());
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
            int limit = firstNonNull(statement.querySpec().limit(), Constants.DEFAULT_SELECT_LIMIT);
            TopNProjection topN;
            if (orderBy == null){
                topN = new TopNProjection(limit, statement.querySpec().offset());
            } else {
                topN = new TopNProjection(limit, statement.querySpec().offset(),
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
        private TopNProjection getTopNForReducer(SelectAnalyzedStatement analysis,
                                                 PlannerContextBuilder contextBuilder,
                                                 List<Symbol> outputs) {
            if (requireLimitOnReducer(analysis, contextBuilder.aggregationsWrappedInScalar)) {
                OrderBy orderBy = analysis.querySpec().orderBy();
                int limit = firstNonNull(analysis.querySpec().limit(), Constants.DEFAULT_SELECT_LIMIT) + analysis.querySpec().offset();
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

        private boolean requireLimitOnReducer(SelectAnalyzedStatement analysis, boolean aggregationsWrappedInScalar) {
            return (analysis.querySpec().limit() != null
                    || analysis.querySpec().offset() > 0
                    || aggregationsWrappedInScalar);
        }
    }
}
