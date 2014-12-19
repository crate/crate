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

package io.crate.planner.v2;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.crate.Constants;
import io.crate.analyze.AnalysisMetaData;
import io.crate.analyze.SelectAnalyzedStatement;
import io.crate.analyze.WhereClause;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.RelationVisitor;
import io.crate.analyze.relations.TableRelation;
import io.crate.analyze.where.WhereClauseAnalyzer;
import io.crate.analyze.where.WhereClauseContext;
import io.crate.metadata.Routing;
import io.crate.metadata.table.TableInfo;
import io.crate.planner.PlanNodeBuilder;
import io.crate.planner.PlannerContextBuilder;
import io.crate.planner.node.dql.CollectNode;
import io.crate.planner.node.dql.DistributedGroupByPlanNode;
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

    private static class Visitor extends RelationVisitor<Context, AnalyzedRelation> {

        private final AnalysisMetaData analysisMetaData;

        public Visitor(AnalysisMetaData analysisMetaData) {
            this.analysisMetaData = analysisMetaData;
        }

        @Override
        public AnalyzedRelation visitSelectAnalyzedStatement(SelectAnalyzedStatement statement, Context context) {
            if (context.consumerContext.rootRelation() != statement) {
                return statement;
            }
            if (!statement.hasGroupBy()) {
                return statement;
            }
            TableRelation tableRelation = ConsumingPlanner.getSingleTableRelation(statement.sources());
            if (tableRelation == null) {
                return statement;
            }
            TableInfo tableInfo = tableRelation.tableInfo();
            List<Symbol> groupBy = tableRelation.resolve(statement.groupBy());

            WhereClauseAnalyzer whereClauseAnalyzer = new WhereClauseAnalyzer(analysisMetaData, tableRelation);
            WhereClauseContext whereClauseContext = whereClauseAnalyzer.analyze(statement.whereClause());
            WhereClause whereClause = whereClauseContext.whereClause();

            Routing routing = tableInfo.getRouting(whereClause);

            if (!GroupByConsumer.requiresDistribution(tableInfo, groupBy, routing)) {
                return statement;
            }
            PlannerContextBuilder contextBuilder = new PlannerContextBuilder(2, tableRelation.resolve(statement.groupBy()))
                    .output(tableRelation.resolve(statement.outputSymbols()))
                    .orderBy(tableRelation.resolve(statement.orderBy().orderBySymbols()));

            Symbol havingClause = null;
            if(statement.havingClause() != null){
                havingClause = tableRelation.resolve(statement.havingClause());
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
                fp.outputs(contextBuilder.genInputColumns(collectNode.finalProjection().get().outputs(), statement.outputSymbols().size()));
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
            List<Symbol> orderBy;
            if (topNForReducer == null) {
                orderBy = contextBuilder.orderBy();
                outputs = contextBuilder.outputs();
            } else {
                orderBy = contextBuilder.passThroughOrderBy();
                outputs = contextBuilder.passThroughOutputs();
            }
            // mergeNode handler
            TopNProjection topN = new TopNProjection(
                    firstNonNull(statement.limit(), Constants.DEFAULT_SELECT_LIMIT),
                    statement.offset(),
                    orderBy,
                    statement.orderBy().reverseFlags(),
                    statement.orderBy().nullsFirst()
            );
            topN.outputs(outputs);
            MergeNode localMergeNode = PlanNodeBuilder.localMerge(ImmutableList.<Projection>of(topN), mergeNode);

            context.result = true;
            return new DistributedGroupByPlanNode(
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
                TopNProjection topN = new TopNProjection(
                        firstNonNull(analysis.limit(), Constants.DEFAULT_SELECT_LIMIT) + analysis.offset(),
                        0,
                        contextBuilder.orderBy(),
                        analysis.orderBy().reverseFlags(),
                        analysis.orderBy().nullsFirst()
                );
                topN.outputs(outputs);
                return topN;
            }
            return null;
        }

        private boolean requireLimitOnReducer(SelectAnalyzedStatement analysis, boolean aggregationsWrappedInScalar) {
            return (analysis.limit() != null
                    || analysis.offset() > 0
                    || aggregationsWrappedInScalar);
        }
    }
}
