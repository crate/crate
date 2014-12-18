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
            PlannerContextBuilder builder = new PlannerContextBuilder(2, groupBy)
                    .output(tableRelation.resolve(statement.outputSymbols()))
                    .orderBy(tableRelation.resolve(statement.orderBy().orderBySymbols()));

            Symbol havingClause = null;
            if (statement.havingClause() != null) {
                havingClause = tableRelation.resolve(statement.havingClause());
                if (havingClause instanceof Function) {
                    havingClause = builder.having(havingClause);
                }
            }

            GroupProjection groupProjection = new GroupProjection(builder.groupBy(), builder.aggregations());
            CollectNode collectNode = PlanNodeBuilder.distributingCollect(
                    tableInfo,
                    whereClause,
                    builder.toCollect(),
                    Lists.newArrayList(routing.nodes()),
                    ImmutableList.<Projection>of(groupProjection)
            );
            builder.nextStep();

            // mergeNode for reducer
            ImmutableList.Builder<Projection> projectionsBuilder = ImmutableList.builder();
            projectionsBuilder.add(new GroupProjection(
                    builder.groupBy(),
                    builder.aggregations()));


            if (havingClause != null) {
                FilterProjection fp = new FilterProjection((Function)havingClause);
                fp.outputs(builder.passThroughOutputs());
                projectionsBuilder.add(fp);
            }

            boolean topNDone = addTopNIfApplicableOnReducer(statement, builder, projectionsBuilder);
            MergeNode mergeNode = PlanNodeBuilder.distributedMerge(collectNode, projectionsBuilder.build());

            List<Symbol> outputs;
            List<Symbol> orderBy;
            if (topNDone) {
                orderBy = builder.passThroughOrderBy();
                outputs = builder.passThroughOutputs();
            } else {
                orderBy = builder.orderBy();
                outputs = builder.outputs();
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
         * this method adds a TopNProjection to the given projectBuilder
         * if the analysis has a limit, offset or if an aggregation is wrapped inside a scalar.
         *
         * the limit given to the topN projection will be limit + offset because there will be another
         * topN projection on the handler node which will do the final sort + limiting (if applicable)
         */
        private boolean addTopNIfApplicableOnReducer(SelectAnalyzedStatement analysis,
                                                     PlannerContextBuilder contextBuilder,
                                                     ImmutableList.Builder<Projection> projectionBuilder) {
            if (requireLimitOnReducer(analysis, contextBuilder.aggregationsWrappedInScalar)) {
                TopNProjection topN = new TopNProjection(
                        firstNonNull(analysis.limit(), Constants.DEFAULT_SELECT_LIMIT) + analysis.offset(),
                        0,
                        contextBuilder.orderBy(),
                        analysis.orderBy().reverseFlags(),
                        analysis.orderBy().nullsFirst()
                );
                topN.outputs(contextBuilder.outputs());
                projectionBuilder.add(topN);
                return true;
            }
            return false;
        }

        private boolean requireLimitOnReducer(SelectAnalyzedStatement analysis, boolean aggregationsWrappedInScalar) {
            return (analysis.limit() != null
                    || analysis.offset() > 0
                    || aggregationsWrappedInScalar);
        }
    }
}
