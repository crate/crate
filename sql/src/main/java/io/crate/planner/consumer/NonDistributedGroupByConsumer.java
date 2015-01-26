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
import io.crate.planner.node.NoopPlannedAnalyzedRelation;
import io.crate.planner.node.dql.CollectNode;
import io.crate.planner.node.dql.GroupByConsumer;
import io.crate.planner.node.dql.MergeNode;
import io.crate.planner.node.dql.NonDistributedGroupBy;
import io.crate.planner.projection.*;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.Symbol;
import org.elasticsearch.common.Nullable;

import java.util.List;

import static com.google.common.base.MoreObjects.firstNonNull;

public class NonDistributedGroupByConsumer implements Consumer {

    private final Visitor visitor;

    public NonDistributedGroupByConsumer(AnalysisMetaData analysisMetaData){
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

        public Context(ConsumerContext context){
            this.consumerContext = context;
        }
    }

    private static class Visitor extends AnalyzedRelationVisitor<Context, AnalyzedRelation> {

        private final AnalysisMetaData analysisMetaData;

        public Visitor(AnalysisMetaData analysisMetaData){
            this.analysisMetaData = analysisMetaData;
        }


        @Override
        public AnalyzedRelation visitSelectAnalyzedStatement(SelectAnalyzedStatement statement, Context context) {
            if(context.consumerContext.rootRelation() != statement){
                return statement;
            }
            if(statement.querySpec().groupBy()==null){
                return statement;
            }
            TableRelation tableRelation = ConsumingPlanner.getSingleTableRelation(statement.sources());
            if(tableRelation == null){
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

            if(GroupByConsumer.requiresDistribution(tableInfo, routing) && !(tableInfo.schemaInfo().systemSchema())){
                return statement;
            }

            context.result = true;
            return nonDistributedGroupBy(statement, tableRelation, whereClauseContext, null);
        }

        @Override
        protected AnalyzedRelation visitAnalyzedRelation(AnalyzedRelation relation, Context context) {
            return relation;
        }
    }

    /**
     * Group by on System Tables (never needs distribution)
     * or Group by on user tables (RowGranulariy.DOC) with only one node.
     *
     * produces:
     *
     * SELECT:
     *  Collect ( GroupProjection ITER -> PARTIAL )
     *  LocalMerge ( GroupProjection PARTIAL -> FINAL, [FilterProjection], TopN )
     *
     * INSERT FROM QUERY:
     *  Collect ( GroupProjection ITER -> PARTIAL )
     *  LocalMerge ( GroupProjection PARTIAL -> FINAL, [FilterProjection], [TopN], IndexWriterProjection )
     */
    public static AnalyzedRelation nonDistributedGroupBy(SelectAnalyzedStatement analysis,
                                                          TableRelation tableRelation,
                                                          WhereClauseContext whereClauseContext,
                                                          @Nullable ColumnIndexWriterProjection indexWriterProjection) {
        boolean ignoreSorting = indexWriterProjection != null;
        TableInfo tableInfo = tableRelation.tableInfo();

        GroupByConsumer.validateGroupBySymbols(tableRelation, analysis.querySpec().groupBy());
        List<Symbol> groupBy = tableRelation.resolve(analysis.querySpec().groupBy());
        int numAggregationSteps = 2;

        PlannerContextBuilder contextBuilder =
                new PlannerContextBuilder(numAggregationSteps, groupBy, ignoreSorting)
                        .output(tableRelation.resolve(analysis.querySpec().outputs()))
                        .orderBy(tableRelation.resolveAndValidateOrderBy(analysis.querySpec().orderBy()));

        Symbol havingClause = null;
        if(analysis.querySpec().having() != null){
            havingClause = tableRelation.resolveHaving(analysis.querySpec().having());
            if (!WhereClause.canMatch(havingClause)) {
                return new NoopPlannedAnalyzedRelation(analysis);
            };
        }
        if (havingClause != null && havingClause instanceof Function) {
            // extract collect symbols and such from having clause
            havingClause = contextBuilder.having(havingClause);
        }

        // mapper / collect
        GroupProjection groupProjection =
                new GroupProjection(contextBuilder.groupBy(), contextBuilder.aggregations());
        contextBuilder.addProjection(groupProjection);

        CollectNode collectNode = PlanNodeBuilder.collect(
                tableInfo,
                whereClauseContext.whereClause(),
                contextBuilder.toCollect(),
                contextBuilder.getAndClearProjections()
        );

        // handler
        contextBuilder.nextStep();
        Projection handlerGroupProjection = new GroupProjection(contextBuilder.groupBy(), contextBuilder.aggregations());
        contextBuilder.addProjection(handlerGroupProjection);
        if (havingClause != null) {
            FilterProjection fp = new FilterProjection((Function)havingClause);
            fp.outputs(contextBuilder.genInputColumns(handlerGroupProjection.outputs(), handlerGroupProjection.outputs().size()));
            contextBuilder.addProjection(fp);
        }
        if (!ignoreSorting) {
            OrderBy orderBy = analysis.querySpec().orderBy();
            int limit = firstNonNull(analysis.querySpec().limit(), Constants.DEFAULT_SELECT_LIMIT);
            TopNProjection topN;
            if (orderBy == null){
                topN = new TopNProjection(limit, analysis.querySpec().offset());

            } else {
                topN = new TopNProjection(limit, analysis.querySpec().offset(),
                        contextBuilder.orderBy(),
                        orderBy.reverseFlags(),
                        orderBy.nullsFirst());
            }
            topN.outputs(contextBuilder.outputs());
            contextBuilder.addProjection(topN);
        }
        if (indexWriterProjection != null) {
            contextBuilder.addProjection(indexWriterProjection);
        }
        MergeNode localMergeNode = PlanNodeBuilder.localMerge(contextBuilder.getAndClearProjections(), collectNode);
        return new NonDistributedGroupBy(collectNode, localMergeNode);
    }


}
