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
import io.crate.planner.node.dql.GroupByConsumer;
import io.crate.planner.node.dql.MergeNode;
import io.crate.planner.node.dql.NonDistributedGroupBy;
import io.crate.planner.projection.*;
import io.crate.planner.symbol.Symbol;
import org.elasticsearch.common.Nullable;

import java.util.List;

import static com.google.common.base.MoreObjects.firstNonNull;

public class NonDistributedGroupByConsumer implements Consumer {

    private final Visitor visitor;

    public NonDistributedGroupByConsumer(AnalysisMetaData analysisMetaData) {
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
            if (context.consumerContext.rootRelation() != table) {
                return table;
            }
            if (table.querySpec().groupBy() == null) {
                return table;
            }
            TableInfo tableInfo = table.tableRelation().tableInfo();

            WhereClauseAnalyzer whereClauseAnalyzer = new WhereClauseAnalyzer(analysisMetaData, table.tableRelation());
            WhereClauseContext whereClauseContext = whereClauseAnalyzer.analyze(table.querySpec().where());
            WhereClause whereClause = whereClauseContext.whereClause();
            if (whereClause.version().isPresent()) {
                context.consumerContext.validationException(new VersionInvalidException());
                return table;
            }

            Routing routing = tableInfo.getRouting(whereClause, null);

            if (GroupByConsumer.requiresDistribution(tableInfo, routing) && !(tableInfo.schemaInfo().systemSchema())) {
                return table;
            }

            context.result = true;
            return nonDistributedGroupBy(table, whereClauseContext, null);
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
     * Collect ( GroupProjection ITER -> PARTIAL )
     * LocalMerge ( GroupProjection PARTIAL -> FINAL, [FilterProjection], TopN )
     *
     * INSERT FROM QUERY:
     * Collect ( GroupProjection ITER -> PARTIAL )
     * LocalMerge ( GroupProjection PARTIAL -> FINAL, [FilterProjection], [TopN], IndexWriterProjection )
     */
    public static AnalyzedRelation nonDistributedGroupBy(QueriedTable table,
                                                         WhereClauseContext whereClauseContext,
                                                         @Nullable ColumnIndexWriterProjection indexWriterProjection) {
        boolean ignoreSorting = indexWriterProjection != null;
        TableInfo tableInfo = table.tableRelation().tableInfo();

        GroupByConsumer.validateGroupBySymbols(table.tableRelation(), table.querySpec().groupBy());
        List<Symbol> groupBy = table.tableRelation().resolve(table.querySpec().groupBy());
        int numAggregationSteps = 2;

        PlannerContextBuilder contextBuilder =
                new PlannerContextBuilder(numAggregationSteps, groupBy, ignoreSorting)
                        .output(table.tableRelation().resolve(table.querySpec().outputs()))
                        .orderBy(table.tableRelation().resolveAndValidateOrderBy(table.querySpec().orderBy()));

        HavingClause havingClause = table.querySpec().having();
        Symbol havingQuery = null;
        if (havingClause != null) {
            if (havingClause.noMatch()) {
                return new NoopPlannedAnalyzedRelation(table);
            } else if (havingClause.hasQuery()){
                havingQuery = contextBuilder.having(havingClause.query());
            }
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
        if (havingQuery != null) {
            FilterProjection fp = new FilterProjection(havingQuery);
            fp.outputs(contextBuilder.genInputColumns(handlerGroupProjection.outputs(), handlerGroupProjection.outputs().size()));
            contextBuilder.addProjection(fp);
        }
        if (!ignoreSorting) {
            OrderBy orderBy = table.querySpec().orderBy();
            int limit = firstNonNull(table.querySpec().limit(), Constants.DEFAULT_SELECT_LIMIT);
            TopNProjection topN;
            if (orderBy == null) {
                topN = new TopNProjection(limit, table.querySpec().offset());

            } else {
                topN = new TopNProjection(limit, table.querySpec().offset(),
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
