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


import com.google.common.collect.Lists;
import io.crate.Constants;
import io.crate.analyze.*;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.AnalyzedRelationVisitor;
import io.crate.analyze.relations.TableRelation;
import io.crate.analyze.where.WhereClauseAnalyzer;
import io.crate.analyze.where.WhereClauseContext;
import io.crate.exceptions.VersionInvalidException;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Functions;
import io.crate.metadata.Routing;
import io.crate.metadata.table.TableInfo;
import io.crate.planner.PlanNodeBuilder;
import io.crate.planner.PlannerContextBuilder;
import io.crate.planner.node.NoopPlannedAnalyzedRelation;
import io.crate.planner.node.dql.CollectNode;
import io.crate.planner.node.dql.DistributedGroupBy;
import io.crate.planner.node.dql.GroupByConsumer;
import io.crate.planner.node.dql.MergeNode;
import io.crate.planner.projection.*;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.settings.ImmutableSettings;

import java.util.List;

import static com.google.common.base.MoreObjects.firstNonNull;


public class InsertFromSubQueryConsumer implements Consumer {

    private final Visitor visitor;

    public InsertFromSubQueryConsumer(AnalysisMetaData analysisMetaData){
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
        boolean insertVisited = false;
        ColumnIndexWriterProjection indexWriterProjection;

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
        public AnalyzedRelation visitInsertFromQuery(InsertFromSubQueryAnalyzedStatement insertFromSubQueryAnalyzedStatement, Context context) {
            List<ColumnIdent> columns = Lists.transform(insertFromSubQueryAnalyzedStatement.columns(), new com.google.common.base.Function<Reference, ColumnIdent>() {
                @Nullable
                @Override
                public ColumnIdent apply(@Nullable Reference input) {
                    if (input == null) {
                        return null;
                    }
                    return input.info().ident().columnIdent();
                }
            });
            ColumnIndexWriterProjection indexWriterProjection = new ColumnIndexWriterProjection(
                    insertFromSubQueryAnalyzedStatement.tableInfo().ident().name(),
                    insertFromSubQueryAnalyzedStatement.tableInfo().primaryKey(),
                    insertFromSubQueryAnalyzedStatement.columns(),
                    insertFromSubQueryAnalyzedStatement.primaryKeyColumnIndices(),
                    insertFromSubQueryAnalyzedStatement.partitionedByIndices(),
                    insertFromSubQueryAnalyzedStatement.routingColumn(),
                    insertFromSubQueryAnalyzedStatement.routingColumnIndex(),
                    ImmutableSettings.EMPTY,
                    insertFromSubQueryAnalyzedStatement.tableInfo().isPartitioned()
            );

            context.insertVisited = true;
            context.indexWriterProjection = indexWriterProjection;
            return insertFromSubQueryAnalyzedStatement.subQueryRelation().accept(this, context);
        }

        @Override
        public AnalyzedRelation visitQueriedTable(QueriedTable table, Context context) {
            if(!context.insertVisited){
                return table;
            }
            TableRelation tableRelation = table.tableRelation();
            if (tableRelation == null) {
                return table;
            }
            WhereClauseAnalyzer whereClauseAnalyzer = new WhereClauseAnalyzer(analysisMetaData, tableRelation);
            WhereClause whereClause = whereClauseAnalyzer.analyze(table.querySpec().where());
            if(whereClause.version().isPresent()){
                context.consumerContext.validationException(new VersionInvalidException());
                return table;
            }
            context.result = true;
            if(table.querySpec().groupBy()!=null){
                return groupBy(table, tableRelation, whereClause, context.indexWriterProjection, analysisMetaData.functions());
            } else if(table.querySpec().hasAggregates()){
                return GlobalAggregateConsumer.globalAggregates(table, tableRelation, whereClause, context.indexWriterProjection);
            } else {
                return QueryAndFetchConsumer.normalSelect(table.querySpec(), whereClause, tableRelation,
                        context.indexWriterProjection, analysisMetaData.functions());
            }
        }

        @Override
        protected AnalyzedRelation visitAnalyzedRelation(AnalyzedRelation relation, Context context) {
            return relation;
        }

        private AnalyzedRelation groupBy(QueriedTable table, TableRelation tableRelation, WhereClause whereClause,
                                               @Nullable ColumnIndexWriterProjection indexWriterProjection, @Nullable Functions functions){
            TableInfo tableInfo = tableRelation.tableInfo();
            if (tableInfo.schemaInfo().systemSchema() || !GroupByConsumer.requiresDistribution(tableInfo, tableInfo.getRouting(table.querySpec().where(), null))) {
                return NonDistributedGroupByConsumer.nonDistributedGroupBy(table, whereClause, indexWriterProjection);
            } else if (groupedByClusteredColumnOrPrimaryKeys(table, tableRelation)) {
                return ReduceOnCollectorGroupByConsumer.optimizedReduceOnCollectorGroupBy(table, tableRelation, whereClause, indexWriterProjection);
            } else if (indexWriterProjection != null) {
                return distributedWriterGroupBy(table, tableRelation, whereClause, indexWriterProjection, functions);
            } else {
                assert false : "this case should have been handled in the ConsumingPlanner";
            }
            return null;
        }

        private static boolean groupedByClusteredColumnOrPrimaryKeys(QueriedTable analysis, TableRelation tableRelation) {
            assert analysis.querySpec().groupBy() != null;
            return GroupByConsumer.groupedByClusteredColumnOrPrimaryKeys(tableRelation, analysis.querySpec().groupBy());
        }

        /**
         * distributed collect on mapper nodes
         * with merge on reducer to final (they have row authority) and index write
         * if no limit and not offset is set
         * <p/>
         * final merge + index write on handler if limit or offset is set
         */
        private static AnalyzedRelation distributedWriterGroupBy(QueriedTable table,
                                                                 TableRelation tableRelation,
                                                                 WhereClause whereClause,
                                                                 Projection writerProjection,
                                                                 Functions functions) {
            boolean ignoreSorting = !table.querySpec().isLimited();
            GroupByConsumer.validateGroupBySymbols(tableRelation, table.querySpec().groupBy());
            List<Symbol> groupBy = tableRelation.resolve(table.querySpec().groupBy());

            tableRelation.validateOrderBy(table.querySpec().orderBy());

            PlannerContextBuilder contextBuilder = new PlannerContextBuilder(2, groupBy, ignoreSorting)
                    .output(tableRelation.resolve(table.querySpec().outputs()))
                    .orderBy(tableRelation.resolveAndValidateOrderBy(table.querySpec().orderBy()));

            HavingClause havingClause = table.querySpec().having();
            Symbol havingQuery = null;
            if(havingClause != null){
                if (havingClause.noMatch()) {
                    return new NoopPlannedAnalyzedRelation(table);
                } else if (havingClause.hasQuery()){
                    havingQuery = contextBuilder.having(havingClause.query());
                }
            }
            TableInfo tableInfo = tableRelation.tableInfo();
            Routing routing = tableInfo.getRouting(whereClause, null);

            // collector
            contextBuilder.addProjection(new GroupProjection(
                    contextBuilder.groupBy(), contextBuilder.aggregations()));
            CollectNode collectNode = PlanNodeBuilder.distributingCollect(
                    tableInfo,
                    whereClause,
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
                fp.outputs(contextBuilder.genInputColumns(collectNode.finalProjection().get().outputs(), table.querySpec().outputs().size()));
                contextBuilder.addProjection(fp);
            }

            boolean topNDone = false;
            OrderBy orderBy = table.querySpec().orderBy();
            if (table.querySpec().isLimited()) {
                topNDone = true;
                TopNProjection topN;
                int limit = firstNonNull(table.querySpec().limit(), Constants.DEFAULT_SELECT_LIMIT) + table.querySpec().offset();
                if (orderBy == null) {
                    topN = new TopNProjection(limit, 0);
                } else {
                    topN = new TopNProjection(limit, 0,
                            tableRelation.resolveAndValidateOrderBy(table.querySpec().orderBy()),
                            orderBy.reverseFlags(),
                            orderBy.nullsFirst()
                    );
                }
                topN.outputs(contextBuilder.outputs());
                contextBuilder.addProjection((topN));
            } else {
                contextBuilder.addProjection((writerProjection));
            }

            MergeNode mergeNode = PlanNodeBuilder.distributedMerge(collectNode, contextBuilder.getAndClearProjections());

            // local merge on handler
            if (table.querySpec().isLimited()) {
                List<Symbol> outputs;
                List<Symbol> orderBySymbols;
                if (topNDone) {
                    orderBySymbols = contextBuilder.passThroughOrderBy();
                    outputs = contextBuilder.passThroughOutputs();
                } else {
                    orderBySymbols = contextBuilder.orderBy();
                    outputs = contextBuilder.outputs();
                }
                // mergeNode handler
                TopNProjection topN;
                int limit = firstNonNull(table.querySpec().limit(), Constants.DEFAULT_SELECT_LIMIT);
                if (orderBy == null) {
                    topN = new TopNProjection(limit, table.querySpec().offset());
                } else {
                    topN = new TopNProjection(limit, table.querySpec().offset(),
                            orderBySymbols,
                            orderBy.reverseFlags(),
                            orderBy.nullsFirst()
                    );
                }
                topN.outputs(outputs);
                contextBuilder.addProjection(topN);
                contextBuilder.addProjection(writerProjection);
            } else {
                // sum up distributed indexWriter results
                contextBuilder.addProjection(QueryAndFetchConsumer.localMergeProjection(functions));
            }
            MergeNode localMergeNode = PlanNodeBuilder.localMerge(contextBuilder.getAndClearProjections(), mergeNode);
            return new DistributedGroupBy(
                    collectNode,
                    mergeNode,
                    localMergeNode
            );
        }
    }

}
