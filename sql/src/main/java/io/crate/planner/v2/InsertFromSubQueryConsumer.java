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


import com.google.common.collect.Lists;
import io.crate.Constants;
import io.crate.analyze.AnalysisMetaData;
import io.crate.analyze.InsertFromSubQueryAnalyzedStatement;
import io.crate.analyze.SelectAnalyzedStatement;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.RelationVisitor;
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
import io.crate.planner.node.dql.CollectNode;
import io.crate.planner.node.dql.DistributedGroupByNode;
import io.crate.planner.node.dql.GroupByConsumer;
import io.crate.planner.node.dql.MergeNode;
import io.crate.planner.projection.*;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;
import io.crate.planner.symbol.SymbolType;
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

    private static class Visitor extends RelationVisitor<Context, AnalyzedRelation> {

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
                    columns,
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
        public AnalyzedRelation visitSelectAnalyzedStatement(SelectAnalyzedStatement statement, Context context) {
            if(!context.insertVisited){
                return statement;
            }
            TableRelation tableRelation = ConsumingPlanner.getSingleTableRelation(statement.sources());
            if (tableRelation == null) {
                return statement;
            }
            WhereClauseAnalyzer whereClauseAnalyzer = new WhereClauseAnalyzer(analysisMetaData, tableRelation);
            WhereClauseContext whereClauseContext = whereClauseAnalyzer.analyze(statement.whereClause());
            if(whereClauseContext.whereClause().version().isPresent()){
                context.consumerContext.validationException(new VersionInvalidException());
                return statement;
            }
            context.result = true;
            if(statement.hasGroupBy()){
                return groupBy(statement, tableRelation, whereClauseContext, context.indexWriterProjection, analysisMetaData.functions());
            } else if(statement.hasAggregates()){
                return GlobalAggregateConsumer.globalAggregates(statement, context.indexWriterProjection);
            } else {
                return QueryAndFetchConsumer.normalSelect(statement, whereClauseContext, tableRelation,
                        context.indexWriterProjection, analysisMetaData.functions());
            }
        }

        @Override
        protected AnalyzedRelation visitAnalyzedRelation(AnalyzedRelation relation, Context context) {
            return relation;
        }

        private AnalyzedRelation groupBy(SelectAnalyzedStatement statement, TableRelation tableRelation, WhereClauseContext whereClauseContext,
                                               @Nullable ColumnIndexWriterProjection indexWriterProjection, @Nullable Functions functions){
            TableInfo tableInfo = tableRelation.tableInfo();
            if (tableInfo.schemaInfo().systemSchema() || !GroupByConsumer.requiresDistribution(tableInfo, tableInfo.getRouting(statement.whereClause()))) {
                return NonDistributedGroupByConsumer.nonDistributedGroupBy(statement, tableRelation, whereClauseContext, indexWriterProjection);
            } else if (groupedByClusteredColumnOrPrimaryKeys(statement, tableRelation)) {
                return ReduceOnCollectorGroupByConsumer.optimizedReduceOnCollectorGroupBy(statement, tableRelation, whereClauseContext, indexWriterProjection);
            } else if (indexWriterProjection != null) {
                return distributedWriterGroupBy(statement, tableRelation, whereClauseContext, indexWriterProjection, functions);
            } else {
                assert false : "this case should have been handled in the ConsumingPlanner";
            }
            return null;
        }

        private static boolean groupedByClusteredColumnOrPrimaryKeys(SelectAnalyzedStatement analysis, TableRelation tableRelation) {
            List<Symbol> groupBy = tableRelation.resolve(analysis.groupBy());
            assert groupBy != null;
            return GroupByConsumer.groupedByClusteredColumnOrPrimaryKeys(tableRelation.tableInfo(), groupBy);
        }

        /**
         * distributed collect on mapper nodes
         * with merge on reducer to final (they have row authority) and index write
         * if no limit and not offset is set
         * <p/>
         * final merge + index write on handler if limit or offset is set
         */
        private static AnalyzedRelation distributedWriterGroupBy(SelectAnalyzedStatement analysis,
                                                                 TableRelation tableRelation,
                                                                 WhereClauseContext whereClauseContext,
                                                                 Projection writerProjection,
                                                                 Functions functions) {
            boolean ignoreSorting = !analysis.isLimited();
            List<Symbol> groupBy = tableRelation.resolve(analysis.groupBy());
            PlannerContextBuilder contextBuilder = new PlannerContextBuilder(2, groupBy, ignoreSorting)
                    .output(tableRelation.resolve(analysis.outputSymbols()))
                    .orderBy(tableRelation.resolve(analysis.orderBy().orderBySymbols()));

            Symbol havingClause = null;
            if(analysis.havingClause() != null){
                havingClause = tableRelation.resolve(analysis.havingClause());
            }
            if (havingClause != null && havingClause.symbolType() == SymbolType.FUNCTION) {
                // replace aggregation symbols with input columns from previous projection
                havingClause = contextBuilder.having(havingClause);
            }

            TableInfo tableInfo = tableRelation.tableInfo();
            Routing routing = tableInfo.getRouting(whereClauseContext.whereClause());

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
                fp.outputs(contextBuilder.genInputColumns(collectNode.finalProjection().get().outputs(), analysis.outputSymbols().size()));
                contextBuilder.addProjection(fp);
            }

            boolean topNDone = false;
            if (analysis.isLimited()) {
                topNDone = true;
                TopNProjection topN = new TopNProjection(
                        firstNonNull(analysis.limit(), Constants.DEFAULT_SELECT_LIMIT) + analysis.offset(),
                        0,
                        tableRelation.resolve(analysis.orderBy().orderBySymbols()),
                        analysis.orderBy().reverseFlags(),
                        analysis.orderBy().nullsFirst()
                );
                topN.outputs(contextBuilder.outputs());
                contextBuilder.addProjection((topN));
            } else {
                contextBuilder.addProjection((writerProjection));
            }

            MergeNode mergeNode = PlanNodeBuilder.distributedMerge(collectNode, contextBuilder.getAndClearProjections());

            // local merge on handler
            if (analysis.isLimited()) {
                List<Symbol> outputs;
                List<Symbol> orderBy;
                if (topNDone) {
                    orderBy = contextBuilder.passThroughOrderBy();
                    outputs = contextBuilder.passThroughOutputs();
                } else {
                    orderBy = contextBuilder.orderBy();
                    outputs = contextBuilder.outputs();
                }
                // mergeNode handler
                TopNProjection topN = new TopNProjection(
                        firstNonNull(analysis.limit(), Constants.DEFAULT_SELECT_LIMIT),
                        analysis.offset(),
                        orderBy,
                        analysis.orderBy().reverseFlags(),
                        analysis.orderBy().nullsFirst()
                );
                topN.outputs(outputs);
                contextBuilder.addProjection(topN);
                contextBuilder.addProjection(writerProjection);
            } else {
                // sum up distributed indexWriter results
                contextBuilder.addProjection(QueryAndFetchConsumer.localMergeProjection(functions));
            }
            MergeNode localMergeNode = PlanNodeBuilder.localMerge(contextBuilder.getAndClearProjections(), mergeNode);
            return new DistributedGroupByNode(
                    collectNode,
                    mergeNode,
                    localMergeNode
            );
        }
    }

}
