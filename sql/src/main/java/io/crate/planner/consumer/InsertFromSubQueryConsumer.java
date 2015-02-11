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
import io.crate.analyze.relations.TableRelation;
import io.crate.analyze.where.WhereClauseAnalyzer;
import io.crate.exceptions.VersionInvalidException;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Functions;
import io.crate.metadata.Routing;
import io.crate.metadata.table.TableInfo;
import io.crate.planner.PlanNodeBuilder;
import io.crate.planner.node.NoopPlannedAnalyzedRelation;
import io.crate.planner.node.dql.CollectNode;
import io.crate.planner.node.dql.DistributedGroupBy;
import io.crate.planner.node.dql.GroupByConsumer;
import io.crate.planner.node.dql.MergeNode;
import io.crate.planner.projection.ColumnIndexWriterProjection;
import io.crate.planner.projection.GroupProjection;
import io.crate.planner.projection.Projection;
import io.crate.planner.projection.builder.ProjectionBuilder;
import io.crate.planner.projection.builder.SplitPoints;
import io.crate.planner.symbol.Aggregation;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.settings.ImmutableSettings;

import java.util.ArrayList;
import java.util.LinkedList;
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
                return NonDistributedGroupByConsumer.nonDistributedGroupBy(table, indexWriterProjection);
            } else if (groupedByClusteredColumnOrPrimaryKeys(table, tableRelation)) {
                return ReduceOnCollectorGroupByConsumer.optimizedReduceOnCollectorGroupBy(table, tableRelation, indexWriterProjection);
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
            GroupByConsumer.validateGroupBySymbols(tableRelation, table.querySpec().groupBy());
            List<Symbol> groupBy = table.querySpec().groupBy();

            tableRelation.validateOrderBy(table.querySpec().orderBy());

            ProjectionBuilder projectionBuilder = new ProjectionBuilder(table.querySpec());
            SplitPoints splitPoints = projectionBuilder.getSplitPoints();

            GroupProjection groupProjection = projectionBuilder.groupProjection(
                    splitPoints.leaves(),
                    table.querySpec().groupBy(),
                    splitPoints.aggregates(),
                    Aggregation.Step.ITER,
                    Aggregation.Step.PARTIAL);
            TableInfo tableInfo = tableRelation.tableInfo();
            Routing routing = tableInfo.getRouting(whereClause, null);
            CollectNode collectNode = PlanNodeBuilder.distributingCollect(
                    tableInfo,
                    table.querySpec().where(),
                    splitPoints.leaves(),
                    Lists.newArrayList(routing.nodes()),
                    ImmutableList.<Projection>of(groupProjection)
            );

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
            if(havingClause != null){
                if (havingClause.noMatch()) {
                    return new NoopPlannedAnalyzedRelation(table);
                } else if (havingClause.hasQuery()){
                    reducerProjections.add(projectionBuilder.filterProjection(
                            collectOutputs,
                            havingClause.query()
                    ));
                }
            }

            if (table.querySpec().isLimited()) {
                int limit = firstNonNull(table.querySpec().limit(), Constants.DEFAULT_SELECT_LIMIT) + table.querySpec().offset();
                reducerProjections.add(projectionBuilder.topNProjection(
                        collectOutputs,
                        orderBy,
                        0,
                        limit,
                        table.querySpec().outputs()
                ));
            } else {
                reducerProjections.add(writerProjection);
            }

            MergeNode mergeNode = PlanNodeBuilder.distributedMerge(collectNode, reducerProjections);

            // local merge on handler
            List<Projection> handlerProjections = new ArrayList<>();
            if (table.querySpec().isLimited()) {
                handlerProjections.add(projectionBuilder.topNProjection(
                        table.querySpec().outputs(),
                        orderBy,
                        table.querySpec().offset(),
                        firstNonNull(table.querySpec().limit(), Constants.DEFAULT_SELECT_LIMIT),
                        table.querySpec().outputs()
                ));
                handlerProjections.add(writerProjection);

            } else {
                // sum up distributed indexWriter results
                handlerProjections.add(QueryAndFetchConsumer.localMergeProjection(functions));
            }
            MergeNode localMergeNode = PlanNodeBuilder.localMerge(handlerProjections, mergeNode);
            return new DistributedGroupBy(
                    collectNode,
                    mergeNode,
                    localMergeNode
            );
        }
    }

}
