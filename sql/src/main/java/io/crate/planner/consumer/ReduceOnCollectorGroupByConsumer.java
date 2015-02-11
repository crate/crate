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
import io.crate.Constants;
import io.crate.analyze.*;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.AnalyzedRelationVisitor;
import io.crate.analyze.relations.TableRelation;
import io.crate.analyze.where.WhereClauseAnalyzer;
import io.crate.exceptions.VersionInvalidException;
import io.crate.metadata.table.TableInfo;
import io.crate.operation.projectors.TopN;
import io.crate.planner.PlanNodeBuilder;
import io.crate.planner.RowGranularity;
import io.crate.planner.node.NoopPlannedAnalyzedRelation;
import io.crate.planner.node.dql.CollectNode;
import io.crate.planner.node.dql.GroupByConsumer;
import io.crate.planner.node.dql.MergeNode;
import io.crate.planner.node.dql.NonDistributedGroupBy;
import io.crate.planner.projection.ColumnIndexWriterProjection;
import io.crate.planner.projection.FilterProjection;
import io.crate.planner.projection.GroupProjection;
import io.crate.planner.projection.Projection;
import io.crate.planner.projection.builder.ProjectionBuilder;
import io.crate.planner.projection.builder.SplitPoints;
import io.crate.planner.symbol.Aggregation;
import io.crate.planner.symbol.Symbol;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.MoreObjects.firstNonNull;

public class ReduceOnCollectorGroupByConsumer implements Consumer {

    private final Visitor visitor;

    public ReduceOnCollectorGroupByConsumer(AnalysisMetaData analysisMetaData) {
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
            if (!GroupByConsumer.groupedByClusteredColumnOrPrimaryKeys(table.tableRelation(), table.querySpec().groupBy())) {
                return table;
            }

            WhereClauseAnalyzer whereClauseAnalyzer = new WhereClauseAnalyzer(analysisMetaData, table.tableRelation());
            WhereClause whereClause = whereClauseAnalyzer.analyze(table.querySpec().where());
            if (whereClause.version().isPresent()) {
                context.consumerContext.validationException(new VersionInvalidException());
                return table;
            }
            context.result = true;
            return ReduceOnCollectorGroupByConsumer.optimizedReduceOnCollectorGroupBy(table, table.tableRelation(),
                    whereClause, null);
        }


        @Override
        protected AnalyzedRelation visitAnalyzedRelation(AnalyzedRelation relation, Context context) {
            return relation;
        }


    }

    /**
     * grouping on doc tables by clustered column or primary keys, no distribution needed
     * only one aggregation step as the mappers (shards) have row-authority
     *
     * produces:
     *
     * SELECT:
     * CollectNode ( GroupProjection, [FilterProjection], [TopN] )
     * LocalMergeNode ( TopN )
     *
     * INSERT FROM QUERY:
     * CollectNode ( GroupProjection, [FilterProjection], [TopN] )
     * LocalMergeNode ( [TopN], IndexWriterProjection )
     */
    public static AnalyzedRelation optimizedReduceOnCollectorGroupBy(QueriedTable table, TableRelation tableRelation, WhereClause whereClause, ColumnIndexWriterProjection indexWriterProjection) {
        assert GroupByConsumer.groupedByClusteredColumnOrPrimaryKeys(tableRelation, table.querySpec().groupBy()) : "not grouped by clustered column or primary keys";
        TableInfo tableInfo = tableRelation.tableInfo();
        GroupByConsumer.validateGroupBySymbols(tableRelation, table.querySpec().groupBy());
        List<Symbol> groupBy = table.querySpec().groupBy();

        boolean ignoreSorting = indexWriterProjection != null
                && table.querySpec().limit() == null
                && table.querySpec().offset() == TopN.NO_OFFSET;

        ProjectionBuilder projectionBuilder = new ProjectionBuilder(table.querySpec());
        SplitPoints splitPoints = projectionBuilder.getSplitPoints();

        // mapper / collect
        List<Symbol> collectOutputs = new ArrayList<>(
                groupBy.size() +
                splitPoints.aggregates().size());
        collectOutputs.addAll(groupBy);
        collectOutputs.addAll(splitPoints.aggregates());

        OrderBy orderBy = table.querySpec().orderBy();
        if (orderBy != null) {
            table.tableRelation().validateOrderBy(orderBy);
        }

        List<Projection> projections = new ArrayList<>();
        GroupProjection groupProjection = projectionBuilder.groupProjection(
                splitPoints.leaves(),
                table.querySpec().groupBy(),
                splitPoints.aggregates(),
                Aggregation.Step.ITER,
                Aggregation.Step.FINAL
        );
        groupProjection.setRequiredGranularity(RowGranularity.SHARD);
        projections.add(groupProjection);

        HavingClause havingClause = table.querySpec().having();
        if(havingClause != null){
            if (havingClause.noMatch()) {
                return new NoopPlannedAnalyzedRelation(table);
            } else if (havingClause.hasQuery()){
                FilterProjection fp = projectionBuilder.filterProjection(
                        collectOutputs,
                        havingClause.query()
                );
                fp.requiredGranularity(RowGranularity.SHARD);
                projections.add(fp);
            }
        }
        // mapper / collect
        // use topN on collector if needed
        boolean outputsMatch = table.querySpec().outputs().size() == collectOutputs.size() &&
                collectOutputs.containsAll(table.querySpec().outputs());
        boolean collectorTopN = table.querySpec().limit() != null || table.querySpec().offset() > 0 || !outputsMatch;

        if(collectorTopN) {
           projections.add(projectionBuilder.topNProjection(
                    collectOutputs,
                    orderBy,
                    0, // no offset
                    firstNonNull(table.querySpec().limit(), Constants.DEFAULT_SELECT_LIMIT) + table.querySpec().offset(),
                    table.querySpec().outputs()
           ));
        }

        CollectNode collectNode = PlanNodeBuilder.collect(
                tableInfo,
                table.querySpec().where(),
                splitPoints.leaves(),
                ImmutableList.copyOf(projections)
        );
        // handler
        List<Projection> handlerProjections = new ArrayList<>();
        if (!ignoreSorting) {
            List<Symbol> inputs;
            if(collectorTopN){
                inputs = table.querySpec().outputs();
            } else {
                inputs = collectOutputs;
            }
            handlerProjections.add(projectionBuilder.topNProjection(
                    inputs,
                    orderBy,
                    table.querySpec().offset(),
                    firstNonNull(table.querySpec().limit(), Constants.DEFAULT_SELECT_LIMIT),
                    table.querySpec().outputs()
            ));
        }
        if (indexWriterProjection != null) {
            handlerProjections.add(indexWriterProjection);
        }
        MergeNode localMergeNode = PlanNodeBuilder.localMerge(handlerProjections, collectNode);
        return new NonDistributedGroupBy(collectNode, localMergeNode);
    }
}
