/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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
import io.crate.analyze.SelectAnalyzedStatement;
import io.crate.analyze.WhereClause;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.where.WhereClauseContext;
import io.crate.metadata.DocReferenceConverter;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.Functions;
import io.crate.metadata.Routing;
import io.crate.metadata.table.TableInfo;
import io.crate.operation.aggregation.impl.SumAggregation;
import io.crate.operation.projectors.TopN;
import io.crate.planner.PlanNodeBuilder;
import io.crate.planner.PlannerContextBuilder;
import io.crate.planner.RowGranularity;
import io.crate.planner.node.dml.QueryAndFetchNode;
import io.crate.planner.node.dql.CollectNode;
import io.crate.planner.node.dql.DistributedGroupByNode;
import io.crate.planner.node.dql.GroupByConsumer;
import io.crate.planner.node.dql.MergeNode;
import io.crate.planner.projection.*;
import io.crate.planner.symbol.*;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.LongType;
import org.elasticsearch.common.Nullable;

import java.util.*;

import static com.google.common.base.MoreObjects.firstNonNull;
import static io.crate.planner.symbol.Field.unwrap;

/**
 * Utils class which contains methods to create PlanNodes which are used by consumers
 * and the legacy Planner. This class should only exists during rebuilding the planner
 * to the new consumer pattern and should be DELETED after the refactoring
 * is done.
 */
public class ConsumerUtils {

    public static AnalyzedRelation normalSelect(SelectAnalyzedStatement statement, WhereClauseContext whereClauseContext,
                                          TableInfo tableInfo, ColumnIndexWriterProjection indexWriterProjection, Functions functions){
        WhereClause whereClause = whereClauseContext.whereClause();
        PlannerContextBuilder contextBuilder = new PlannerContextBuilder()
                .output(unwrap(statement.outputSymbols()))
                .orderBy(unwrap(statement.orderBy().orderBySymbols()));
        ImmutableList<Projection> projections;
        if (statement.isLimited()) {
            // if we have an offset we have to get as much docs from every node as we have offset+limit
            // otherwise results will be wrong
            TopNProjection tnp = new TopNProjection(
                    statement.offset() + statement.limit(),
                    0,
                    contextBuilder.orderBy(),
                    statement.orderBy().reverseFlags(),
                    statement.orderBy().nullsFirst()
            );
            tnp.outputs(contextBuilder.outputs());
            projections = ImmutableList.<Projection>of(tnp);
        } else if(indexWriterProjection != null) {
            // no limit, projection (index writer) will run on shard/CollectNode
            projections = ImmutableList.<Projection>of(indexWriterProjection);
        } else {
            projections = ImmutableList.of();
        }

        List<Symbol> toCollect;
        if (tableInfo.schemaInfo().systemSchema()) {
            toCollect = contextBuilder.toCollect();
        } else {
            toCollect = new ArrayList<>();
            for (Symbol symbol : contextBuilder.toCollect()) {
                toCollect.add(DocReferenceConverter.convertIfPossible(symbol, tableInfo));
            }
        }

        CollectNode collectNode = PlanNodeBuilder.collect(tableInfo, whereClause, toCollect, projections);
        ImmutableList.Builder<Projection> projectionBuilder = ImmutableList.builder();

        if (indexWriterProjection == null || statement.isLimited()) {
            // limit set, apply topN projection
            TopNProjection tnp = new TopNProjection(
                    firstNonNull(statement.limit(), Constants.DEFAULT_SELECT_LIMIT),
                    statement.offset(),
                    contextBuilder.orderBy(),
                    statement.orderBy().reverseFlags(),
                    statement.orderBy().nullsFirst()
            );
            tnp.outputs(contextBuilder.outputs());
            projectionBuilder.add(tnp);
        }
        if (indexWriterProjection != null && statement.isLimited()) {
            // limit set, context projection (index writer) will run on handler
            projectionBuilder.add(indexWriterProjection);
        } else if (indexWriterProjection != null && !statement.isLimited()) {
            // no limit -> no topN projection, use aggregation projection to merge node results
            projectionBuilder.add(localMergeProjection(functions));
        }
        MergeNode localMergeNode = PlanNodeBuilder.localMerge(projectionBuilder.build(),collectNode);
        return new QueryAndFetchNode(
                collectNode,
                localMergeNode
        );
    }

    public static AnalyzedRelation groupBy(SelectAnalyzedStatement statement, TableInfo tableInfo, WhereClauseContext whereClauseContext,
                                           @Nullable ColumnIndexWriterProjection indexWriterProjection, @Nullable Functions functions){
        if (tableInfo.schemaInfo().systemSchema() || !requiresDistribution(statement, tableInfo)) {
            return nonDistributedGroupBy(statement, tableInfo, whereClauseContext, indexWriterProjection);
        } else if (groupedByClusteredColumnOrPrimaryKeys(statement, tableInfo)) {
            return optimizedReduceOnCollectorGroupBy(statement, tableInfo, whereClauseContext, indexWriterProjection);
        } else if (indexWriterProjection != null) {
            return distributedWriterGroupBy(statement, tableInfo, whereClauseContext, indexWriterProjection, functions);
        } else {
            assert false : "this case should have been handled in the ConsumingPlanner";
        }
        return null;
    }

    private static boolean requiresDistribution(SelectAnalyzedStatement analysis, TableInfo tableInfo) {
        Routing routing = tableInfo.getRouting(analysis.whereClause());
        if (!routing.hasLocations()) return false;
        if (routing.locations().size() > 1) return true;
        Map<String, Map<String, Set<Integer>>> locations = routing.locations();
        if (locations != null && locations.size() > 1) {
            return true;
        }
        return false;
    }

    private static boolean groupedByClusteredColumnOrPrimaryKeys(SelectAnalyzedStatement analysis, TableInfo tableInfo) {
        List<Symbol> groupBy = unwrap(analysis.groupBy());
        assert groupBy != null;
        return GroupByConsumer.groupedByClusteredColumnOrPrimaryKeys(tableInfo, groupBy);
    }

    private static AnalyzedRelation nonDistributedGroupBy(SelectAnalyzedStatement analysis,
                                       TableInfo tableInfo,
                                       WhereClauseContext whereClauseContext,
                                       ColumnIndexWriterProjection indexWriterProjection) {
        boolean ignoreSorting = indexWriterProjection != null
                && analysis.limit() == null
                && analysis.offset() == TopN.NO_OFFSET;


        List<Symbol> groupBy = unwrap(analysis.groupBy());
        int numAggregationSteps = 2;

        PlannerContextBuilder contextBuilder =
                new PlannerContextBuilder(numAggregationSteps, groupBy, ignoreSorting)
                        .output(unwrap(analysis.outputSymbols()))
                        .orderBy(unwrap(analysis.orderBy().orderBySymbols()));

        Symbol havingClause = null;
        Symbol having = unwrap(analysis.havingClause());
        if (having != null && having.symbolType() == SymbolType.FUNCTION) {
            // extract collect symbols and such from having clause
            havingClause = contextBuilder.having(having);
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
            TopNProjection topN = new TopNProjection(
                    firstNonNull(analysis.limit(), Constants.DEFAULT_SELECT_LIMIT),
                    analysis.offset(),
                    contextBuilder.orderBy(),
                    analysis.orderBy().reverseFlags(),
                    analysis.orderBy().nullsFirst()
            );
            topN.outputs(contextBuilder.outputs());
            contextBuilder.addProjection(topN);
        }
        if (indexWriterProjection != null) {
            contextBuilder.addProjection(indexWriterProjection);
        }
        MergeNode localMergeNode = PlanNodeBuilder.localMerge(contextBuilder.getAndClearProjections(), collectNode);
        return new QueryAndFetchNode(collectNode, localMergeNode);
    }

    /**
     * grouping on doc tables by clustered column or primary keys, no distribution needed
     * only one aggregation step as the mappers (shards) have row-authority
     *
     * produces:
     *
     * SELECT:
     *  CollectNode ( GroupProjection, [FilterProjection], [TopN] )
     *  LocalMergeNode ( TopN )
     *
     * INSERT FROM QUERY:
     *  CollectNode ( GroupProjection, [FilterProjection], [TopN] )
     *  LocalMergeNode ( [TopN], IndexWriterProjection )
     */
    public static AnalyzedRelation optimizedReduceOnCollectorGroupBy(SelectAnalyzedStatement analysis, TableInfo tableInfo, WhereClauseContext whereClauseContext, ColumnIndexWriterProjection indexWriterProjection) {
        assert groupedByClusteredColumnOrPrimaryKeys(analysis, tableInfo) : "not grouped by clustered column or primary keys";
        boolean ignoreSorting = indexWriterProjection != null
                && analysis.limit() == null
                && analysis.offset() == TopN.NO_OFFSET;
        int numAggregationSteps = 1;
        PlannerContextBuilder contextBuilder =
                new PlannerContextBuilder(numAggregationSteps, unwrap(analysis.groupBy()), ignoreSorting)
                        .output(unwrap(analysis.outputSymbols()))
                        .orderBy(unwrap(analysis.orderBy().orderBySymbols()));
        Symbol havingClause = unwrap(analysis.havingClause());
        if (havingClause != null && havingClause.symbolType() == SymbolType.FUNCTION) {
            // replace aggregation symbols with input columns from previous projection
            havingClause = contextBuilder.having(havingClause);
        }

        // mapper / collect
        List<Symbol> toCollect = contextBuilder.toCollect();

        // grouping
        GroupProjection groupProjection =
                new GroupProjection(contextBuilder.groupBy(), contextBuilder.aggregations());
        groupProjection.setRequiredGranularity(RowGranularity.SHARD);
        contextBuilder.addProjection(groupProjection);

        // optional having
        if (havingClause != null) {
            FilterProjection fp = new FilterProjection((Function)havingClause);
            fp.outputs(contextBuilder.genInputColumns(groupProjection.outputs(), groupProjection.outputs().size()));
            fp.requiredGranularity(RowGranularity.SHARD); // running on every shard
            contextBuilder.addProjection(fp);
        }

        // use topN on collector if needed
        TopNProjection topNReducer = getTopNForReducer(
                analysis,
                contextBuilder,
                contextBuilder.outputs());
        if (topNReducer != null) {
            contextBuilder.addProjection(topNReducer);
        }

        CollectNode collectNode = PlanNodeBuilder.collect(
                tableInfo,
                whereClauseContext.whereClause(),
                toCollect,
                contextBuilder.getAndClearProjections()
        );
        // handler

        if (!ignoreSorting) {
            List<Symbol> orderBy;
            List<Symbol> outputs;
            if (topNReducer == null) {
                orderBy = contextBuilder.orderBy();
                outputs = contextBuilder.outputs();
            } else {
                orderBy = contextBuilder.passThroughOrderBy();
                outputs = contextBuilder.passThroughOutputs();
            }

            TopNProjection topN = new TopNProjection(
                    firstNonNull(analysis.limit(), Constants.DEFAULT_SELECT_LIMIT),
                    analysis.offset(),
                    orderBy,
                    analysis.orderBy().reverseFlags(),
                    analysis.orderBy().nullsFirst()
            );
            topN.outputs(outputs);
            contextBuilder.addProjection(topN);
        }
        if (indexWriterProjection != null) {
            contextBuilder.addProjection(indexWriterProjection);
        }
        MergeNode localMergeNode = PlanNodeBuilder.localMerge(contextBuilder.getAndClearProjections(), collectNode);
        return new QueryAndFetchNode(collectNode, localMergeNode);
    }

    /**
     * distributed collect on mapper nodes
     * with merge on reducer to final (they have row authority) and index write
     * if no limit and not offset is set
     * <p/>
     * final merge + index write on handler if limit or offset is set
     */
    private static AnalyzedRelation distributedWriterGroupBy(SelectAnalyzedStatement analysis,
                                          TableInfo tableInfo,
                                          WhereClauseContext whereClauseContext,
                                          Projection writerProjection,
                                          Functions functions) {
        boolean ignoreSorting = !analysis.isLimited();
        PlannerContextBuilder contextBuilder = new PlannerContextBuilder(2, unwrap(analysis.groupBy()), ignoreSorting)
                .output(unwrap(analysis.outputSymbols()))
                .orderBy(unwrap(analysis.orderBy().orderBySymbols()));

        Symbol havingClause = unwrap(analysis.havingClause());
        if (havingClause != null && havingClause.symbolType() == SymbolType.FUNCTION) {
            // replace aggregation symbols with input columns from previous projection
            havingClause = contextBuilder.having(havingClause);
        }

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
                    unwrap(analysis.orderBy().orderBySymbols()),
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
            contextBuilder.addProjection(localMergeProjection(functions));
        }
        MergeNode localMergeNode = PlanNodeBuilder.localMerge(contextBuilder.getAndClearProjections(), mergeNode);
        return new DistributedGroupByNode(
                collectNode,
                mergeNode,
                localMergeNode
        );
    }

    private static AggregationProjection localMergeProjection(Functions functions) {
        return new AggregationProjection(
                    Arrays.asList(new Aggregation(
                            functions.getSafe(
                                    new FunctionIdent(SumAggregation.NAME, Arrays.<DataType>asList(LongType.INSTANCE))
                            ).info(),
                            Arrays.<Symbol>asList(new InputColumn(0, DataTypes.LONG)),
                            Aggregation.Step.ITER,
                            Aggregation.Step.FINAL
                    )
                    )
            );
    }

    /**
     * returns a topNProjection intended for the reducer in a group by query.
     *
     * result will be null if topN on reducer is not needed or possible.
     *
     * the limit given to the topN projection will be limit + offset because there will be another
     * @param outputs list of outputs to add to the topNProjection if applicable.
     */
    @javax.annotation.Nullable
    private static TopNProjection getTopNForReducer(SelectAnalyzedStatement analysis,
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

    private static boolean requireLimitOnReducer(SelectAnalyzedStatement analysis, boolean aggregationsWrappedInScalar) {
        return (analysis.limit() != null
                || analysis.offset() > 0
                || aggregationsWrappedInScalar);
    }
}
