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

package io.crate.planner;

import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import io.crate.Constants;
import io.crate.PartitionName;
import io.crate.analyze.AbstractDataAnalysis;
import io.crate.analyze.SelectAnalysis;
import io.crate.analyze.where.WhereClause;
import io.crate.metadata.DocReferenceBuildingVisitor;
import io.crate.metadata.Routing;
import io.crate.metadata.relation.AnalyzedQuerySpecification;
import io.crate.metadata.relation.TableRelation;
import io.crate.metadata.table.TableInfo;
import io.crate.operation.projectors.TopN;
import io.crate.planner.node.dql.DQLPlanNode;
import io.crate.planner.node.dql.MergeNode;
import io.crate.planner.node.dql.QueryAndFetchNode;
import io.crate.planner.node.dql.QueryThenFetchNode;
import io.crate.planner.projection.*;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;
import io.crate.planner.symbol.SymbolType;

import javax.annotation.Nullable;
import java.util.*;

class PlanNodeBuilder {


    private static final Predicate<Object> IS_REFERENCE = Predicates.instanceOf(Reference.class);
    private static final PlannerFunctionArgumentCopier functionArgumentCopier = new PlannerFunctionArgumentCopier();

    static QueryAndFetchNode distributingCollect(AbstractDataAnalysis analysis,
                                                 List<Symbol> toCollect,
                                                 List<String> downstreamNodes,
                                                 ImmutableList<Projection> projections) {
        Routing routing = null;
        boolean isPartitioned = false;
        WhereClause whereClause;
        if (analysis instanceof SelectAnalysis) {
            AnalyzedQuerySpecification querySpec = ((SelectAnalysis) analysis).querySpecification();
            TableInfo tableInfo = ((TableRelation) querySpec.sourceRelation()).tableInfo();
            routing = tableInfo.getRouting(querySpec.whereClause());
            whereClause = querySpec.whereClause();
            isPartitioned = tableInfo.isPartitioned();
        } else {
            whereClause = analysis.whereClause();
            routing = analysis.table().getRouting(whereClause);
            isPartitioned = analysis.table().isPartitioned();
        }
        QueryAndFetchNode node = new QueryAndFetchNode("distributing collect",
                routing,
                toCollect,
                ImmutableList.<Symbol>of(),
                null, null, null, null, null, null,
                projections,
                whereClause,
                analysis.rowGranularity(),
                isPartitioned);
        node.downStreamNodes(downstreamNodes);
        node.configure();
        return node;
    }

    static MergeNode distributedMerge(QueryAndFetchNode queryAndFetchNode,
                                      ImmutableList<Projection> projections) {
        MergeNode node = new MergeNode("distributed merge", queryAndFetchNode.executionNodes().size());
        node.projections(projections);

        assert queryAndFetchNode.downStreamNodes()!=null && queryAndFetchNode.downStreamNodes().size()>0;
        node.executionNodes(ImmutableSet.copyOf(queryAndFetchNode.downStreamNodes()));
        connectTypes(queryAndFetchNode, node);
        return node;
    }

    static MergeNode localMerge(List<Projection> projections,
                                DQLPlanNode previousNode) {
        MergeNode node = new MergeNode("localMerge", previousNode.executionNodes().size());
        node.projections(projections);
        connectTypes(previousNode, node);
        return node;
    }

    /**
     * calculates the outputTypes using the projections and input types.
     * must be called after projections have been set.
     */
    static void setOutputTypes(QueryAndFetchNode node) {
        if (node.projections().isEmpty() && node.collectorProjections().isEmpty()) {
            node.outputTypes(Planner.extractDataTypes(node.toCollect()));
        } else if (node.projections().isEmpty()) {
            node.outputTypes(Planner.extractDataTypes(node.collectorProjections(), Planner.extractDataTypes(node.toCollect())));
        } else {
            node.outputTypes(Planner.extractDataTypes(node.projections(), Planner.extractDataTypes(node.toCollect())));
        }
    }

    /**
     * sets the inputTypes from the previousNode's outputTypes
     * and calculates the outputTypes using the projections and input types.
     * <p/>
     * must be called after projections have been set
     */
    static void connectTypes(DQLPlanNode previousNode, DQLPlanNode nextNode) {
        nextNode.inputTypes(previousNode.outputTypes());
        nextNode.outputTypes(Planner.extractDataTypes(nextNode.projections(), nextNode.inputTypes()));
    }

    /**
     * create a new QueryAndFetchNode from the given analysis and other information.
     *
     * @param analysis the query analysis
     * @param toCollect the symbols to collect
     * @param collectorProjections the projections that process the collected results
     * @param projections projections executed during merge
     * @param partitionIdent if not null, this query is routed to this partition
     * @return a QueryAndFetchNode, configured and ready to be added to a plan
     */
    static QueryAndFetchNode queryAndFetch(AbstractDataAnalysis analysis,
                                           List<Symbol> toCollect,
                                           ImmutableList<Projection> collectorProjections,
                                           @Nullable List<Projection> projections,
                                           @Nullable String partitionIdent) {
        WhereClause whereClause;
        TableInfo tableInfo;

        if (analysis instanceof SelectAnalysis) {
            AnalyzedQuerySpecification querySpecification = ((SelectAnalysis) analysis).querySpecification();
            whereClause = querySpecification.whereClause();
            tableInfo = ((TableRelation) querySpecification.sourceRelation()).tableInfo();
        } else {
            tableInfo = analysis.table();
            whereClause = analysis.whereClause();
        }

        Routing routing = tableInfo.getRouting(whereClause);
        if (partitionIdent != null && routing.hasLocations()) {
            routing = filterRouting(routing, PartitionName.fromPartitionIdent(
                    tableInfo.ident().name(), partitionIdent).stringValue());
        }
        QueryAndFetchNode node = new QueryAndFetchNode("collect",
                routing,
                toCollect,
                ImmutableList.<Symbol>of(),
                null,
                null,
                null,
                null,
                null,
                collectorProjections,
                projections,
                whereClause,
                analysis.rowGranularity(),
                tableInfo.isPartitioned());
        node.configure();
        return node;
    }

    public static QueryThenFetchNode queryThenFetch(AnalyzedQuerySpecification querySpec,
                                                    TableInfo tableInfo,
                                                    Optional<ColumnIndexWriterProjection> writerProjection) {
        PlannerContextBuilder contextBuilder = new PlannerContextBuilder();
        boolean needsProjection = !Iterables.all(querySpec.outputs(), IS_REFERENCE)
                || writerProjection.isPresent();
        ImmutableList.Builder<Projection> projectionBuilder = ImmutableList.builder();

        List<Symbol> searchSymbols;

        WhereClause whereClause = querySpec.whereClause();
        if (needsProjection) {
            // we must create a deep copy of references if they are function arguments
            // or they will be replaced with InputColumn instances by the context builder
            if (whereClause.hasQuery()) {
                whereClause = new WhereClause(functionArgumentCopier.process(whereClause.query()));
            }
            List<Symbol> sortSymbols = querySpec.orderBy();

            // do the same for sortsymbols if we have a function there
            if (sortSymbols != null && !Iterables.all(sortSymbols, IS_REFERENCE)) {
                functionArgumentCopier.process(sortSymbols);
            }

            contextBuilder.searchOutput(querySpec.outputs());
            searchSymbols = contextBuilder.toCollect();

            TopNProjection topN = new TopNProjection(
                    Objects.firstNonNull(querySpec.limit(), Constants.DEFAULT_SELECT_LIMIT),
                    TopN.NO_OFFSET
            );
            topN.outputs(contextBuilder.outputs());
            projectionBuilder.add(topN);
            if (writerProjection.isPresent()) {
                projectionBuilder.add(writerProjection.get());
            }
        } else {
            searchSymbols = querySpec.outputs();
        }
        QueryThenFetchNode node = new QueryThenFetchNode(
                tableInfo.getRouting(whereClause),
                searchSymbols,
                querySpec.orderBy(),
                querySpec.reverseFlags(),
                querySpec.nullsFirst(),
                querySpec.limit(),
                querySpec.offset(),
                whereClause,
                tableInfo.partitionedByColumns(),
                projectionBuilder.build()
        );
        node.configure();
        return node;

    }

    /**
     * create a {@linkplain io.crate.planner.node.dql.QueryAndFetchNode} for use
     * in a collect context e.g. when selecting from information_schema
     *
     * TODO: keep the writerProjection and sumUpResultsProjection out
     */
    public static QueryAndFetchNode collect(AnalyzedQuerySpecification querySpec,
                                            TableInfo tableInfo,
                                            Optional<ColumnIndexWriterProjection> writerProjection,
                                            AggregationProjection sumUpResultsProjection) {
        ImmutableList.Builder<Projection> collectorProjections = ImmutableList.builder();
        ImmutableList.Builder<Projection> projections = ImmutableList.builder();
        PlannerContextBuilder contextBuilder = new PlannerContextBuilder()
                .output(querySpec.outputs())
                .orderBy(querySpec.orderBy());

        List<Symbol> outputs = contextBuilder.outputs();


        List<Symbol> toCollect;
        if (tableInfo.schemaInfo().systemSchema()) {
            toCollect = contextBuilder.toCollect();
        } else {
            toCollect = new ArrayList<>();
            for (Symbol symbol : contextBuilder.toCollect()) {
                toCollect.add(DocReferenceBuildingVisitor.convert(symbol));
            }
        }

        boolean isLimited = querySpec.limit() != null || querySpec.offset() > 0;
        int effectiveLimit = Objects.firstNonNull(querySpec.limit(), Constants.DEFAULT_SELECT_LIMIT);
        if (isLimited) {
            // apply limit on collector
            TopNProjection tnp = new TopNProjection(
                    querySpec.offset() + effectiveLimit,
                    0,
                    contextBuilder.orderBy(),
                    querySpec.reverseFlags(),
                    querySpec.nullsFirst()
            );
            tnp.outputs(outputs);
            collectorProjections.add(tnp);
        }

        if (!writerProjection.isPresent() || isLimited) {
            // limit set, apply topN projection
            TopNProjection tnp = new TopNProjection(
                    effectiveLimit,
                    querySpec.offset(),
                    contextBuilder.orderBy(),
                    querySpec.reverseFlags(),
                    querySpec.nullsFirst()
            );
            tnp.outputs(outputs);
            projections.add(tnp);
        }

        if (writerProjection.isPresent()) {
            if (isLimited) {
                projections.add(writerProjection.get());
            } else {
                collectorProjections.add(writerProjection.get());
                projections.add(sumUpResultsProjection);
            }
        }

        Routing routing = tableInfo.getRouting(querySpec.whereClause());
        QueryAndFetchNode node = new QueryAndFetchNode("collect",
                routing,
                toCollect,
                contextBuilder.outputs(),
                contextBuilder.orderBy(),
                querySpec.reverseFlags(),
                querySpec.nullsFirst(),
                querySpec.limit(),
                querySpec.offset(),
                collectorProjections.build(),
                projections.build(),
                querySpec.whereClause(),
                tableInfo.rowGranularity(),
                tableInfo.isPartitioned()
        );
        node.configure();
        return node;
    }

    /**
     * create {@linkplain io.crate.planner.node.dql.QueryAndFetchNode} for use
     * in an aggregation context, e.g. a global aggregate query
     *
     */
    public static QueryAndFetchNode collectAggregate(AnalyzedQuerySpecification querySpec,
                                                     TableInfo tableInfo) {
        ImmutableList.Builder<Projection> collectorProjections = ImmutableList.builder();
        ImmutableList.Builder<Projection> projections = ImmutableList.builder();

        // global aggregate: queryAndFetch and partial aggregate on C and final agg on H
        PlannerContextBuilder contextBuilder = new PlannerContextBuilder(2)
                .output(querySpec.outputs());

        Symbol havingClause = null;
        if (querySpec.having().isPresent() && querySpec.having().get().symbolType() == SymbolType.FUNCTION) {
            // replace aggregation symbols with input columns from previous projection
            havingClause = contextBuilder.having(querySpec.having().get());
        }

        AggregationProjection ap = new AggregationProjection();
        ap.aggregations(contextBuilder.aggregations());
        collectorProjections.add(ap);

        contextBuilder.nextStep();

        projections.add(new AggregationProjection(contextBuilder.aggregations()));

        // havingClause could be a Literal or Function.
        // if its a Literal and value is false, we'll never reach this point (no match),
        // otherwise (true value) having can be ignored
        if (havingClause != null) {
            FilterProjection fp = new FilterProjection((Function)havingClause);
            fp.outputs(contextBuilder.passThroughOutputs());
            projections.add(fp);
        }
        if (contextBuilder.aggregationsWrappedInScalar || havingClause != null) {
            // will filter out optional having symbols which are not selected
            TopNProjection topNProjection = new TopNProjection(1, 0);
            topNProjection.outputs(contextBuilder.outputs());
            projections.add(topNProjection);
        }
        Routing routing = tableInfo.getRouting(querySpec.whereClause());
        QueryAndFetchNode queryAndFetchNode = new QueryAndFetchNode("collect_aggregate",
                routing,
                contextBuilder.toCollect(),
                contextBuilder.outputs(),
                querySpec.orderBy(),
                querySpec.reverseFlags(),
                querySpec.nullsFirst(),
                querySpec.limit(),
                querySpec.offset(),
                collectorProjections.build(),
                projections.build(),
                querySpec.whereClause(),
                tableInfo.rowGranularity(),
                tableInfo.isPartitioned()
                );
        queryAndFetchNode.configure();
        return queryAndFetchNode;
    }

    private static Routing filterRouting(Routing routing, String includeTableName) {
        assert routing.hasLocations();
        assert includeTableName != null;
        Map<String, Map<String, Set<Integer>>> newLocations = new HashMap<>();

        for (Map.Entry<String, Map<String, Set<Integer>>> entry : routing.locations().entrySet()) {
            Map<String, Set<Integer>> tableMap = new HashMap<>();
            for (Map.Entry<String, Set<Integer>> tableEntry : entry.getValue().entrySet()) {
                if (includeTableName.equals(tableEntry.getKey())) {
                    tableMap.put(tableEntry.getKey(), tableEntry.getValue());
                }
            }
            if (tableMap.size()>0){
                newLocations.put(entry.getKey(), tableMap);
            }

        }
        if (newLocations.size()>0) {
            return new Routing(newLocations);

        } else {
            return new Routing();
        }
    }

}
