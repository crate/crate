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

import com.google.common.base.MoreObjects;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import io.crate.analyze.OrderBy;
import io.crate.analyze.WhereClause;
import io.crate.metadata.PartitionName;
import io.crate.metadata.Routing;
import io.crate.metadata.table.TableInfo;
import io.crate.planner.consumer.OrderByPositionVisitor;
import io.crate.planner.node.dql.CollectNode;
import io.crate.planner.node.dql.DQLPlanNode;
import io.crate.planner.node.dql.MergeNode;
import io.crate.planner.projection.Projection;
import io.crate.planner.symbol.InputColumn;
import io.crate.planner.symbol.Symbol;
import io.crate.planner.symbol.Symbols;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.HashMap;
import java.util.Set;

public class PlanNodeBuilder {

    public static CollectNode distributingCollect(TableInfo tableInfo,
                                                  Planner.Context plannerContext,
                                                  WhereClause whereClause,
                                                  List<Symbol> toCollect,
                                                  List<String> downstreamNodes,
                                                  ImmutableList<Projection> projections) {
        Routing routing = tableInfo.getRouting(whereClause, null);
        plannerContext.allocateJobSearchContextIds(routing);
        CollectNode node = new CollectNode("distributing collect", routing);
        node.whereClause(whereClause);
        node.maxRowGranularity(tableInfo.rowGranularity());
        node.downstreamNodes(downstreamNodes);
        node.toCollect(toCollect);
        node.projections(projections);

        node.isPartitioned(tableInfo.isPartitioned());
        setOutputTypes(node);
        return node;
    }

    public static MergeNode distributedMerge(CollectNode collectNode,
                                      List<Projection> projections) {
        MergeNode node = new MergeNode("distributed merge", collectNode.executionNodes().size());
        node.projections(projections);

        assert collectNode.hasDownstreams();
        node.executionNodes(ImmutableSet.copyOf(collectNode.downstreamNodes()));
        connectTypes(collectNode, node);
        return node;
    }

    public static MergeNode localMerge(List<Projection> projections,
                                       DQLPlanNode previousNode,
                                       Planner.Context plannerContext) {
        MergeNode node = new MergeNode("localMerge", previousNode.executionNodes().size());
        node.jobSearchContextIdToNode(plannerContext.jobSearchContextIdToNode());
        node.jobSearchContextIdToShard(plannerContext.jobSearchContextIdToShard());
        node.projections(projections);
        connectTypes(previousNode, node);
        return node;
    }

    /**
     * Create a MergeNode which uses a {@link io.crate.operation.merge.SortingBucketMerger}
     * as it expects sorted input and produces sorted output.
     *
     * @param projections the projections to include in the resulting MergeNode
     * @param orderBy {@linkplain io.crate.analyze.OrderBy} containing sorting parameters
     * @param sourceSymbols the input symbols for this mergeNode
     * @param orderBySymbols the symbols to sort on. If this is null,
     *                       {@linkplain io.crate.analyze.OrderBy#orderBySymbols()}
     *                       will be used
     * @param previousNode the previous planNode to derive inputtypes from
     */
    public static MergeNode sortedLocalMerge(List<Projection> projections,
                                             OrderBy orderBy,
                                             List<Symbol> sourceSymbols,
                                             @Nullable List<Symbol> orderBySymbols,
                                             DQLPlanNode previousNode,
                                             Planner.Context plannerContext) {
        int[] orderByIndices = OrderByPositionVisitor.orderByPositions(
                MoreObjects.firstNonNull(orderBySymbols, orderBy.orderBySymbols()),
                sourceSymbols
        );
        MergeNode node = MergeNode.sortedMergeNode(
                "sortedLocalMerge",
                previousNode.executionNodes().size(),
                orderByIndices,
                orderBy.reverseFlags(),
                orderBy.nullsFirst()
        );
        node.jobSearchContextIdToNode(plannerContext.jobSearchContextIdToNode());
        node.jobSearchContextIdToShard(plannerContext.jobSearchContextIdToShard());
        node.projections(projections);
        connectTypes(previousNode, node);
        return node;
    }

    /**
     * calculates the outputTypes using the projections and input types.
     * must be called after projections have been set.
     */
    public static void setOutputTypes(CollectNode node) {
        if (node.projections().isEmpty()) {
            node.outputTypes(Symbols.extractTypes(node.toCollect()));
        } else {
            node.outputTypes(Planner.extractDataTypes(node.projections(), Symbols.extractTypes(node.toCollect())));
        }
    }

    /**
     * sets the inputTypes from the previousNode's outputTypes
     * and calculates the outputTypes using the projections and input types.
     * <p/>
     * must be called after projections have been set
     */
    public static void connectTypes(DQLPlanNode previousNode, DQLPlanNode nextNode) {
        nextNode.inputTypes(previousNode.outputTypes());
        nextNode.outputTypes(Planner.extractDataTypes(nextNode.projections(), nextNode.inputTypes()));
    }

    public static CollectNode collect(TableInfo tableInfo,
                                      Planner.Context plannerContext,
                                      WhereClause whereClause,
                                      List<Symbol> toCollect,
                                      ImmutableList<Projection> projections,
                                      @Nullable String partitionIdent,
                                      @Nullable String routingPreference,
                                      @Nullable OrderBy orderBy,
                                      @Nullable Integer limit) {
        assert !Iterables.any(toCollect, Predicates.instanceOf(InputColumn.class)) : "cannot collect inputcolumns";
        Routing routing = tableInfo.getRouting(whereClause, routingPreference);
        if (partitionIdent != null && routing.hasLocations()) {
            routing = filterRouting(routing, PartitionName.fromPartitionIdent(
                    tableInfo.ident().schema(), tableInfo.ident().name(), partitionIdent).stringValue());
        }
        plannerContext.allocateJobSearchContextIds(routing);
        CollectNode node = new CollectNode("collect", routing, toCollect, projections);
        node.whereClause(whereClause);
        node.maxRowGranularity(tableInfo.rowGranularity());
        node.isPartitioned(tableInfo.isPartitioned());
        setOutputTypes(node);
        node.orderBy(orderBy);
        node.limit(limit);
        return node;
    }

    private static Routing filterRouting(Routing routing, String includeTableName) {
        assert routing.hasLocations();
        assert includeTableName != null;
        Map<String, Map<String, List<Integer>>> newLocations = new TreeMap<>();

        for (Map.Entry<String, Map<String, List<Integer>>> entry : routing.locations().entrySet()) {
            Map<String, List<Integer>> tableMap = new TreeMap<>();
            for (Map.Entry<String, List<Integer>> tableEntry : entry.getValue().entrySet()) {
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

    public static CollectNode collect(TableInfo tableInfo,
                                      Planner.Context plannerContext,
                                      WhereClause whereClause,
                                      List<Symbol> toCollect,
                                      ImmutableList<Projection> projections) {
        return collect(tableInfo, plannerContext, whereClause, toCollect, projections, null, null, null, null);
    }

    public static CollectNode collect(TableInfo tableInfo,
                                      Planner.Context plannerContext,
                                      WhereClause whereClause,
                                      List<Symbol> toCollect,
                                      ImmutableList<Projection> projections,
                                      @Nullable String partitionIdent,
                                      @Nullable String routingPreference) {
        return collect(tableInfo, plannerContext, whereClause, toCollect, projections, partitionIdent, routingPreference, null, null);
    }

    public static CollectNode collect(TableInfo tableInfo,
                                      Planner.Context plannerContext,
                                      WhereClause whereClause,
                                      List<Symbol> toCollect,
                                      ImmutableList<Projection> projections,
                                      @Nullable String partitionIdent) {
        return collect(tableInfo, plannerContext, whereClause, toCollect, projections, partitionIdent, null);
    }

    public static CollectNode collect(TableInfo tableInfo,
                                      Planner.Context plannerContext,
                                      WhereClause whereClause,
                                      List<Symbol> toCollect,
                                      ImmutableList<Projection> projections,
                                      @Nullable OrderBy orderBy,
                                      @Nullable Integer limit) {
        return collect(tableInfo, plannerContext, whereClause, toCollect, projections, null, null, orderBy, limit);
    }
}
