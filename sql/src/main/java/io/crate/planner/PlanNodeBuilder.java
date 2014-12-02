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

import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import io.crate.PartitionName;
import io.crate.analyze.WhereClause;
import io.crate.metadata.Routing;
import io.crate.metadata.table.TableInfo;
import io.crate.planner.node.dql.CollectNode;
import io.crate.planner.node.dql.DQLPlanNode;
import io.crate.planner.node.dql.MergeNode;
import io.crate.planner.projection.Projection;
import io.crate.planner.symbol.InputColumn;
import io.crate.planner.symbol.Symbol;
import io.crate.planner.symbol.Symbols;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

class PlanNodeBuilder {

    static CollectNode distributingCollect(TableInfo tableInfo,
                                           WhereClause whereClause,
                                           List<Symbol> toCollect,
                                           List<String> downstreamNodes,
                                           ImmutableList<Projection> projections) {
        CollectNode node = new CollectNode("distributing collect", tableInfo.getRouting(whereClause));
        node.whereClause(whereClause);
        node.maxRowGranularity(tableInfo.rowGranularity());
        node.downStreamNodes(downstreamNodes);
        node.toCollect(toCollect);
        node.projections(projections);

        node.isPartitioned(tableInfo.isPartitioned());
        setOutputTypes(node);
        return node;
    }

    static MergeNode distributedMerge(CollectNode collectNode,
                                      ImmutableList<Projection> projections) {
        MergeNode node = new MergeNode("distributed merge", collectNode.executionNodes().size());
        node.projections(projections);

        assert collectNode.downStreamNodes()!=null && collectNode.downStreamNodes().size()>0;
        node.executionNodes(ImmutableSet.copyOf(collectNode.downStreamNodes()));
        connectTypes(collectNode, node);
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
    static void setOutputTypes(CollectNode node) {
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
    static void connectTypes(DQLPlanNode previousNode, DQLPlanNode nextNode) {
        nextNode.inputTypes(previousNode.outputTypes());
        nextNode.outputTypes(Planner.extractDataTypes(nextNode.projections(), nextNode.inputTypes()));
    }

    static CollectNode collect(TableInfo tableInfo,
                               WhereClause whereClause,
                               List<Symbol> toCollect,
                               ImmutableList<Projection> projections,
                               @Nullable String partitionIdent) {
        assert !Iterables.any(toCollect, Predicates.instanceOf(InputColumn.class)) : "cannot collect inputcolumns";
        Routing routing = tableInfo.getRouting(whereClause);
        if (partitionIdent != null && routing.hasLocations()) {
            routing = filterRouting(routing, PartitionName.fromPartitionIdent(
                    tableInfo.ident().name(), partitionIdent).stringValue());
        }
        CollectNode node = new CollectNode("collect", routing);
        node.whereClause(whereClause);
        node.toCollect(toCollect);
        node.maxRowGranularity(tableInfo.rowGranularity());
        node.projections(projections);
        node.isPartitioned(tableInfo.isPartitioned());
        setOutputTypes(node);
        return node;
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

    static CollectNode collect(TableInfo tableInfo,
                               WhereClause whereClause,
                               List<Symbol> toCollect,
                               ImmutableList<Projection> projections) {
        return collect(tableInfo, whereClause, toCollect, projections, null);
    }
}
