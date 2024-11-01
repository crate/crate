/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.ShardRouting;

import com.carrotsearch.hppc.IntIndexedContainer;
import com.carrotsearch.hppc.cursors.IntCursor;

import io.crate.analyze.WhereClause;
import io.crate.metadata.RelationName;
import io.crate.metadata.Routing;
import io.crate.metadata.RoutingProvider;
import io.crate.metadata.settings.CoordinatorSessionSettings;
import io.crate.metadata.table.TableInfo;
import io.crate.planner.fetch.IndexBaseBuilder;

final class RoutingBuilder {

    final Deque<Map<RelationName, List<Routing>>> routingListByTableStack = new ArrayDeque<>();
    private final ClusterState clusterState;
    private final RoutingProvider routingProvider;

    RoutingBuilder(ClusterState clusterState, RoutingProvider routingProvider) {
        this.clusterState = clusterState;
        this.routingProvider = routingProvider;
    }

    /**
     * Create a new context for routing allocations.
     *
     * <p>
     * This ensures all following
     * {@link #allocateRouting(TableInfo, WhereClause, RoutingProvider.ShardSelection, CoordinatorSessionSettings)}
     * calls get stored in a unique context.
     * </p>
     *
     * <p>
     * This context will be used - and discarded - once
     * {@link #buildReaderAllocations()} is called.
     * </p>
     *
     * This is important to ensure that each `Fetch` operation which uses
     * `ReaderAllocations` gets its own isolated set. Consider a tree as follows:
     *
     * <pre>
     *          Root
     *            |
     *          Fetch
     *            |
     *          Join
     *        /     \
     *     Collect   Fetch
     *       t1        |
     *               Limit
     *                 |
     *              Collect
     *                t2
     * </pre>
     *
     * <p>
     * The `Fetch` for t2 must use the routing allocations for t2, and must not
     * intefere with the routing allocations from t1. The routing allocations for t1
     * must remain until the top-level fetch uses it.
     * </p>
     **/
    void newAllocations() {
        routingListByTableStack.add(new HashMap<>());
    }

    Routing allocateRouting(TableInfo tableInfo,
                            WhereClause where,
                            RoutingProvider.ShardSelection shardSelection,
                            CoordinatorSessionSettings sessionSettings) {

        Routing routing = tableInfo.getRouting(clusterState, routingProvider, where, shardSelection, sessionSettings);
        Map<RelationName, List<Routing>> routingListByTable = routingListByTableStack.peekLast();
        if (routingListByTable == null) {
            return routing;
        }
        List<Routing> existingRoutings = routingListByTable.get(tableInfo.ident());
        if (existingRoutings == null) {
            existingRoutings = new ArrayList<>();
            routingListByTable.put(tableInfo.ident(), existingRoutings);
        }
        existingRoutings.add(routing);
        return routing;
    }

    ReaderAllocations buildReaderAllocations() {
        Map<RelationName, Collection<String>> indicesByTable = new HashMap<>();
        IndexBaseBuilder indexBaseBuilder = new IndexBaseBuilder();
        Map<String, Map<Integer, String>> shardNodes = new HashMap<>();

        Map<RelationName, List<Routing>> routingListByTable = routingListByTableStack.removeLast();
        assert routingListByTable != null : "Call to `buildReaderAllocations` without prior `newAllocations` call";
        for (var tableRouting : routingListByTable.entrySet()) {
            RelationName table = tableRouting.getKey();
            List<Routing> routingList = tableRouting.getValue();
            for (Routing routing : routingList) {
                allocateRoutingNodes(shardNodes, routing.locations());

                for (Map.Entry<String, Map<String, IntIndexedContainer>> entry : routing.locations().entrySet()) {
                    Map<String, IntIndexedContainer> shardsByIndex = entry.getValue();
                    Collection<String> indices = indicesByTable.computeIfAbsent(table, ignored -> new ArrayList<>());
                    indices.addAll(shardsByIndex.keySet());

                    for (Map.Entry<String, IntIndexedContainer> shardsByIndexEntry : shardsByIndex.entrySet()) {
                        indexBaseBuilder.allocate(shardsByIndexEntry.getKey(), shardsByIndexEntry.getValue());
                    }
                }
            }
        }
        return new ReaderAllocations(indexBaseBuilder.build(), shardNodes, indicesByTable);
    }

    private static void allocateRoutingNodes(Map<String, Map<Integer, String>> shardNodes,
                                             Map<String, Map<String, IntIndexedContainer>> locations) {
        for (Map.Entry<String, Map<String, IntIndexedContainer>> indicesByNodeId : locations.entrySet()) {
            String nodeId = indicesByNodeId.getKey();
            for (Map.Entry<String, IntIndexedContainer> shardsByIndexEntry : indicesByNodeId.getValue().entrySet()) {
                String index = shardsByIndexEntry.getKey();
                IntIndexedContainer shards = shardsByIndexEntry.getValue();

                Map<Integer, String> shardsOnIndex = shardNodes.get(index);
                if (shardsOnIndex == null) {
                    shardsOnIndex = HashMap.newHashMap(shards.size());
                    shardNodes.put(index, shardsOnIndex);
                    for (IntCursor id : shards) {
                        shardsOnIndex.put(id.value, nodeId);
                    }
                } else {
                    for (IntCursor id : shards) {
                        String allocatedNodeId = shardsOnIndex.get(id.value);
                        assert allocatedNodeId == null || allocatedNodeId.equals(nodeId) : "allocatedNodeId must match nodeId";
                        shardsOnIndex.put(id.value, nodeId);
                    }
                }
            }
        }
    }

    public ShardRouting resolveShard(String indexName, String id, String routing) {
        return routingProvider.forId(clusterState, indexName, id, routing);
    }
}
