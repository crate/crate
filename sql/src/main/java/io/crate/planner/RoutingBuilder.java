/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.planner;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import io.crate.action.sql.SessionContext;
import io.crate.analyze.WhereClause;
import io.crate.metadata.Routing;
import io.crate.metadata.RoutingProvider;
import io.crate.metadata.TableIdent;
import io.crate.metadata.table.TableInfo;
import io.crate.planner.fetch.IndexBaseBuilder;
import org.elasticsearch.cluster.ClusterState;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

final class RoutingBuilder {

    final Map<TableIdent, List<TableRouting>> routingListByTable = new HashMap<>();
    private final ClusterState clusterState;
    private final RoutingProvider routingProvider;

    private ReaderAllocations readerAllocations;

    RoutingBuilder(ClusterState clusterState, RoutingProvider routingProvider) {
        this.clusterState = clusterState;
        this.routingProvider = routingProvider;
    }

    @VisibleForTesting
    static final class TableRouting {
        final WhereClause where;
        private final RoutingProvider.ShardSelection shardSelection;
        final Routing routing;

        TableRouting(WhereClause where, RoutingProvider.ShardSelection shardSelection, Routing routing) {
            this.where = where;
            this.shardSelection = shardSelection;
            this.routing = routing;
        }
    }

    Routing allocateRouting(TableInfo tableInfo,
                            WhereClause where,
                            RoutingProvider.ShardSelection shardSelection,
                            SessionContext sessionContext) {

        Routing routing = tableInfo.getRouting(clusterState, routingProvider, where, shardSelection, sessionContext);
        List<TableRouting> existingRoutings = routingListByTable.get(tableInfo.ident());
        if (existingRoutings == null) {
            existingRoutings = new ArrayList<>();
            routingListByTable.put(tableInfo.ident(), existingRoutings);
        }
        existingRoutings.add(new TableRouting(where, shardSelection, routing));
        return routing;
    }

    ReaderAllocations buildReaderAllocations() {
        if (readerAllocations != null) {
            return readerAllocations;
        }

        Multimap<TableIdent, String> indicesByTable = HashMultimap.create();
        IndexBaseBuilder indexBaseBuilder = new IndexBaseBuilder();
        Map<String, Map<Integer, String>> shardNodes = new HashMap<>();

        for (final Map.Entry<TableIdent, List<TableRouting>> tableRoutingEntry : routingListByTable.entrySet()) {
            TableIdent table = tableRoutingEntry.getKey();
            List<TableRouting> routingList = tableRoutingEntry.getValue();
            for (TableRouting tr : routingList) {
                allocateRoutingNodes(shardNodes, tr.routing.locations());

                for (Map.Entry<String, Map<String, List<Integer>>> entry : tr.routing.locations().entrySet()) {
                    Map<String, List<Integer>> shardsByIndex = entry.getValue();
                    indicesByTable.putAll(table, shardsByIndex.keySet());

                    for (Map.Entry<String, List<Integer>> shardsByIndexEntry : shardsByIndex.entrySet()) {
                        indexBaseBuilder.allocate(shardsByIndexEntry.getKey(), shardsByIndexEntry.getValue());
                    }
                }
            }
        }
        readerAllocations = new ReaderAllocations(indexBaseBuilder.build(), shardNodes, indicesByTable);
        return readerAllocations;
    }

    private static void allocateRoutingNodes(Map<String, Map<Integer, String>> shardNodes,
                                             Map<String, Map<String, List<Integer>>> locations) {
        for (Map.Entry<String, Map<String, List<Integer>>> indicesByNodeId : locations.entrySet()) {
            String nodeId = indicesByNodeId.getKey();
            for (Map.Entry<String, List<Integer>> shardsByIndexEntry : indicesByNodeId.getValue().entrySet()) {
                String index = shardsByIndexEntry.getKey();
                List<Integer> shards = shardsByIndexEntry.getValue();

                Map<Integer, String> shardsOnIndex = shardNodes.get(index);
                if (shardsOnIndex == null) {
                    shardsOnIndex = new HashMap<>(shards.size());
                    shardNodes.put(index, shardsOnIndex);
                    for (Integer id : shards) {
                        shardsOnIndex.put(id, nodeId);
                    }
                } else {
                    for (Integer id : shards) {
                        String allocatedNodeId = shardsOnIndex.get(id);
                        assert allocatedNodeId == null || allocatedNodeId.equals(nodeId) : "allocatedNodeId must match nodeId";
                        shardsOnIndex.put(id, nodeId);
                    }
                }
            }
        }
    }
}
