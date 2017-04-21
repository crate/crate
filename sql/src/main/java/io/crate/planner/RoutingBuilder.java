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
import io.crate.analyze.WhereClause;
import io.crate.metadata.Routing;
import io.crate.metadata.TableIdent;
import io.crate.metadata.table.TableInfo;
import io.crate.planner.fetch.IndexBaseBuilder;

import javax.annotation.Nullable;
import java.util.*;

final class RoutingBuilder {

    final Map<TableIdent, List<TableRouting>> routingListByTable = new HashMap<>();

    private ReaderAllocations readerAllocations;

    @VisibleForTesting
    final static class TableRouting {
        final WhereClause where;
        final String preference;
        final Routing routing;

        TableRouting(WhereClause where, String preference, Routing routing) {
            this.where = where;
            this.preference = preference;
            this.routing = routing;
        }
    }

    Routing allocateRouting(TableInfo tableInfo, WhereClause where, @Nullable String preference) {
        List<TableRouting> existingRoutings = routingListByTable.get(tableInfo.ident());
        if (existingRoutings == null) {
            return allocateNewRouting(tableInfo, where, preference);
        }
        Routing existing = tryFindMatchInExisting(where, preference, existingRoutings);
        if (existing != null) return existing;

        Routing routing = tableInfo.getRouting(where, preference);
        existingRoutings.add(new TableRouting(where, preference, routing));
        // ensure all routings of this table are allocated
        // and update new routing by merging with existing ones
        for (TableRouting existingRouting : existingRoutings) {
            // Merge locations with existing routing
            routing.mergeLocations(existingRouting.routing.locations());
        }
        return routing;
    }

    private static Routing tryFindMatchInExisting(WhereClause where,
                                                  @Nullable String preference,
                                                  Iterable<TableRouting> existingRoutings) {
        for (TableRouting existing : existingRoutings) {
            assert preference == null || preference.equals(existing.preference) :
                "preference must not be null or equals existing preference";
            if (Objects.equals(existing.where, where)) {
                return existing.routing;
            }
        }
        return null;
    }

    private Routing allocateNewRouting(TableInfo tableInfo, WhereClause where, @Nullable String preference) {
        List<TableRouting> existingRoutings = new ArrayList<>();
        routingListByTable.put(tableInfo.ident(), existingRoutings);
        Routing routing = tableInfo.getRouting(where, preference);
        existingRoutings.add(new TableRouting(where, preference, routing));
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
