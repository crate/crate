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

package io.crate.metadata;

import static io.crate.metadata.Routing.forTableOnSingleNode;
import static org.elasticsearch.cluster.routing.OperationRouting.calculateScaledShardId;
import static org.elasticsearch.cluster.routing.OperationRouting.generateShardId;
import static org.elasticsearch.cluster.routing.OperationRouting.indexMetadata;

import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardNotFoundException;
import org.jetbrains.annotations.Nullable;

import com.carrotsearch.hppc.IntArrayList;
import com.carrotsearch.hppc.IntIndexedContainer;

import io.crate.exceptions.UnavailableShardsException;

/**
 * This component can be used to get the Routing for indices.
 *
 * {@link #forIndices(ClusterState, String[], Map, boolean, ShardSelection)} and
 * {@link #forIndices(ClusterState, String[], Map, boolean, ShardSelection)}
 * will always return the same results for the same arguments, because {@code random} is used to fixate a seed.
 *
 * The implementation is similar to {@link org.elasticsearch.cluster.routing.OperationRouting}.
 */
public final class RoutingProvider {

    private final int seed;
    private final List<String> awarenessAttributes;

    public enum ShardSelection {
        ANY,
        PRIMARIES
    }

    /**
     * @param randomSeed seed used to shuffle/randomize the used shards.
     *                   Using a fixed seed means that this instance will always use the same shards for the same
     *                   arguments.
     *                   It's recommended to use *different* seeds across different instances to distribute the load
     *                   across different (replica) shards
     */
    public RoutingProvider(int randomSeed, List<String> awarenessAttributes) {
        this.awarenessAttributes = awarenessAttributes;
        this.seed = randomSeed;
    }

    public Routing forRandomMasterOrDataNode(RelationName relationName, DiscoveryNodes nodes) {
        DiscoveryNode localNode = nodes.getLocalNode();
        if (localNode.isMasterEligibleNode() || localNode.isDataNode()) {
            return forTableOnSingleNode(relationName, localNode.getId());
        }
        ImmutableOpenMap<String, DiscoveryNode> masterAndDataNodes = nodes.getMasterAndDataNodes();
        int randomIdx = Math.abs(seed % masterAndDataNodes.size());
        Iterator<DiscoveryNode> it = masterAndDataNodes.valuesIt();
        int currIdx = 0;
        while (it.hasNext()) {
            DiscoveryNode next = it.next();
            if (currIdx == randomIdx) {
                return forTableOnSingleNode(relationName, next.getId());
            }
            currIdx++;
        }
        throw new AssertionError("Cannot find a master or data node with given random index " + randomIdx);
    }

    public ShardRouting forId(ClusterState state, String index, String id, @Nullable String routing) {
        IndexMetadata indexMetadata = indexMetadata(state, index);
        ShardId shardId = new ShardId(indexMetadata.getIndex(), generateShardId(indexMetadata, id, routing));
        IndexShardRoutingTable routingTable = state.routingTable().shardRoutingTable(shardId);
        ShardRouting shardRouting;
        if (awarenessAttributes.isEmpty()) {
            shardRouting = routingTable.activeInitializingShardsIt(seed).nextOrNull();
        } else {
            shardRouting = routingTable
                .preferAttributesActiveInitializingShardsIt(awarenessAttributes, state.nodes(), seed)
                .nextOrNull();
        }
        return shardRouting == null ? routingTable.primaryShard() : shardRouting;
    }

    public Routing forIndices(ClusterState state,
                              String[] concreteIndices,
                              Set<String> routingValues,
                              boolean ignoreMissingShards,
                              ShardSelection shardSelection) {

        Set<IndexShardRoutingTable> shards;
        try {
            shards = computeTargetedShards(state, concreteIndices, routingValues);
        } catch (IndexNotFoundException e) {
            return new Routing(Collections.emptyMap());
        }
        Map<String, Map<String, IntIndexedContainer>> locations = new TreeMap<>();

        for (IndexShardRoutingTable shard : shards) {
            final ShardIterator shardIt;
            switch (shardSelection) {
                case ANY:
                    if (awarenessAttributes.isEmpty()) {
                        shardIt = shard.activeInitializingShardsIt(seed);
                    } else {
                        shardIt = shard.preferAttributesActiveInitializingShardsIt(
                            awarenessAttributes, state.nodes(), seed);
                    }
                    break;

                case PRIMARIES:
                    shardIt = shard.primaryShardIt();
                    break;


                default:
                    throw new AssertionError("Invalid ShardSelection: " + shardSelection);
            }
            fillLocationsFromShardIterator(ignoreMissingShards, locations, shardIt);
        }
        return new Routing(locations);
    }

    private static void fillLocationsFromShardIterator(boolean ignoreMissingShards,
                                                       Map<String, Map<String, IntIndexedContainer>> locations,
                                                       ShardIterator shardIterator) {
        ShardRouting shardRouting = shardIterator.nextOrNull();
        if (shardRouting == null) {
            if (ignoreMissingShards) {
                return;
            }
            throw new UnavailableShardsException(shardIterator.shardId());
        }
        processShardRouting(locations, shardRouting);
    }

    private static void processShardRouting(Map<String, Map<String, IntIndexedContainer>> locations, ShardRouting shardRouting) {
        String node = shardRouting.currentNodeId();
        Map<String, IntIndexedContainer> nodeMap = locations.get(node);
        if (nodeMap == null) {
            nodeMap = new TreeMap<>();
            locations.put(shardRouting.currentNodeId(), nodeMap);
        }

        String indexName = shardRouting.getIndexName();
        IntIndexedContainer shards = nodeMap.get(indexName);
        if (shards == null) {
            shards = new IntArrayList();
            nodeMap.put(indexName, shards);
        }
        shards.add(shardRouting.id());
    }

    private static Set<IndexShardRoutingTable> computeTargetedShards(ClusterState clusterState,
                                                                     String[] concreteIndices,
                                                                     Set<String> routingValues) {
        LinkedHashSet<IndexShardRoutingTable> set = new LinkedHashSet<>();
        for (String index : concreteIndices) {
            final IndexRoutingTable indexRouting = indexRoutingTable(clusterState, index);
            final IndexMetadata indexMetadata = indexMetadata(clusterState, index);
            if (!routingValues.isEmpty()) {
                for (String r : routingValues) {
                    final int routingPartitionSize = indexMetadata.getRoutingPartitionSize();
                    for (int partitionOffset = 0; partitionOffset < routingPartitionSize; partitionOffset++) {
                        set.add(shardRoutingTable(indexRouting, calculateScaledShardId(indexMetadata, r, partitionOffset)));
                    }
                }
            } else {
                for (IndexShardRoutingTable indexShard : indexRouting) {
                    set.add(indexShard);
                }
            }
        }
        return set;
    }

    private static IndexRoutingTable indexRoutingTable(ClusterState clusterState, String index) {
        IndexRoutingTable indexRouting = clusterState.routingTable().index(index);
        if (indexRouting == null) {
            throw new IndexNotFoundException(index);
        }
        return indexRouting;
    }

    private static IndexShardRoutingTable shardRoutingTable(IndexRoutingTable indexRouting, int shardId) {
        IndexShardRoutingTable indexShard = indexRouting.shard(shardId);
        if (indexShard == null) {
            throw new ShardNotFoundException(new ShardId(indexRouting.getIndex(), shardId));
        }
        return indexShard;
    }
}
