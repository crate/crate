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

package io.crate.metadata;

import io.crate.exceptions.UnavailableShardsException;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.Murmur3HashFunction;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardNotFoundException;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static io.crate.metadata.Routing.forTableOnSingleNode;

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
    private final String[] awarenessAttributes;

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
    public RoutingProvider(int randomSeed, String[] awarenessAttributes) {
        this.awarenessAttributes = awarenessAttributes;
        this.seed = randomSeed;
    }

    public Routing forRandomMasterOrDataNode(RelationName relationName, DiscoveryNodes nodes) {
        DiscoveryNode localNode = nodes.getLocalNode();
        if (localNode.isMasterNode() || localNode.isDataNode()) {
            return forTableOnSingleNode(relationName, localNode.getId());
        }
        ImmutableOpenMap<String, DiscoveryNode> masterAndDataNodes = nodes.getMasterAndDataNodes();
        int randomIdx = seed % masterAndDataNodes.size();
        Iterator<DiscoveryNode> it = masterAndDataNodes.valuesIt();
        int currIdx = 0;
        while (it.hasNext()) {
            if (currIdx == randomIdx) {
                return forTableOnSingleNode(relationName, it.next().getId());
            }
            currIdx++;
        }
        throw new AssertionError("Cannot find a master or data node with given random index " + randomIdx);
    }

    public ShardRouting forId(ClusterState state, String index, String id, @Nullable String routing) {
        IndexMetaData indexMetaData = indexMetaData(state, index);
        ShardId shardId = new ShardId(indexMetaData.getIndex(), generateShardId(indexMetaData, id, routing));
        return state.getRoutingTable().shardRoutingTable(shardId).primaryShard();
    }

    public Routing forIndices(ClusterState state,
                              String[] concreteIndices,
                              Map<String, Set<String>> routingValuesByIndex,
                              boolean ignoreMissingShards,
                              ShardSelection shardSelection) {

        Set<IndexShardRoutingTable> shards;
        try {
            shards = computeTargetedShards(state, concreteIndices, routingValuesByIndex);
        } catch (IndexNotFoundException e) {
            return new Routing(Collections.emptyMap());
        }
        Map<String, Map<String, List<Integer>>> locations = new TreeMap<>();

        for (IndexShardRoutingTable shard : shards) {
            final ShardIterator shardIt;
            switch (shardSelection) {
                case ANY:
                    if (awarenessAttributes.length == 0) {
                        shardIt = shard.activeInitializingShardsIt(seed);
                    } else {
                        shardIt = shard.preferAttributesActiveInitializingShardsIt(
                            awarenessAttributes, state.getNodes(), seed);
                    }
                    break;

                case PRIMARIES:
                    shardIt = shard.primaryActiveInitializingShardIt();
                    break;


                default:
                    throw new AssertionError("Invalid ShardSelection: " + shardSelection);
            }
            fillLocationsFromShardIterator(ignoreMissingShards, locations, shardIt);
        }
        return new Routing(locations);
    }

    private static void fillLocationsFromShardIterator(boolean ignoreMissingShards,
                                                       Map<String, Map<String, List<Integer>>> locations,
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

    private static void processShardRouting(Map<String, Map<String, List<Integer>>> locations, ShardRouting shardRouting) {
        String node = shardRouting.currentNodeId();
        Map<String, List<Integer>> nodeMap = locations.get(node);
        if (nodeMap == null) {
            nodeMap = new TreeMap<>();
            locations.put(shardRouting.currentNodeId(), nodeMap);
        }

        String indexName = shardRouting.getIndexName();
        List<Integer> shards = nodeMap.get(indexName);
        if (shards == null) {
            shards = new ArrayList<>();
            nodeMap.put(indexName, shards);
        }
        shards.add(shardRouting.id());
    }

    private static Set<IndexShardRoutingTable> computeTargetedShards(ClusterState clusterState,
                                                                     String[] concreteIndices,
                                                                     Map<String, Set<String>> routing) {
        LinkedHashSet<IndexShardRoutingTable> set = new LinkedHashSet<>();
        for (String index : concreteIndices) {
            final IndexRoutingTable indexRouting = indexRoutingTable(clusterState, index);
            final IndexMetaData indexMetaData = indexMetaData(clusterState, index);
            final Set<String> effectiveRouting = routing.get(index);
            if (effectiveRouting != null) {
                for (String r : effectiveRouting) {
                    final int routingPartitionSize = indexMetaData.getRoutingPartitionSize();
                    for (int partitionOffset = 0; partitionOffset < routingPartitionSize; partitionOffset++) {
                        set.add(shardRoutingTable(indexRouting, calculateScaledShardId(indexMetaData, r, partitionOffset)));
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

    private static IndexMetaData indexMetaData(ClusterState clusterState, String index) {
        IndexMetaData indexMetaData = clusterState.metaData().index(index);
        if (indexMetaData == null) {
            throw new IndexNotFoundException(index);
        }
        return indexMetaData;
    }

    private static IndexShardRoutingTable shardRoutingTable(IndexRoutingTable indexRouting, int shardId) {
        IndexShardRoutingTable indexShard = indexRouting.shard(shardId);
        if (indexShard == null) {
            throw new ShardNotFoundException(new ShardId(indexRouting.getIndex(), shardId));
        }
        return indexShard;
    }

    private static int calculateScaledShardId(IndexMetaData indexMetaData, String effectiveRouting, int partitionOffset) {
        final int hash = Murmur3HashFunction.hash(effectiveRouting) + partitionOffset;
        // we don't use IMD#getNumberOfShards since the index might have been shrunk such that we need to use the size
        // of original index to hash documents
        return Math.floorMod(hash, indexMetaData.getRoutingNumShards()) / indexMetaData.getRoutingFactor();
    }

    private static int generateShardId(IndexMetaData indexMetaData, @Nullable String id, @Nullable String routing) {
        final String effectiveRouting;
        final int partitionOffset;

        if (routing == null) {
            assert (indexMetaData.isRoutingPartitionedIndex() == false) : "A routing value is required for gets from a partitioned index";
            effectiveRouting = id;
        } else {
            effectiveRouting = routing;
        }

        if (indexMetaData.isRoutingPartitionedIndex()) {
            partitionOffset = Math.floorMod(Murmur3HashFunction.hash(id), indexMetaData.getRoutingPartitionSize());
        } else {
            // we would have still got 0 above but this check just saves us an unnecessary hash calculation
            partitionOffset = 0;
        }

        return calculateScaledShardId(indexMetaData, effectiveRouting, partitionOffset);
    }
}
