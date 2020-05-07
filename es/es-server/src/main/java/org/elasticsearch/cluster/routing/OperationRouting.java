/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.cluster.routing;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.allocation.decider.AwarenessAllocationDecider;
import javax.annotation.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexNotFoundException;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class OperationRouting {

    private List<String> awarenessAttributes;

    public OperationRouting(Settings settings, ClusterSettings clusterSettings) {
        this.awarenessAttributes = AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_ATTRIBUTE_SETTING.get(settings);
        clusterSettings.addSettingsUpdateConsumer(AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_ATTRIBUTE_SETTING,
            this::setAwarenessAttributes);
    }

    private void setAwarenessAttributes(List<String> awarenessAttributes) {
        this.awarenessAttributes = awarenessAttributes;
    }

    public ShardIterator indexShards(ClusterState clusterState, String index, String id, @Nullable String routing) {
        return shards(clusterState, index, id, routing).shardsIt();
    }

    public ShardIterator getShards(ClusterState clusterState,
                                   String index,
                                   String id,
                                   @Nullable String routing,
                                   @Nullable String preference) {
        return preferenceActiveShardIterator(shards(clusterState, index, id, routing), clusterState.nodes().getLocalNodeId(), clusterState.nodes(), preference);
    }

    private ShardIterator preferenceActiveShardIterator(IndexShardRoutingTable indexShard,
                                                        String localNodeId,
                                                        DiscoveryNodes nodes,
                                                        @Nullable String preference) {
        if (preference == null || preference.isEmpty()) {
            if (awarenessAttributes.isEmpty()) {
                return indexShard.activeInitializingShardsRandomIt();
            } else {
                return indexShard.preferAttributesActiveInitializingShardsIt(awarenessAttributes, nodes);
            }
        }
        if (preference.charAt(0) == '_') {
            Preference preferenceType = Preference.parse(preference);
            if (preferenceType == Preference.SHARDS) {
                // starts with _shards, so execute on specific ones
                int index = preference.indexOf('|');

                String shards;
                if (index == -1) {
                    shards = preference.substring(Preference.SHARDS.type().length() + 1);
                } else {
                    shards = preference.substring(Preference.SHARDS.type().length() + 1, index);
                }
                String[] ids = Strings.splitStringByCommaToArray(shards);
                boolean found = false;
                for (String id : ids) {
                    if (Integer.parseInt(id) == indexShard.shardId().id()) {
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    return null;
                }
                // no more preference
                if (index == -1 || index == preference.length() - 1) {
                    if (awarenessAttributes.isEmpty()) {
                        return indexShard.activeInitializingShardsRandomIt();
                    } else {
                        return indexShard.preferAttributesActiveInitializingShardsIt(awarenessAttributes, nodes);
                    }
                } else {
                    // update the preference and continue
                    preference = preference.substring(index + 1);
                }
            }
            preferenceType = Preference.parse(preference);
            switch (preferenceType) {
                case PREFER_NODES:
                    final Set<String> nodesIds =
                            Arrays.stream(
                                    preference.substring(Preference.PREFER_NODES.type().length() + 1).split(",")
                            ).collect(Collectors.toSet());
                    return indexShard.preferNodeActiveInitializingShardsIt(nodesIds);
                case LOCAL:
                    return indexShard.preferNodeActiveInitializingShardsIt(Collections.singleton(localNodeId));
                case ONLY_LOCAL:
                    return indexShard.onlyNodeActiveInitializingShardsIt(localNodeId);
                case ONLY_NODES:
                    String nodeAttributes = preference.substring(Preference.ONLY_NODES.type().length() + 1);
                    return indexShard.onlyNodeSelectorActiveInitializingShardsIt(nodeAttributes.split(","), nodes);
                default:
                    throw new IllegalArgumentException("unknown preference [" + preferenceType + "]");
            }
        }
        // if not, then use it as the index
        // The AllocationService lists shards in a fixed order based on nodes
        // so earlier versions of this class would have a tendency to
        // select the same node across different shardIds.
        // Better overall balancing can be achieved if each shardId opts
        // for a different element in the list by also incorporating the
        // shard ID into the hash of the user-supplied preference key.
        int routingHash = 31 * Murmur3HashFunction.hash(preference) + indexShard.shardId.hashCode();
        if (awarenessAttributes.isEmpty()) {
            return indexShard.activeInitializingShardsIt(routingHash);
        } else {
            return indexShard.preferAttributesActiveInitializingShardsIt(awarenessAttributes, nodes, routingHash);
        }
    }

    public static IndexMetaData indexMetaData(ClusterState clusterState, String index) {
        IndexMetaData indexMetaData = clusterState.metaData().index(index);
        if (indexMetaData == null) {
            throw new IndexNotFoundException(index);
        }
        return indexMetaData;
    }

    protected IndexShardRoutingTable shards(ClusterState clusterState, String index, String id, String routing) {
        int shardId = generateShardId(indexMetaData(clusterState, index), id, routing);
        return clusterState.getRoutingTable().shardRoutingTable(index, shardId);
    }

    public static int generateShardId(IndexMetaData indexMetaData, @Nullable String id, @Nullable String routing) {
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

    public static int calculateScaledShardId(IndexMetaData indexMetaData, String effectiveRouting, int partitionOffset) {
        final int hash = Murmur3HashFunction.hash(effectiveRouting) + partitionOffset;

        // we don't use IMD#getNumberOfShards since the index might have been shrunk such that we need to use the size
        // of original index to hash documents
        return Math.floorMod(hash, indexMetaData.getRoutingNumShards()) / indexMetaData.getRoutingFactor();
    }

}
