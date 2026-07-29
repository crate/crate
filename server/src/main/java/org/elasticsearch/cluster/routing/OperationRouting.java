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

import java.util.Collections;
import java.util.List;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.allocation.decider.AwarenessAllocationDecider;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexNotFoundException;
import org.jspecify.annotations.Nullable;

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

    public ShardIterator indexShards(ClusterState clusterState, String indexUUID, String id, @Nullable String routing) {
        return shards(clusterState, indexUUID, id, routing).shardsIt();
    }

    public ShardIterator getShards(ClusterState clusterState,
                                   String indexUUID,
                                   String id,
                                   @Nullable String routing,
                                   @Nullable Preference preference) {
        return preferenceActiveShardIterator(shards(clusterState, indexUUID, id, routing), clusterState.nodes().getLocalNodeId(), clusterState.nodes(), preference);
    }

    private ShardIterator preferenceActiveShardIterator(IndexShardRoutingTable indexShard,
                                                        String localNodeId,
                                                        DiscoveryNodes nodes,
                                                        @Nullable Preference preference) {
        if (preference == null) {
            return shardRoutings(indexShard, nodes);
        }
        return switch (preference) {
            case LOCAL -> indexShard.preferNodeActiveInitializingShardsIt(Collections.singleton(localNodeId));
            case ONLY_LOCAL -> indexShard.onlyNodeActiveInitializingShardsIt(localNodeId);
        };
    }

    private ShardIterator shardRoutings(IndexShardRoutingTable indexShard, DiscoveryNodes nodes) {
        if (awarenessAttributes.isEmpty()) {
            return indexShard.activeInitializingShardsRandomIt();
        } else {
            return indexShard.preferAttributesActiveInitializingShardsIt(awarenessAttributes, nodes);
        }
    }

    public static IndexMetadata indexMetadata(ClusterState clusterState, String indexUUID) {
        IndexMetadata indexMetadata = clusterState.metadata().index(indexUUID);
        if (indexMetadata == null) {
            throw new IndexNotFoundException(indexUUID);
        }
        return indexMetadata;
    }

    protected IndexShardRoutingTable shards(ClusterState clusterState, String indexUUID, String id, String routing) {
        int shardId = generateShardId(indexMetadata(clusterState, indexUUID), id, routing);
        return clusterState.routingTable().shardRoutingTable(indexUUID, shardId);
    }

    public static int generateShardId(IndexMetadata indexMetadata, @Nullable String id, @Nullable String routing) {
        final String effectiveRouting;
        final int partitionOffset;

        if (routing == null) {
            assert (indexMetadata.isRoutingPartitionedIndex() == false) : "A routing value is required for gets from a partitioned index";
            effectiveRouting = id;
        } else {
            effectiveRouting = routing;
        }

        if (indexMetadata.isRoutingPartitionedIndex()) {
            partitionOffset = Math.floorMod(Murmur3HashFunction.hash(id), indexMetadata.getRoutingPartitionSize());
        } else {
            // we would have still got 0 above but this check just saves us an unnecessary hash calculation
            partitionOffset = 0;
        }

        return calculateScaledShardId(indexMetadata, effectiveRouting, partitionOffset);
    }

    public static int calculateScaledShardId(IndexMetadata indexMetadata, String effectiveRouting, int partitionOffset) {
        final int hash = Murmur3HashFunction.hash(effectiveRouting) + partitionOffset;

        // we don't use IMD#getNumberOfShards since the index might have been shrunk such that we need to use the size
        // of original index to hash documents
        return Math.floorMod(hash, indexMetadata.getRoutingNumShards()) / indexMetadata.getRoutingFactor();
    }

}
