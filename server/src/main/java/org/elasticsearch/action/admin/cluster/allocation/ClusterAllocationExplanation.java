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

package org.elasticsearch.action.admin.cluster.allocation;

import javax.annotation.Nullable;

import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.ShardAllocationDecision;
import org.elasticsearch.index.shard.ShardId;

/**
 * A {@code ClusterAllocationExplanation} is an explanation of why a shard is unassigned,
 * or if it is not unassigned, then which nodes it could possibly be relocated to.
 * It is an immutable class.
 */
public final class ClusterAllocationExplanation {

    private final ShardRouting shardRouting;
    private final DiscoveryNode currentNode;
    private final DiscoveryNode relocationTargetNode;
    private final ClusterInfo clusterInfo;
    private final ShardAllocationDecision shardAllocationDecision;

    public ClusterAllocationExplanation(ShardRouting shardRouting,
                                        @Nullable DiscoveryNode currentNode,
                                        @Nullable DiscoveryNode relocationTargetNode,
                                        @Nullable ClusterInfo clusterInfo,
                                        ShardAllocationDecision shardAllocationDecision) {
        this.shardRouting = shardRouting;
        this.currentNode = currentNode;
        this.relocationTargetNode = relocationTargetNode;
        this.clusterInfo = clusterInfo;
        this.shardAllocationDecision = shardAllocationDecision;
    }

    /**
     * Returns the shard that the explanation is about.
     */
    public ShardId getShard() {
        return shardRouting.shardId();
    }

    /**
     * Returns {@code true} if the explained shard is primary, {@code false} otherwise.
     */
    public boolean isPrimary() {
        return shardRouting.primary();
    }

    /**
     * Returns the current {@link ShardRoutingState} of the shard.
     */
    public ShardRoutingState getShardState() {
        return shardRouting.state();
    }

    /**
     * Returns the currently assigned node, or {@code null} if the shard is unassigned.
     */
    @Nullable
    public DiscoveryNode getCurrentNode() {
        return currentNode;
    }

    /**
     * Returns the relocating target node, or {@code null} if the shard is not in the {@link ShardRoutingState#RELOCATING} state.
     */
    @Nullable
    public DiscoveryNode getRelocationTargetNode() {
        return relocationTargetNode;
    }

    /**
     * Returns the unassigned info for the shard, or {@code null} if the shard is active.
     */
    @Nullable
    public UnassignedInfo getUnassignedInfo() {
        return shardRouting.unassignedInfo();
    }

    /**
     * Returns the cluster disk info for the cluster, or {@code null} if none available.
     */
    @Nullable
    public ClusterInfo getClusterInfo() {
        return this.clusterInfo;
    }

    /** \
     * Returns the shard allocation decision for attempting to assign or move the shard.
     */
    public ShardAllocationDecision getShardAllocationDecision() {
        return shardAllocationDecision;
    }
}
