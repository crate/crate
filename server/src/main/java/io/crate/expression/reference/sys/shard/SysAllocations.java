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

package io.crate.expression.reference.sys.shard;

import io.crate.metadata.IndexParts;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterInfoService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.AllocateUnassignedDecision;
import org.elasticsearch.cluster.routing.allocation.MoveDecision;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.ShardAllocationDecision;
import org.elasticsearch.cluster.routing.allocation.allocator.ShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.gateway.GatewayAllocator;

import java.util.Iterator;
import java.util.function.Supplier;

@Singleton
public class SysAllocations implements Iterable<SysAllocation> {

    private final ClusterService clusterService;
    private final ClusterInfoService clusterInfoService;
    private final AllocationDeciders allocationDeciders;
    private final ShardsAllocator shardAllocator;
    private final GatewayAllocator gatewayAllocator;

    @Inject
    public SysAllocations(ClusterService clusterService,
                          ClusterInfoService clusterInfoService,
                          AllocationDeciders allocationDeciders,
                          ShardsAllocator shardAllocator,
                          GatewayAllocator gatewayAllocator) {
        this.clusterService = clusterService;
        this.clusterInfoService = clusterInfoService;
        this.allocationDeciders = allocationDeciders;
        this.shardAllocator = shardAllocator;
        this.gatewayAllocator = gatewayAllocator;
    }

    @Override
    public Iterator<SysAllocation> iterator() {
        final ClusterState state = clusterService.state();
        final RoutingNodes routingNodes = state.getRoutingNodes();
        final ClusterInfo clusterInfo = clusterInfoService.getClusterInfo();
        final RoutingAllocation allocation = new RoutingAllocation(
            allocationDeciders, routingNodes, state, clusterInfo, System.nanoTime());
        return allocation.routingTable().allShards()
            .stream()
            .filter(shardRouting -> !IndexParts.isDangling(shardRouting.getIndexName()))
            .map(shardRouting -> createSysAllocations(allocation, shardRouting))
            .iterator();
    }

    private SysAllocation createSysAllocations(RoutingAllocation allocation, ShardRouting shardRouting) {
        allocation.setDebugMode(RoutingAllocation.DebugMode.EXCLUDE_YES_DECISIONS);
        Supplier<ShardAllocationDecision> shardDecision = () -> {
            if (shardRouting.initializing() || shardRouting.relocating()) {
                return ShardAllocationDecision.NOT_TAKEN;
            } else {
                AllocateUnassignedDecision allocateDecision = shardRouting.unassigned()
                    ? gatewayAllocator.decideUnassignedShardAllocation(shardRouting, allocation)
                    : AllocateUnassignedDecision.NOT_TAKEN;
                if (allocateDecision.isDecisionTaken() == false) {
                    return shardAllocator.decideShardAllocation(shardRouting, allocation);
                } else {
                    return new ShardAllocationDecision(allocateDecision, MoveDecision.NOT_TAKEN);
                }
            }
        };
        return new SysAllocation(
            shardRouting.shardId(),
            shardRouting.state(),
            shardDecision,
            shardRouting.currentNodeId(),
            shardRouting.primary()
        );
    }
}
