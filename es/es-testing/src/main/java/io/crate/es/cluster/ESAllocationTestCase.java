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

package io.crate.es.cluster;

import io.crate.es.Version;
import io.crate.es.cluster.node.DiscoveryNode;
import io.crate.es.cluster.routing.RoutingNode;
import io.crate.es.cluster.routing.RoutingNodes;
import io.crate.es.cluster.routing.ShardRouting;
import io.crate.es.cluster.routing.UnassignedInfo;
import io.crate.es.cluster.routing.allocation.AllocationService;
import io.crate.es.cluster.routing.allocation.FailedShard;
import io.crate.es.cluster.routing.allocation.RoutingAllocation;
import io.crate.es.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import io.crate.es.cluster.routing.allocation.allocator.ShardsAllocator;
import io.crate.es.cluster.routing.allocation.decider.AllocationDecider;
import io.crate.es.cluster.routing.allocation.decider.AllocationDeciders;
import io.crate.es.cluster.routing.allocation.decider.Decision;
import io.crate.es.common.settings.ClusterSettings;
import io.crate.es.common.settings.Settings;
import io.crate.es.gateway.GatewayAllocator;
import io.crate.es.test.ESTestCase;
import io.crate.es.test.gateway.TestGatewayAllocator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static java.util.Collections.emptyMap;

public abstract class ESAllocationTestCase extends ESTestCase {
    private static final ClusterSettings EMPTY_CLUSTER_SETTINGS =
        new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);

    public static MockAllocationService createAllocationService() {
        return createAllocationService(Settings.Builder.EMPTY_SETTINGS);
    }

    public static MockAllocationService createAllocationService(Settings settings) {
        return createAllocationService(settings, random());
    }

    public static MockAllocationService createAllocationService(Settings settings, Random random) {
        return createAllocationService(settings, EMPTY_CLUSTER_SETTINGS, random);
    }

    public static MockAllocationService createAllocationService(Settings settings, ClusterSettings clusterSettings, Random random) {
        return new MockAllocationService(settings,
                randomAllocationDeciders(settings, clusterSettings, random),
                new TestGatewayAllocator(), new BalancedShardsAllocator(settings), EmptyClusterInfoService.INSTANCE);
    }

    public static MockAllocationService createAllocationService(Settings settings, ClusterInfoService clusterInfoService) {
        return new MockAllocationService(settings,
                randomAllocationDeciders(settings, EMPTY_CLUSTER_SETTINGS, random()),
            new TestGatewayAllocator(), new BalancedShardsAllocator(settings), clusterInfoService);
    }

    public static MockAllocationService createAllocationService(Settings settings, GatewayAllocator gatewayAllocator) {
        return new MockAllocationService(settings,
                randomAllocationDeciders(settings, EMPTY_CLUSTER_SETTINGS, random()),
                gatewayAllocator, new BalancedShardsAllocator(settings), EmptyClusterInfoService.INSTANCE);
    }

    public static AllocationDeciders randomAllocationDeciders(Settings settings, ClusterSettings clusterSettings, Random random) {
        List<AllocationDecider> deciders = new ArrayList<>(
            ClusterModule.createAllocationDeciders(settings, clusterSettings, Collections.emptyList()));
        Collections.shuffle(deciders, random);
        return new AllocationDeciders(settings, deciders);
    }

    protected static Set<DiscoveryNode.Role> MASTER_DATA_ROLES =
            Collections.unmodifiableSet(EnumSet.of(DiscoveryNode.Role.MASTER, DiscoveryNode.Role.DATA));

    protected static DiscoveryNode newNode(String nodeId) {
        return newNode(nodeId, Version.CURRENT);
    }

    protected static DiscoveryNode newNode(String nodeName, String nodeId, Map<String, String> attributes) {
        return new DiscoveryNode(nodeName, nodeId, buildNewFakeTransportAddress(), attributes, MASTER_DATA_ROLES, Version.CURRENT);
    }

    protected static DiscoveryNode newNode(String nodeId, Map<String, String> attributes) {
        return new DiscoveryNode(nodeId, buildNewFakeTransportAddress(), attributes, MASTER_DATA_ROLES, Version.CURRENT);
    }

    protected static DiscoveryNode newNode(String nodeId, Set<DiscoveryNode.Role> roles) {
        return new DiscoveryNode(nodeId, buildNewFakeTransportAddress(), emptyMap(), roles, Version.CURRENT);
    }

    protected static DiscoveryNode newNode(String nodeId, Version version) {
        return new DiscoveryNode(nodeId, buildNewFakeTransportAddress(), emptyMap(), MASTER_DATA_ROLES, version);
    }

    public static class TestAllocateDecision extends AllocationDecider {

        private final Decision decision;

        public TestAllocateDecision(Decision decision) {
            super(Settings.EMPTY);
            this.decision = decision;
        }

        @Override
        public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
            return decision;
        }

        @Override
        public Decision canAllocate(ShardRouting shardRouting, RoutingAllocation allocation) {
            return decision;
        }

        @Override
        public Decision canAllocate(RoutingNode node, RoutingAllocation allocation) {
            return decision;
        }
    }

    /** A lock {@link AllocationService} allowing tests to override time */
    protected static class MockAllocationService extends AllocationService {

        private volatile long nanoTimeOverride = -1L;

        public MockAllocationService(Settings settings, AllocationDeciders allocationDeciders, GatewayAllocator gatewayAllocator,
                                     ShardsAllocator shardsAllocator, ClusterInfoService clusterInfoService) {
            super(settings, allocationDeciders, gatewayAllocator, shardsAllocator, clusterInfoService);
        }

        public void setNanoTimeOverride(long nanoTime) {
            this.nanoTimeOverride = nanoTime;
        }

        @Override
        protected long currentNanoTime() {
            return nanoTimeOverride == -1L ? super.currentNanoTime() : nanoTimeOverride;
        }
    }

    /**
     * Mocks behavior in ReplicaShardAllocator to remove delayed shards from list of unassigned shards so they don't get reassigned yet.
     */
    protected static class DelayedShardsMockGatewayAllocator extends GatewayAllocator {

        public DelayedShardsMockGatewayAllocator() {
            super(Settings.EMPTY);
        }

        @Override
        public void applyStartedShards(RoutingAllocation allocation, List<ShardRouting> startedShards) {
            // no-op
        }

        @Override
        public void applyFailedShards(RoutingAllocation allocation, List<FailedShard> failedShards) {
            // no-op
        }

        @Override
        public void allocateUnassigned(RoutingAllocation allocation) {
            final RoutingNodes.UnassignedShards.UnassignedIterator unassignedIterator = allocation.routingNodes().unassigned().iterator();
            while (unassignedIterator.hasNext()) {
                ShardRouting shard = unassignedIterator.next();
                if (shard.primary() || shard.unassignedInfo().getReason() == UnassignedInfo.Reason.INDEX_CREATED) {
                    continue;
                }
                if (shard.unassignedInfo().isDelayed()) {
                    unassignedIterator.removeAndIgnore(UnassignedInfo.AllocationStatus.DELAYED_ALLOCATION, allocation.changes());
                }
            }
        }
    }
}
