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

package org.elasticsearch.cluster.routing.allocation.decider;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collection;
import java.util.Collections;
import java.util.function.Consumer;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;

public class AllocationDecidersTests extends ESTestCase {

    public void testDebugMode() {
        verifyDebugMode(RoutingAllocation.DebugMode.ON, result -> assertThat(result).hasSize(1));
    }

    public void testNoDebugMode() {
        verifyDebugMode(RoutingAllocation.DebugMode.OFF, result -> assertThat(result).isEmpty());
    }

    public void testDebugExcludeYesMode() {
        verifyDebugMode(RoutingAllocation.DebugMode.EXCLUDE_YES_DECISIONS, result -> assertThat(result).isEmpty());
    }

    private void verifyDebugMode(RoutingAllocation.DebugMode mode, Consumer<Collection<? extends Decision>> matcher) {
        AllocationDeciders deciders = new AllocationDeciders(Collections.singleton(new AllocationDecider() {
            @Override
            public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
                return Decision.YES;
            }

            @Override
            public Decision canRebalance(ShardRouting shardRouting, RoutingAllocation allocation) {
                return Decision.YES;
            }

            @Override
            public Decision canRemain(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
                return Decision.YES;
            }

            @Override
            public Decision canAllocate(ShardRouting shardRouting, RoutingAllocation allocation) {
                return Decision.YES;
            }

            @Override
            public Decision canAllocate(IndexMetadata indexMetadata, RoutingNode node, RoutingAllocation allocation) {
                return Decision.YES;
            }

            @Override
            public Decision shouldAutoExpandToNode(IndexMetadata indexMetadata, DiscoveryNode node, RoutingAllocation allocation) {
                return Decision.YES;
            }

            @Override
            public Decision canRebalance(RoutingAllocation allocation) {
                return Decision.YES;
            }
        }));

        ClusterState clusterState = ClusterState.builder(new ClusterName("test")).build();
        final RoutingAllocation allocation = new RoutingAllocation(deciders,
            clusterState.getRoutingNodes(), clusterState, null, null,0L);

        allocation.setDebugMode(mode);
        final UnassignedInfo unassignedInfo = new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "_message");
        final ShardRouting shardRouting = ShardRouting.newUnassigned(new ShardId("test", "testUUID", 0), true,
            RecoverySource.ExistingStoreRecoverySource.INSTANCE, unassignedInfo);
        IndexMetadata idx =
            IndexMetadata.builder("idx").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(0).build();

        RoutingNode routingNode = new RoutingNode("testNode", null);
        verify(deciders.canAllocate(shardRouting, routingNode, allocation), matcher);
        verify(deciders.canAllocate(idx, routingNode, allocation), matcher);
        verify(deciders.canAllocate(shardRouting, allocation), matcher);
        verify(deciders.canRebalance(shardRouting, allocation), matcher);
        verify(deciders.canRebalance(allocation), matcher);
        verify(deciders.canRemain(shardRouting, routingNode, allocation), matcher);
        verify(deciders.canForceAllocatePrimary(shardRouting, routingNode, allocation), matcher);
        verify(deciders.shouldAutoExpandToNode(idx, null, allocation), matcher);
    }

    private void verify(Decision decision, Consumer<Collection<? extends Decision>> matcher) {
        assertThat(decision.type()).isEqualTo(Decision.Type.YES);
        assertThat(decision).isExactlyInstanceOf(Decision.Multi.class);
        Decision.Multi multi = (Decision.Multi) decision;
        matcher.accept(multi.getDecisions());
    }
}
