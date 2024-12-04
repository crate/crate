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

package io.crate.cluster.gracefulstop;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.ShardId;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import io.crate.test.integration.CrateDummyClusterServiceUnitTest;

public class DecommissionAllocationDeciderTest extends CrateDummyClusterServiceUnitTest {

    private RoutingAllocation routingAllocation;
    private ShardRouting primaryShard;
    private ShardRouting replicaShard;
    private RoutingNode n1;
    private RoutingNode n2;

    @Before
    public void init() throws Exception {
        routingAllocation = mock(RoutingAllocation.class);
        when(routingAllocation.decision(any(Decision.class), anyString(), anyString())).then(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                return invocation.getArguments()[0];
            }
        });

        primaryShard = ShardRouting.newUnassigned(
            new ShardId("t", UUIDs.randomBase64UUID(), 0),
            true,
            RecoverySource.PeerRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "dummy"));
        replicaShard = ShardRouting.newUnassigned(
            new ShardId("t", UUIDs.randomBase64UUID(), 0),
            false,
            RecoverySource.PeerRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "dummy")
        );
        n1 = new RoutingNode("n1", mock(DiscoveryNode.class));
        n2 = new RoutingNode("n2", mock(DiscoveryNode.class));
    }

    @Test
    public void testShouldNotBeAbleToAllocatePrimaryOntoDecommissionedNode() throws Exception {
        Settings settings = Settings.builder()
            .put(DecommissioningService.DECOMMISSION_PREFIX + "n1", true).build();

        DecommissionAllocationDecider allocationDecider =
            new DecommissionAllocationDecider(settings, clusterService.getClusterSettings());

        Decision decision = allocationDecider.canAllocate(primaryShard, n1, routingAllocation);
        assertThat(decision.type()).isEqualTo(Decision.Type.NO);

        decision = allocationDecider.canAllocate(primaryShard, n2, routingAllocation);
        assertThat(decision.type()).isEqualTo(Decision.Type.YES);
    }

    @Test
    public void testCanAlwaysAllocateIfDataAvailabilityIsNone() throws Exception {
        Settings settings = Settings.builder()
            .put(DecommissioningService.GRACEFUL_STOP_MIN_AVAILABILITY_SETTING.getKey(), "none")
            .put(DecommissioningService.DECOMMISSION_PREFIX + "n1", true).build();
        DecommissionAllocationDecider allocationDecider =
            new DecommissionAllocationDecider(settings, clusterService.getClusterSettings());

        Decision decision = allocationDecider.canAllocate(primaryShard, n1, routingAllocation);
        assertThat(decision.type()).isEqualTo(Decision.Type.YES);

        decision = allocationDecider.canAllocate(primaryShard, n2, routingAllocation);
        assertThat(decision.type()).isEqualTo(Decision.Type.YES);
    }

    @Test
    public void testReplicasCanRemainButCannotAllocateOnDecommissionedNodeWithPrimariesDataAvailability() throws Exception {
        Settings settings = Settings.builder()
            .put(DecommissioningService.GRACEFUL_STOP_MIN_AVAILABILITY_SETTING.getKey(), "primaries")
            .put(DecommissioningService.DECOMMISSION_PREFIX + "n1", true).build();

        DecommissionAllocationDecider allocationDecider =
            new DecommissionAllocationDecider(settings, clusterService.getClusterSettings());

        Decision decision = allocationDecider.canAllocate(replicaShard, n1, routingAllocation);
        assertThat(decision.type()).isEqualTo(Decision.Type.NO);

        decision = allocationDecider.canRemain(replicaShard, n1, routingAllocation);
        assertThat(decision.type()).isEqualTo(Decision.Type.YES);
    }

    @Test
    public void testCannotAllocatePrimaryOrReplicaIfDataAvailabilityIsFull() throws Exception {
        Settings settings = Settings.builder()
            .put(DecommissioningService.GRACEFUL_STOP_MIN_AVAILABILITY_SETTING.getKey(), "full")
            .put(DecommissioningService.DECOMMISSION_PREFIX + "n1", true).build();

        DecommissionAllocationDecider allocationDecider =
            new DecommissionAllocationDecider(settings, clusterService.getClusterSettings());

        Decision decision = allocationDecider.canAllocate(replicaShard, n1, routingAllocation);
        assertThat(decision.type()).isEqualTo(Decision.Type.NO);
        decision = allocationDecider.canRemain(replicaShard, n1, routingAllocation);
        assertThat(decision.type()).isEqualTo(Decision.Type.NO);

        decision = allocationDecider.canAllocate(primaryShard, n1, routingAllocation);
        assertThat(decision.type()).isEqualTo(Decision.Type.NO);
        decision = allocationDecider.canRemain(primaryShard, n1, routingAllocation);
        assertThat(decision.type()).isEqualTo(Decision.Type.NO);
    }

    @Test
    public void testDecommissionSettingsAreUpdated() {
        DecommissionAllocationDecider allocationDecider =
            new DecommissionAllocationDecider(Settings.EMPTY, clusterService.getClusterSettings());

        Settings settings = Settings.builder()
            .put(DecommissioningService.GRACEFUL_STOP_MIN_AVAILABILITY_SETTING.getKey(), "full")
            .put(DecommissioningService.DECOMMISSION_PREFIX + "n1", true).build();
        clusterService.getClusterSettings().applySettings(settings);

        Decision decision = allocationDecider.canAllocate(primaryShard, n1, routingAllocation);
        assertThat(decision.type()).isEqualTo(Decision.Type.NO);
    }
}
