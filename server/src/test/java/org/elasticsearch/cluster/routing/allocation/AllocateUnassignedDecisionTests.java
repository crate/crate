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

package org.elasticsearch.cluster.routing.allocation;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.UnassignedInfo.AllocationStatus;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

/**
 * Unit tests for the {@link AllocateUnassignedDecision} class.
 */
public class AllocateUnassignedDecisionTests extends ESTestCase {

    private DiscoveryNode node1 = new DiscoveryNode("node1", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);
    private DiscoveryNode node2 = new DiscoveryNode("node2", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);

    @Test
    public void testDecisionNotTaken() {
        AllocateUnassignedDecision allocateUnassignedDecision = AllocateUnassignedDecision.NOT_TAKEN;
        assertThat(allocateUnassignedDecision.isDecisionTaken()).isFalse();
        assertThatThrownBy(() -> allocateUnassignedDecision.getAllocationDecision())
            .isExactlyInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> allocateUnassignedDecision.getAllocationStatus())
            .isExactlyInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> allocateUnassignedDecision.getAllocationId())
            .isExactlyInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> allocateUnassignedDecision.getTargetNode())
            .isExactlyInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> allocateUnassignedDecision.getNodeDecisions())
            .isExactlyInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> allocateUnassignedDecision.getExplanation())
            .isExactlyInstanceOf(IllegalStateException.class);
    }

    @Test
    public void testNoDecision() {
        final AllocationStatus allocationStatus = randomFrom(
            AllocationStatus.DELAYED_ALLOCATION, AllocationStatus.NO_VALID_SHARD_COPY, AllocationStatus.FETCHING_SHARD_DATA
        );
        AllocateUnassignedDecision noDecision = AllocateUnassignedDecision.no(allocationStatus, null);
        assertThat(noDecision.isDecisionTaken()).isTrue();
        assertThat(noDecision.getAllocationDecision()).isEqualTo(AllocationDecision.fromAllocationStatus(allocationStatus));
        assertThat(noDecision.getAllocationStatus()).isEqualTo(allocationStatus);
        if (allocationStatus == AllocationStatus.FETCHING_SHARD_DATA) {
            assertThat(noDecision.getExplanation()).isEqualTo("cannot allocate because information about existing shard data is still being retrieved from " +
                         "some of the nodes");
        } else if (allocationStatus == AllocationStatus.DELAYED_ALLOCATION) {
            assertThat(noDecision.getExplanation()).startsWith("cannot allocate because the cluster is still waiting");
        } else {
            assertThat(noDecision.getExplanation())
                .startsWith("cannot allocate because a previous copy of the primary shard existed");
        }
        assertThat(noDecision.getNodeDecisions()).isNull();
        assertThat(noDecision.getTargetNode()).isNull();
        assertThat(noDecision.getAllocationId()).isNull();

        List<NodeAllocationResult> nodeDecisions = new ArrayList<>();
        nodeDecisions.add(new NodeAllocationResult(node1, Decision.NO, 1));
        nodeDecisions.add(new NodeAllocationResult(node2, Decision.NO, 2));
        final boolean reuseStore = randomBoolean();
        noDecision = AllocateUnassignedDecision.no(AllocationStatus.DECIDERS_NO, nodeDecisions, reuseStore);
        assertThat(noDecision.isDecisionTaken()).isTrue();
        assertThat(noDecision.getAllocationDecision()).isEqualTo(AllocationDecision.NO);
        assertThat(noDecision.getAllocationStatus()).isEqualTo(AllocationStatus.DECIDERS_NO);
        if (reuseStore) {
            assertThat(noDecision.getExplanation()).isEqualTo("cannot allocate because allocation is not permitted to any of the nodes that hold an in-sync shard copy");
        } else {
            assertThat(noDecision.getExplanation()).isEqualTo("cannot allocate because allocation is not permitted to any of the nodes");
        }
        assertThat(noDecision.getNodeDecisions()).isEqualTo(nodeDecisions.stream().sorted().toList());
        // node1 should be sorted first b/c of better weight ranking
        assertThat(noDecision.getNodeDecisions().getFirst().getNode().getId()).isEqualTo("node1");
        assertThat(noDecision.getTargetNode()).isNull();
        assertThat(noDecision.getAllocationId()).isNull();

        // test bad values
        assertThatThrownBy(() -> AllocateUnassignedDecision.no(null, null))
            .isExactlyInstanceOf(NullPointerException.class);
    }

    @Test
    public void testThrottleDecision() {
        List<NodeAllocationResult> nodeDecisions = new ArrayList<>();
        nodeDecisions.add(new NodeAllocationResult(node1, Decision.NO, 1));
        nodeDecisions.add(new NodeAllocationResult(node2, Decision.THROTTLE, 2));
        AllocateUnassignedDecision throttleDecision = AllocateUnassignedDecision.throttle(nodeDecisions);
        assertThat(throttleDecision.isDecisionTaken()).isTrue();
        assertThat(throttleDecision.getAllocationDecision()).isEqualTo(AllocationDecision.THROTTLED);
        assertThat(throttleDecision.getAllocationStatus()).isEqualTo(AllocationStatus.DECIDERS_THROTTLED);
        assertThat(throttleDecision.getExplanation()).startsWith("allocation temporarily throttled");
        assertThat(throttleDecision.getNodeDecisions()).isEqualTo(nodeDecisions.stream().sorted().toList());
        // node2 should be sorted first b/c a THROTTLE is higher than a NO decision
        assertThat(throttleDecision.getNodeDecisions().getFirst().getNode().getId()).isEqualTo("node2");
        assertThat(throttleDecision.getTargetNode()).isNull();
        assertThat(throttleDecision.getAllocationId()).isNull();
    }

    @Test
    public void testYesDecision() {
        List<NodeAllocationResult> nodeDecisions = new ArrayList<>();
        nodeDecisions.add(new NodeAllocationResult(node1, Decision.NO, 1));
        nodeDecisions.add(new NodeAllocationResult(node2, Decision.YES, 2));
        String allocId = randomBoolean() ? "allocId" : null;
        AllocateUnassignedDecision yesDecision = AllocateUnassignedDecision.yes(
            node2, allocId, nodeDecisions, randomBoolean());
        assertThat(yesDecision.isDecisionTaken()).isTrue();
        assertThat(yesDecision.getAllocationDecision()).isEqualTo(AllocationDecision.YES);
        assertThat(yesDecision.getAllocationStatus()).isNull();
        assertThat(yesDecision.getExplanation()).isEqualTo("can allocate the shard");
        assertThat(yesDecision.getNodeDecisions()).isEqualTo(nodeDecisions.stream().sorted().toList());
        assertThat(yesDecision.getTargetNode().getId()).isEqualTo("node2");
        assertThat(yesDecision.getAllocationId()).isEqualTo(allocId);
        // node1 should be sorted first b/c YES decisions are the highest
        assertThat(yesDecision.getNodeDecisions().getFirst().getNode().getId()).isEqualTo("node2");
    }

    @Test
    public void testCachedDecisions() {
        List<AllocationStatus> cacheableStatuses = Arrays.asList(AllocationStatus.DECIDERS_NO, AllocationStatus.DECIDERS_THROTTLED,
                                                                 AllocationStatus.NO_VALID_SHARD_COPY, AllocationStatus.FETCHING_SHARD_DATA, AllocationStatus.DELAYED_ALLOCATION);
        for (AllocationStatus allocationStatus : cacheableStatuses) {
            if (allocationStatus == AllocationStatus.DECIDERS_THROTTLED) {
                AllocateUnassignedDecision cached = AllocateUnassignedDecision.throttle(null);
                AllocateUnassignedDecision another = AllocateUnassignedDecision.throttle(null);
                assertThat(another).isSameAs(cached);
                AllocateUnassignedDecision notCached = AllocateUnassignedDecision.throttle(new ArrayList<>());
                another = AllocateUnassignedDecision.throttle(new ArrayList<>());
                assertThat(another).isNotSameAs(notCached);
            } else {
                AllocateUnassignedDecision cached = AllocateUnassignedDecision.no(allocationStatus, null);
                AllocateUnassignedDecision another = AllocateUnassignedDecision.no(allocationStatus, null);
                assertThat(another).isSameAs(cached);
                AllocateUnassignedDecision notCached = AllocateUnassignedDecision.no(allocationStatus, new ArrayList<>());
                another = AllocateUnassignedDecision.no(allocationStatus, new ArrayList<>());
                assertThat(another).isNotSameAs(notCached);
            }
        }

        // yes decisions are not precomputed and cached
        AllocateUnassignedDecision first = AllocateUnassignedDecision.yes(node1, "abc", emptyList(), randomBoolean());
        AllocateUnassignedDecision second = AllocateUnassignedDecision.yes(node1, "abc", emptyList(), randomBoolean());
        // same fields for the ShardAllocationDecision, but should be different instances
        assertThat(second).isNotSameAs(first);
    }

    @Test
    public void testSerialization() throws IOException {
        DiscoveryNode node1 = new DiscoveryNode("node1", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);
        DiscoveryNode node2 = new DiscoveryNode("node2", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);
        Decision.Type finalDecision = randomFrom(Decision.Type.values());
        DiscoveryNode assignedNode = finalDecision == Decision.Type.YES ? node1 : null;
        List<NodeAllocationResult> nodeDecisions = new ArrayList<>();
        nodeDecisions.add(new NodeAllocationResult(node1, Decision.NO, 2));
        nodeDecisions.add(new NodeAllocationResult(node2, finalDecision == Decision.Type.YES ? Decision.YES :
            randomFrom(Decision.NO, Decision.THROTTLE, Decision.YES), 1));
        AllocateUnassignedDecision decision;
        if (finalDecision == Decision.Type.YES) {
            decision = AllocateUnassignedDecision.yes(assignedNode, randomBoolean() ? randomAlphaOfLength(5) : null,
                                                      nodeDecisions, randomBoolean());
        } else {
            decision = AllocateUnassignedDecision.no(randomFrom(
                AllocationStatus.DELAYED_ALLOCATION, AllocationStatus.NO_VALID_SHARD_COPY, AllocationStatus.FETCHING_SHARD_DATA
            ), nodeDecisions, randomBoolean());
        }
        BytesStreamOutput output = new BytesStreamOutput();
        decision.writeTo(output);
        AllocateUnassignedDecision readDecision = new AllocateUnassignedDecision(output.bytes().streamInput());
        assertThat(readDecision.getTargetNode()).isEqualTo(decision.getTargetNode());
        assertThat(readDecision.getAllocationStatus()).isEqualTo(decision.getAllocationStatus());
        assertThat(readDecision.getExplanation()).isEqualTo(decision.getExplanation());
        assertThat(readDecision.getNodeDecisions().size()).isEqualTo(decision.getNodeDecisions().size());
        assertThat(readDecision.getAllocationId()).isEqualTo(decision.getAllocationId());
        assertThat(readDecision.getAllocationDecision()).isEqualTo(decision.getAllocationDecision());
        // node2 should have the highest sort order
        assertThat(readDecision.getNodeDecisions().getFirst().getNode().getId()).isEqualTo("node2");
    }
}
