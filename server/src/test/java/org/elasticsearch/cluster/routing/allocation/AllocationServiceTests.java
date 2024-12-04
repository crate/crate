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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.IntStream;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.gateway.GatewayAllocator;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.gateway.TestGatewayAllocator;
import org.junit.Test;

public class AllocationServiceTests extends ESTestCase {

    @Test
    public void testFirstListElementsToCommaDelimitedStringReportsAllElementsIfShort() {
        List<String> strings = IntStream.range(0, between(0, 10)).mapToObj(i -> randomAlphaOfLength(10)).toList();
        assertAllElementsReported(strings, randomBoolean());
    }

    @Test
    public void testFirstListElementsToCommaDelimitedStringReportsAllElementsIfDebugEnabled() {
        List<String> strings = IntStream.range(0, between(0, 100)).mapToObj(i -> randomAlphaOfLength(10)).toList();
        assertAllElementsReported(strings, true);
    }

    private void assertAllElementsReported(List<String> strings, boolean isDebugEnabled) {
        final String abbreviated = AllocationService.firstListElementsToCommaDelimitedString(strings, Function.identity(), isDebugEnabled);
        for (String string : strings) {
            assertThat(abbreviated).contains(string);
        }
        assertThat(abbreviated).doesNotContain("...");
    }

    @Test
    public void testFirstListElementsToCommaDelimitedStringReportsFirstElementsIfLong() {
        List<String> strings = IntStream.range(0, between(11, 100)).mapToObj(i -> randomAlphaOfLength(10))
            .distinct().toList();
        final String abbreviated = AllocationService.firstListElementsToCommaDelimitedString(strings, Function.identity(), false);
        for (int i = 0; i < strings.size(); i++) {
            if (i < 10) {
                assertThat(abbreviated).contains(strings.get(i));
            } else {
                assertThat(abbreviated).doesNotContain(strings.get(i));
            }
        }
        assertThat(abbreviated)
            .contains("...")
            .contains("[" + strings.size() + " items in total]");
    }

    @Test
    public void testFirstListElementsToCommaDelimitedStringUsesFormatterNotToString() {
        List<String> strings = IntStream.range(0, between(1, 100)).mapToObj(i -> "original").toList();
        final String abbreviated = AllocationService.firstListElementsToCommaDelimitedString(strings, s -> "formatted", randomBoolean());
        assertThat(abbreviated)
            .contains("formatted")
            .doesNotContain("original");
    }

    @Test
    public void testExplainsNonAllocationOfShardWithUnknownAllocator() {
        final AllocationService allocationService = new AllocationService(null, null, null, null);
        allocationService.setExistingShardsAllocators(
            Collections.singletonMap(GatewayAllocator.ALLOCATOR_NAME, new TestGatewayAllocator()));

        final DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder();
        nodesBuilder.add(new DiscoveryNode("node1", buildNewFakeTransportAddress(), Version.CURRENT));
        nodesBuilder.add(new DiscoveryNode("node2", buildNewFakeTransportAddress(), Version.CURRENT));

        final Metadata.Builder metadata = Metadata.builder().put(indexMetadata("index", Settings.builder()
            .put(ExistingShardsAllocator.EXISTING_SHARDS_ALLOCATOR_SETTING.getKey(), "unknown")));

        final RoutingTable.Builder routingTableBuilder = RoutingTable.builder().addAsRecovery(metadata.get("index"));

        final ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(nodesBuilder)
            .metadata(metadata)
            .routingTable(routingTableBuilder.build())
            .build();

        final RoutingAllocation allocation = new RoutingAllocation(new AllocationDeciders(Collections.emptyList()),
            clusterState.getRoutingNodes(), clusterState, ClusterInfo.EMPTY, null,0L);
        allocation.setDebugMode(randomBoolean() ? RoutingAllocation.DebugMode.ON : RoutingAllocation.DebugMode.EXCLUDE_YES_DECISIONS);

        final ShardAllocationDecision shardAllocationDecision
            = allocationService.explainShardAllocation(clusterState.routingTable().index("index").shard(0).primaryShard(), allocation);

        assertThat(shardAllocationDecision.isDecisionTaken()).isTrue();
        assertThat(shardAllocationDecision.getAllocateDecision().getAllocationStatus()).isEqualTo(UnassignedInfo.AllocationStatus.NO_VALID_SHARD_COPY);
        assertThat(shardAllocationDecision.getAllocateDecision().getAllocationDecision()).isEqualTo(AllocationDecision.NO_VALID_SHARD_COPY);
        assertThat(shardAllocationDecision.getAllocateDecision().getExplanation()).isEqualTo("cannot allocate because a previous copy of " +
            "the primary shard existed but can no longer be found on the nodes in the cluster");

        for (NodeAllocationResult nodeAllocationResult : shardAllocationDecision.getAllocateDecision().nodeDecisions) {
            assertThat(nodeAllocationResult.getNodeDecision()).isEqualTo(AllocationDecision.NO);
            assertThat(nodeAllocationResult.getCanAllocateDecision().type()).isEqualTo(Decision.Type.NO);
            assertThat(nodeAllocationResult.getCanAllocateDecision().label()).isEqualTo("allocator_plugin");
            assertThat(nodeAllocationResult.getCanAllocateDecision().getExplanation()).isEqualTo("finding the previous copies of this " +
                "shard requires an allocator called [unknown] but that allocator was not found; perhaps the corresponding plugin is " +
                "not installed");
        }
    }

    private static final String FAKE_IN_SYNC_ALLOCATION_ID = "_in_sync_"; // so we can allocate primaries anywhere

    private static IndexMetadata.Builder indexMetadata(String name, Settings.Builder settings) {
        return IndexMetadata.builder(name)
            .settings(settings(Version.CURRENT).put(settings.build()))
            .numberOfShards(2).numberOfReplicas(1)
            .putInSyncAllocationIds(0, Collections.singleton(FAKE_IN_SYNC_ALLOCATION_ID))
            .putInSyncAllocationIds(1, Collections.singleton(FAKE_IN_SYNC_ALLOCATION_ID));
    }
}
