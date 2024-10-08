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

package org.elasticsearch.action.admin.indices.shrink;

import static java.util.Collections.emptyMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Collections;
import java.util.Set;

import org.apache.lucene.index.IndexWriter;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.create.CreateIndexClusterStateUpdateRequest;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.EmptyClusterInfoService;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.routing.allocation.decider.MaxRetryAllocationDecider;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.DocsStats;
import org.elasticsearch.snapshots.EmptySnapshotsInfoService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.gateway.TestGatewayAllocator;
import org.junit.Test;

public class TransportResizeActionTest extends ESTestCase {

    private ClusterState createClusterState(String name, int numShards, int numReplicas, Settings settings) {
        return createClusterState(name, numShards, numReplicas, numShards, settings);
    }

    private ClusterState createClusterState(String name, int numShards, int numReplicas, int numRoutingShards, Settings settings) {
        Metadata.Builder metaBuilder = Metadata.builder();
        IndexMetadata indexMetadata = IndexMetadata.builder(name).settings(settings(Version.CURRENT)
            .put(settings))
            .numberOfShards(numShards).numberOfReplicas(numReplicas).setRoutingNumShards(numRoutingShards).build();
        metaBuilder.put(indexMetadata, false);
        Metadata metadata = metaBuilder.build();
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();
        routingTableBuilder.addAsNew(metadata.index(name));

        RoutingTable routingTable = routingTableBuilder.build();
        ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata).routingTable(routingTable).blocks(ClusterBlocks.builder().addBlocks(indexMetadata)).build();
        return clusterState;
    }

    @Test
    public void testErrorCondition() {
        ClusterState state = createClusterState("source", randomIntBetween(2, 42), randomIntBetween(0, 10),
            Settings.builder().put("index.blocks.write", true).build());
        assertThatThrownBy(() -> TransportResizeAction.prepareCreateIndexRequest(new ResizeRequest("target", "source"),
                state,
                (i) -> new DocsStats(Integer.MAX_VALUE, between(1, 1000), between(1, 100)), "source", "target"))
            .isExactlyInstanceOf(IllegalStateException.class)
            .hasMessageStartingWith("Can't merge index with more than [2147483519] docs - too many documents in shards ");


        ResizeRequest req = new ResizeRequest("target", "source");
        req.getTargetIndexRequest().settings(Settings.builder().put("index.number_of_shards", 4).build());
        final ClusterState cs = createClusterState("source", 8, 1,
            Settings.builder().put("index.blocks.write", true).build());
        assertThatThrownBy(() -> TransportResizeAction.prepareCreateIndexRequest(req, cs,
                        (i) -> i == 2 || i == 3 ? new DocsStats(Integer.MAX_VALUE / 2, between(1, 1000), between(1, 10000)) : null,
                        "source", "target"))
            .isExactlyInstanceOf(IllegalStateException.class)
            .hasMessageStartingWith("Can't merge index with more than [2147483519] docs - too many documents in shards ");

        // create one that won't fail
        ClusterState clusterState = ClusterState.builder(createClusterState("source", randomIntBetween(2, 10), 0,
            Settings.builder().put("index.blocks.write", true).build())).nodes(DiscoveryNodes.builder().add(newNode("node1")))
            .build();
        AllocationService service = new AllocationService(
            new AllocationDeciders(Collections.singleton(new MaxRetryAllocationDecider())),
            new TestGatewayAllocator(),
            new BalancedShardsAllocator(Settings.EMPTY),
            EmptyClusterInfoService.INSTANCE,
            EmptySnapshotsInfoService.INSTANCE);

        RoutingTable routingTable = service.reroute(clusterState, "reroute").routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        // now we start the shard
        routingTable = service.applyStartedShards(clusterState,
            routingTable.index("source").shardsWithState(ShardRoutingState.INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        TransportResizeAction.prepareCreateIndexRequest(new ResizeRequest("target", "source"), clusterState,
            (i) -> new DocsStats(between(1, 1000), between(1, 1000), between(0, 10000)), "source", "target");
    }

    @Test
    public void testPassNumRoutingShards() {
        ClusterState clusterState = ClusterState.builder(createClusterState("source", 1, 0,
            Settings.builder().put("index.blocks.write", true).build())).nodes(DiscoveryNodes.builder().add(newNode("node1")))
            .build();
        AllocationService service = new AllocationService(
            new AllocationDeciders(Collections.singleton(new MaxRetryAllocationDecider())),
            new TestGatewayAllocator(),
            new BalancedShardsAllocator(Settings.EMPTY),
            EmptyClusterInfoService.INSTANCE,
            EmptySnapshotsInfoService.INSTANCE);

        RoutingTable routingTable = service.reroute(clusterState, "reroute").routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        // now we start the shard
        routingTable = service.applyStartedShards(clusterState,
            routingTable.index("source").shardsWithState(ShardRoutingState.INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        ResizeRequest resizeRequest = new ResizeRequest("target", "source");
        resizeRequest.setResizeType(ResizeType.SPLIT);
        resizeRequest.getTargetIndexRequest()
            .settings(Settings.builder().put("index.number_of_shards", 2).build());
        TransportResizeAction.prepareCreateIndexRequest(resizeRequest, clusterState, null, "source", "target");

        resizeRequest.getTargetIndexRequest()
            .settings(Settings.builder()
                .put("index.number_of_routing_shards", randomIntBetween(2, 10))
                .put("index.number_of_shards", 2)
                .build());
        TransportResizeAction.prepareCreateIndexRequest(resizeRequest, clusterState, null, "source", "target");
    }

    @Test
    public void testPassNumRoutingShardsAndFail() {
        int numShards = randomIntBetween(2, 100);
        ClusterState clusterState = ClusterState.builder(createClusterState("source", numShards, 0, numShards * 4,
            Settings.builder().put("index.blocks.write", true).build())).nodes(DiscoveryNodes.builder().add(newNode("node1")))
            .build();
        AllocationService service = new AllocationService(
            new AllocationDeciders(Collections.singleton(new MaxRetryAllocationDecider())),
            new TestGatewayAllocator(),
            new BalancedShardsAllocator(Settings.EMPTY),
            EmptyClusterInfoService.INSTANCE,
            EmptySnapshotsInfoService.INSTANCE);

        RoutingTable routingTable = service.reroute(clusterState, "reroute").routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        // now we start the shard
        routingTable = service.applyStartedShards(clusterState,
            routingTable.index("source").shardsWithState(ShardRoutingState.INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        ResizeRequest resizeRequest = new ResizeRequest("target", "source");
        resizeRequest.setResizeType(ResizeType.SPLIT);
        resizeRequest.getTargetIndexRequest()
            .settings(Settings.builder().put("index.number_of_shards", numShards * 2).build());
        TransportResizeAction.prepareCreateIndexRequest(resizeRequest, clusterState, null, "source", "target");

        resizeRequest.getTargetIndexRequest()
            .settings(Settings.builder()
                .put("index.number_of_shards", numShards * 2)
                .put("index.number_of_routing_shards", numShards * 2).build());
        ClusterState finalState = clusterState;
        assertThatThrownBy(() -> TransportResizeAction.prepareCreateIndexRequest(
            resizeRequest, finalState, null, "source", "target"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("cannot provide index.number_of_routing_shards on resize");
    }

    public void testShrinkIndexSettings() {
        String indexName = randomAlphaOfLength(10);
        // create one that won't fail
        ClusterState clusterState = ClusterState.builder(createClusterState(indexName, randomIntBetween(2, 10), 0,
            Settings.builder()
                .put("index.blocks.write", true)
                .build())).nodes(DiscoveryNodes.builder().add(newNode("node1")))
            .build();
        AllocationService service = new AllocationService(
            new AllocationDeciders(Collections.singleton(new MaxRetryAllocationDecider())),
            new TestGatewayAllocator(),
            new BalancedShardsAllocator(Settings.EMPTY),
            EmptyClusterInfoService.INSTANCE,
            EmptySnapshotsInfoService.INSTANCE);

        RoutingTable routingTable = service.reroute(clusterState, "reroute").routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        // now we start the shard
        routingTable = service.applyStartedShards(clusterState,
            routingTable.index(indexName).shardsWithState(ShardRoutingState.INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        int numSourceShards = clusterState.metadata().index(indexName).getNumberOfShards();
        DocsStats stats = new DocsStats(between(0, (IndexWriter.MAX_DOCS) / numSourceShards), between(1, 1000), between(1, 10000));
        ResizeRequest target = new ResizeRequest("target", indexName);
        final ActiveShardCount activeShardCount = randomBoolean() ? ActiveShardCount.ALL : ActiveShardCount.ONE;
        target.setWaitForActiveShards(activeShardCount);
        CreateIndexClusterStateUpdateRequest request = TransportResizeAction.prepareCreateIndexRequest(
            target, clusterState, (i) -> stats, indexName, "target");
        assertThat(request.recoverFrom()).isNotNull();
        assertThat(request.recoverFrom().getName()).isEqualTo(indexName);
        assertThat(request.settings().get("index.number_of_shards")).isEqualTo("1");
        assertThat(request.cause()).isEqualTo("shrink_index");
        assertThat(request.waitForActiveShards()).isEqualTo(activeShardCount);
    }

    private DiscoveryNode newNode(String nodeId) {
        return new DiscoveryNode(
            nodeId,
            buildNewFakeTransportAddress(),
            emptyMap(),
            Set.of(DiscoveryNodeRole.MASTER_ROLE, DiscoveryNodeRole.DATA_ROLE),
            Version.CURRENT);
    }
}
