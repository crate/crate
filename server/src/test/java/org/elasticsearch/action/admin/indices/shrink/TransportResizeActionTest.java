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
import java.util.List;
import java.util.Set;

import org.apache.lucene.index.IndexWriter;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.create.CreateIndexClusterStateUpdateRequest;
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

import io.crate.execution.ddl.tables.AlterTableClient;
import io.crate.metadata.RelationName;

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
        RelationName tbl = new RelationName("doc", "source");
        String sourceIndex = tbl.indexNameOrAlias();
        String targetIndex = AlterTableClient.RESIZE_PREFIX + sourceIndex;
        int numShards = randomIntBetween(2, 42);
        ClusterState state = createClusterState("source", numShards, randomIntBetween(0, 10),
            Settings.builder().put("index.blocks.write", true).build());
        assertThatThrownBy(() -> TransportResizeAction.prepareCreateIndexRequest(
                new ResizeRequest(tbl, List.of(), 1),
                state,
                (i) -> new DocsStats(Integer.MAX_VALUE, between(1, 1000), between(1, 100)),
                sourceIndex,
                targetIndex))
            .isExactlyInstanceOf(IllegalStateException.class)
            .hasMessageStartingWith("Can't merge index with more than [2147483519] docs - too many documents in shards ");


        ResizeRequest req = new ResizeRequest(tbl, List.of(), 4);
        final ClusterState cs = createClusterState("source", 8, 1,
            Settings.builder().put("index.blocks.write", true).build());
        assertThatThrownBy(() -> TransportResizeAction.prepareCreateIndexRequest(req, cs,
                        (i) -> i == 2 || i == 3 ? new DocsStats(Integer.MAX_VALUE / 2, between(1, 1000), between(1, 10000)) : null,
                        sourceIndex, targetIndex))
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

        TransportResizeAction.prepareCreateIndexRequest(new ResizeRequest(tbl, List.of(), 1), clusterState,
            (i) -> new DocsStats(between(1, 1000), between(1, 1000), between(0, 10000)), sourceIndex, targetIndex);
    }

    @Test
    public void testShrinkIndexSettings() {
        RelationName name = new RelationName("src", "tbl");
        String indexName = name.indexNameOrAlias();

        // create one that won't fail
        int numShards = randomIntBetween(2, 10);
        ClusterState clusterState = ClusterState.builder(createClusterState(indexName, numShards, 0,
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
        routingTable = service.applyStartedShards(
            clusterState,
            routingTable.index(indexName).shardsWithState(ShardRoutingState.INITIALIZING)
        ).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        int numSourceShards = clusterState.metadata().index(indexName).getNumberOfShards();
        DocsStats stats = new DocsStats(between(0, (IndexWriter.MAX_DOCS) / numSourceShards), between(1, 1000), between(1, 10000));
        ResizeRequest target = new ResizeRequest(name, List.of(), 1);
        CreateIndexClusterStateUpdateRequest request = TransportResizeAction.prepareCreateIndexRequest(
            target, clusterState, (i) -> stats, indexName, "target");
        assertThat(request.recoverFrom()).isNotNull();
        assertThat(request.recoverFrom().getName()).isEqualTo(indexName);
        assertThat(request.settings().get("index.number_of_shards")).isEqualTo("1");
        assertThat(request.cause()).isEqualTo("resize_table");
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
