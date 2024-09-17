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

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.EmptyClusterInfoService;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.snapshots.EmptySnapshotsInfoService;
import org.elasticsearch.test.gateway.TestGatewayAllocator;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static io.crate.testing.Asserts.assertThat;
import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;


public class FilterAllocationDeciderTests extends ESAllocationTestCase {

    @Test
    public void test_allocation_require_cluster_setting() {
        var clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        var settings = Settings.builder().put("cluster.routing.allocation.require.storage", "hot").build();

        var clusterState = buildClusterState(settings(Version.CURRENT));
        var allocationService = buildAllocationService(settings, clusterSettings);
        var routing =  allocationService.reroute(clusterState, "reroute").routingTable();

        var t1 = routing.index("t1");
        assertThat(t1.shards().size()).isEqualTo(3);

        var shardRouting = t1.shard(0).shards().get(0);
        assertThat(shardRouting.currentNodeId()).isEqualTo("node_hot_3");
        assertThat(shardRouting.state()).isEqualTo(INITIALIZING);

        shardRouting = t1.shard(1).shards().get(0);
        assertThat(shardRouting.currentNodeId()).isEqualTo("node_hot_2");
        assertThat(shardRouting.state()).isEqualTo(INITIALIZING);

        shardRouting = t1.shard(2).shards().get(0);
        assertThat(shardRouting.currentNodeId()).isEqualTo("node_hot_1");
        assertThat(shardRouting.state()).isEqualTo(INITIALIZING);
    }

    @Test
    public void test_allocation_require_override_cluster_setting_with_index_setting() {
        var clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        var settings = Settings.builder().put("cluster.routing.allocation.require.storage", "hot").build();
        var indexSetting = settings(Version.CURRENT).put("index.routing.allocation.require.storage", "cold");

        var clusterState = buildClusterState(indexSetting);
        var allocationService = buildAllocationService(settings, clusterSettings);
        var routing =  allocationService.reroute(clusterState, "reroute").routingTable();

        var t1 = routing.index("t1");
        assertThat(t1.shards().size()).isEqualTo(3);

        var shardRouting = t1.shard(0).shards().get(0);
        assertThat(shardRouting.currentNodeId()).isEqualTo("node_cold");
        assertThat(shardRouting.state()).isEqualTo(INITIALIZING);

        shardRouting = t1.shard(1).shards().get(0);
        assertThat(shardRouting.currentNodeId()).isEqualTo("node_cold");
        assertThat(shardRouting.state()).isEqualTo(INITIALIZING);

        shardRouting = t1.shard(2).shards().get(0);
        assertThat(shardRouting.currentNodeId()).isEqualTo("node_cold");
        assertThat(shardRouting.state()).isEqualTo(INITIALIZING);
    }

    @Test
    public void test_allocation_include_cluster_setting() {
        var clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        var settings = Settings.builder().put("cluster.routing.allocation.include.storage", "hot").build();

        var clusterState = buildClusterState(settings(Version.CURRENT));
        var allocationService = buildAllocationService(settings, clusterSettings);
        var routing =  allocationService.reroute(clusterState, "reroute").routingTable();

        var t1 = routing.index("t1");
        assertThat(t1.shards().size()).isEqualTo(3);

        var shardRouting = t1.shard(0).shards().get(0);
        assertThat(shardRouting.currentNodeId()).isEqualTo("node_hot_3");
        assertThat(shardRouting.state()).isEqualTo(INITIALIZING);

        shardRouting = t1.shard(1).shards().get(0);
        assertThat(shardRouting.currentNodeId()).isEqualTo("node_hot_2");
        assertThat(shardRouting.state()).isEqualTo(INITIALIZING);

        shardRouting = t1.shard(2).shards().get(0);
        assertThat(shardRouting.currentNodeId()).isEqualTo("node_hot_1");
        assertThat(shardRouting.state()).isEqualTo(INITIALIZING);
    }

    @Test
    public void test_allocation_include_override_cluster_setting_with_index_setting() {
        var clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        var settings = Settings.builder().put("cluster.routing.allocation.include.storage", "hot").build();
        var indexSetting = settings(Version.CURRENT).put("index.routing.allocation.include.storage", "cold");

        var clusterState = buildClusterState(indexSetting);
        var allocationService = buildAllocationService(settings, clusterSettings);
        var routing =  allocationService.reroute(clusterState, "reroute").routingTable();

        var t1 = routing.index("t1");
        assertThat(t1.shards().size()).isEqualTo(3);

        var shardRouting = t1.shard(0).shards().get(0);
        assertThat(shardRouting.currentNodeId()).isEqualTo("node_cold");
        assertThat(shardRouting.state()).isEqualTo(INITIALIZING);

        shardRouting = t1.shard(1).shards().get(0);
        assertThat(shardRouting.currentNodeId()).isEqualTo("node_cold");
        assertThat(shardRouting.state()).isEqualTo(INITIALIZING);

        shardRouting = t1.shard(2).shards().get(0);
        assertThat(shardRouting.currentNodeId()).isEqualTo("node_cold");
        assertThat(shardRouting.state()).isEqualTo(INITIALIZING);
    }

    @Test
    public void test_allocation_exclude_cluster_setting() {
        var clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        var settings = Settings.builder().put("cluster.routing.allocation.exclude.storage", "hot").build();

        var clusterState = buildClusterState(settings(Version.CURRENT));
        var allocationService = buildAllocationService(settings, clusterSettings);
        var routing =  allocationService.reroute(clusterState, "reroute").routingTable();

        var t1 = routing.index("t1");
        assertThat(t1.shards().size()).isEqualTo(3);

        var shardRouting = t1.shard(0).shards().get(0);
        assertThat(shardRouting.currentNodeId()).isEqualTo("node_cold");
        assertThat(shardRouting.state()).isEqualTo(INITIALIZING);

        shardRouting = t1.shard(1).shards().get(0);
        assertThat(shardRouting.currentNodeId()).isEqualTo("node_cold");
        assertThat(shardRouting.state()).isEqualTo(INITIALIZING);

        shardRouting = t1.shard(2).shards().get(0);
        assertThat(shardRouting.currentNodeId()).isEqualTo("node_cold");
        assertThat(shardRouting.state()).isEqualTo(INITIALIZING);
    }

    @Test
    public void test_allocation_exclude_cluster_setting_override_with_index_setting() {
        var clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        var settings = Settings.builder().put("cluster.routing.allocation.exclude.storage", "hot").build();
        var indexSetting = settings(Version.CURRENT).put("index.routing.allocation.exclude.storage", "cold");

        var clusterState = buildClusterState(indexSetting);
        var allocationService = buildAllocationService(settings, clusterSettings);
        var routing =  allocationService.reroute(clusterState, "reroute").routingTable();

        var t1 = routing.index("t1");
        assertThat(t1.shards().size()).isEqualTo(3);

        var shardRouting = t1.shard(0).shards().get(0);
        assertThat(shardRouting.currentNodeId()).isEqualTo("node_hot_3");
        assertThat(shardRouting.state()).isEqualTo(INITIALIZING);

        shardRouting = t1.shard(1).shards().get(0);
        assertThat(shardRouting.currentNodeId()).isEqualTo("node_hot_2");
        assertThat(shardRouting.state()).isEqualTo(INITIALIZING);

        shardRouting = t1.shard(2).shards().get(0);
        assertThat(shardRouting.currentNodeId()).isEqualTo("node_hot_1");
        assertThat(shardRouting.state()).isEqualTo(INITIALIZING);
    }


    private static ClusterState buildClusterState(Settings.Builder indexSettings) {
        Metadata.Builder metadata = Metadata.builder();
        metadata.persistentSettings(Settings.EMPTY);

        final IndexMetadata.Builder indexMetadataBuilder = IndexMetadata.builder("t1")
            .settings(indexSettings)
            .numberOfShards(3)
            .numberOfReplicas(0);

        final IndexMetadata indexMetadata = indexMetadataBuilder.build();
        metadata.put(indexMetadata, false);
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();
        routingTableBuilder.addAsNew(indexMetadata);

        RoutingTable routingTable = routingTableBuilder.build();
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).routingTable(routingTable).build();
        return ClusterState
            .builder(clusterState)
            .nodes(DiscoveryNodes
                .builder()
                .add(newNode("node_hot_1", Map.of("storage", "hot")))
                .add(newNode("node_hot_2", Map.of("storage", "hot")))
                .add(newNode("node_hot_3", Map.of("storage", "hot")))
                .add(newNode("node_cold", Map.of("storage", "cold")))
            )
            .build();
    }

    private static AllocationService buildAllocationService(Settings settings, ClusterSettings clusterSettings) {
        FilterAllocationDecider filterAllocationDecider = new FilterAllocationDecider(settings, clusterSettings);

        AllocationDeciders allocationDeciders = new AllocationDeciders(
            List.of(
                filterAllocationDecider,
                new SameShardAllocationDecider(Settings.EMPTY, clusterSettings),
                new ReplicaAfterPrimaryActiveAllocationDecider()
            )
        );

        return new AllocationService(
            allocationDeciders,
            new TestGatewayAllocator(),
            new BalancedShardsAllocator(Settings.EMPTY),
            EmptyClusterInfoService.INSTANCE,
            EmptySnapshotsInfoService.INSTANCE
        );
    }
}
