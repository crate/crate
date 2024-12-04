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

package io.crate.beans;

import static org.assertj.core.api.Assertions.assertThat;
import static org.elasticsearch.test.ESTestCase.buildNewFakeTransportAddress;
import static org.elasticsearch.test.ESTestCase.settings;

import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.assertj.core.api.ThrowingConsumer;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.carrotsearch.randomizedtesting.RandomizedRunner;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import io.crate.common.collections.Tuple;

@RunWith(RandomizedRunner.class)
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class NodeInfoTest {

    ClusterState.Builder clusterState;

    @Before
    public void setup() {
        DiscoveryNode.setPossibleRoles(DiscoveryNodeRole.BUILT_IN_ROLES);
        var tableName = "test";
        var indexRoutingTableBuilder = IndexRoutingTable
            .builder(new Index(tableName, UUID.randomUUID().toString()))
            .addShard(TestShardRouting.newShardRouting(tableName,
                                                       1,
                                                       "node_1",
                                                       true,
                                                       ShardRoutingState.STARTED))
            .addShard(TestShardRouting.newShardRouting(tableName,
                                                       2,
                                                       "node_1",
                                                       false,
                                                       ShardRoutingState.STARTED))
            .addShard(TestShardRouting.newShardRouting(tableName,
                                                       3,
                                                       "node_1",
                                                       false,
                                                       ShardRoutingState.STARTED))
            .addShard(TestShardRouting.newShardRouting(tableName,
                                                       4,
                                                       null,
                                                       false,
                                                       ShardRoutingState.UNASSIGNED));

        var routingTable = RoutingTable.builder().add(indexRoutingTableBuilder).build();
        var meta = IndexMetadata.builder(tableName).settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(2);
        this.clusterState = ClusterState.builder(new ClusterName("crate")).version(1L).routingTable(routingTable)
            .metadata(Metadata.builder().put(meta));
    }

    @Test
    public void test_local_node_is_master_all_shards_locally() {
        var nodes = DiscoveryNodes
            .builder()
            .add(discoveryNode("node_1"))
            .masterNodeId("node_1")
            .localNodeId("node_1")
            .build();

        var nodeInfo = new NodeInfo(() -> clusterState.nodes(nodes).build(), this::shardStateAndSizeProvider);

        assertThat(nodeInfo.getNodeId()).isEqualTo("node_1");
        assertThat(nodeInfo.getNodeName()).isEqualTo("node_1");

        assertThat(nodeInfo.getClusterStateVersion()).isEqualTo(1L);
        ShardStats shardStats = nodeInfo.getShardStats();
        assertThat(shardStats.getPrimaries()).isEqualTo(1);
        assertThat(shardStats.getTotal()).isEqualTo(3);
        assertThat(shardStats.getReplicas()).isEqualTo(2);
        // Unassigned shards are counted on the master node
        assertThat(shardStats.getUnassigned()).isEqualTo(1);

        assertThat(nodeInfo.getShardInfo())
            .satisfiesExactlyInAnyOrder(
                isShardInfo(1, "test", "", "STARTED", "STARTED", 100),
                isShardInfo(2, "test", "", "STARTED", "STARTED", 100),
                isShardInfo(3, "test", "", "STARTED", "STARTED", 100));
    }

    @Test
    public void test_local_node_is_data_node_no_shards_locally() {
        var nodes = DiscoveryNodes
            .builder()
            .add(discoveryNode("node_1"))
            .add(discoveryNode("node_2"))
            .masterNodeId("node_1")
            .localNodeId("node_2")
            .build();

        var nodeInfo = new NodeInfo(() -> clusterState.nodes(nodes).build(), this::shardStateAndSizeProvider);

        assertThat(nodeInfo.getNodeId()).isEqualTo("node_2");
        assertThat(nodeInfo.getNodeName()).isEqualTo("node_2");
        var shardStats = nodeInfo.getShardStats();
        assertThat(shardStats.getPrimaries()).isEqualTo(0);
        assertThat(shardStats.getTotal()).isEqualTo(0);
        assertThat(shardStats.getReplicas()).isEqualTo(0);
        assertThat(shardStats.getUnassigned()).isEqualTo(0);
    }

    @Test
    public void test_local_node_is_master_node_no_shards_locally() {
        var nodes = DiscoveryNodes
            .builder()
            .add(discoveryNode("node_1"))
            .add(discoveryNode("node_2"))
            .masterNodeId("node_2")
            .localNodeId("node_2")
            .build();

        var nodeInfo = new NodeInfo(() -> clusterState.nodes(nodes).build(), this::shardStateAndSizeProvider);

        assertThat(nodeInfo.getNodeId()).isEqualTo("node_2");
        assertThat(nodeInfo.getNodeName()).isEqualTo("node_2");
        var shardStats = nodeInfo.getShardStats();
        assertThat(shardStats.getPrimaries()).isEqualTo(0);
        assertThat(shardStats.getTotal()).isEqualTo(0);
        assertThat(shardStats.getReplicas()).isEqualTo(0);
        // Unassigned shards are only counted on the master node
        assertThat(shardStats.getUnassigned()).isEqualTo(1);

        assertThat(nodeInfo.getShardInfo().isEmpty()).isTrue();
    }

    @Test
    public void test_local_node_is_data_node_all_shards_locally() {
        var nodes = DiscoveryNodes
            .builder()
            .add(discoveryNode("node_1"))
            .add(discoveryNode("node_2"))
            .masterNodeId("node_2")
            .localNodeId("node_1")
            .build();

        var nodeInfo = new NodeInfo(() -> clusterState.nodes(nodes).build(), this::shardStateAndSizeProvider);
        var shardStats = nodeInfo.getShardStats();
        assertThat(shardStats.getPrimaries()).isEqualTo(1);
        assertThat(shardStats.getTotal()).isEqualTo(3);
        assertThat(shardStats.getReplicas()).isEqualTo(2);
        // Unassigned shards are not counted on a data node
        assertThat(shardStats.getUnassigned()).isEqualTo(0);

        assertThat(nodeInfo.getShardInfo())
            .satisfiesExactlyInAnyOrder(
               isShardInfo(1, "test", "", "STARTED", "STARTED", 100),
               isShardInfo(2, "test", "", "STARTED", "STARTED", 100),
               isShardInfo(3, "test", "", "STARTED", "STARTED", 100));
    }

    @Test
    public void test_partitioned_tables() {
        var tableName = ".partitioned.test.p1";
        var indexRoutingTableBuilder = IndexRoutingTable
            .builder(new Index(tableName, UUID.randomUUID().toString()))
            .addShard(TestShardRouting.newShardRouting(tableName,
                                                       1,
                                                       "node_1",
                                                       true,
                                                       ShardRoutingState.STARTED))
            .addShard(TestShardRouting.newShardRouting(tableName,
                                                       2,
                                                       "node_1",
                                                       false,
                                                       ShardRoutingState.STARTED))
            .addShard(TestShardRouting.newShardRouting(tableName,
                                                       3,
                                                       "node_1",
                                                       false,
                                                       ShardRoutingState.STARTED));


        var routingTable = RoutingTable.builder().add(indexRoutingTableBuilder).build();
        var meta = IndexMetadata.builder(tableName).settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(2);
        var cs = ClusterState.builder(new ClusterName("crate")).version(1L).routingTable(routingTable)
            .metadata(Metadata.builder().put(meta));

        var nodes = DiscoveryNodes
            .builder()
            .add(discoveryNode("node_1"))
            .localNodeId("node_1")
            .masterNodeId("node_1")
            .build();

        var nodeInfo = new NodeInfo(() -> cs.nodes(nodes).build(), this::shardStateAndSizeProvider);
        var shardStats = nodeInfo.getShardStats();
        assertThat(shardStats.getPrimaries()).isEqualTo(1);
        assertThat(shardStats.getTotal()).isEqualTo(3);
        assertThat(shardStats.getReplicas()).isEqualTo(2);

        assertThat(nodeInfo.getShardInfo())
            .satisfiesExactlyInAnyOrder(
               isShardInfo(1, "test", "p1", "STARTED", "STARTED", 100),
               isShardInfo(2, "test", "p1", "STARTED", "STARTED", 100),
               isShardInfo(3, "test", "p1", "STARTED", "STARTED", 100));

    }

    Tuple<IndexShardState, Long> shardStateAndSizeProvider(ShardId shardId) {
        return new Tuple<>(IndexShardState.STARTED, 100L);
    }

    DiscoveryNode discoveryNode(String id) {
        return new DiscoveryNode(id,
                                 id,
                                 buildNewFakeTransportAddress(),
                                 Map.of(),
                                 Set.of(DiscoveryNodeRole.MASTER_ROLE, DiscoveryNodeRole.DATA_ROLE),
                                 Version.CURRENT);
    }

    ThrowingConsumer<ShardInfo> isShardInfo(int shardId, String table, String partitionIdent, String routingState, String state, long size) {
        return s -> assertThat(s)
            .satisfies(si -> assertThat(si.shardId).isEqualTo(shardId))
            .satisfies(si -> assertThat(si.table).isEqualTo(table))
            .satisfies(si -> assertThat(si.routingState).isEqualTo(routingState))
            .satisfies(si -> assertThat(si.state).isEqualTo(state))
            .satisfies(si -> assertThat(si.partitionIdent).isEqualTo(partitionIdent))
            .satisfies(si -> assertThat(si.size).isEqualTo(size));
    }
}
