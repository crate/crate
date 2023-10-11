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

package org.elasticsearch.cluster.allocation;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsAction;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.decider.FilterAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ThrottlingAllocationDecider;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.IntegTestCase;
import org.elasticsearch.test.IntegTestCase.ClusterScope;
import org.elasticsearch.test.IntegTestCase.Scope;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.junit.Test;

@ClusterScope(scope= Scope.TEST, numDataNodes=0)
public class FilteringAllocationIT extends IntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(InternalSettingsPlugin.class);
        return plugins;
    }

    @Test
    public void testDecommissionNodeNoReplicas() throws Exception {
        logger.info("--> starting 2 nodes");
        List<String> nodesIds = cluster().startNodes(2);
        final String node_0 = nodesIds.get(0);
        final String node_1 = nodesIds.get(1);
        assertThat(cluster().size(), equalTo(2));

        logger.info("--> creating an index with no replicas");
        execute("create table test(x int, value text) clustered into ? shards with (number_of_replicas='0')",
                new Object[]{numberOfShards()});

        String tableName = getFqn("test");

        for (int i = 0; i < 100; i++) {
            execute("insert into test(x, value) values(?,?)", new Object[]{i, Integer.toString(i)});
        }

        execute("refresh table test");
        execute("select count(*) from test");
        assertThat(100L, is(response.rows()[0][0]));

        final boolean closed = randomBoolean();
        if (closed) {
            execute("alter table test close");
            ensureGreen(tableName);
        }

        logger.info("--> decommission the second node");
        execute("set global \"cluster.routing.allocation.exclude._name\" = ?", new Object[]{node_1});

        ensureGreen(tableName);

        logger.info("--> verify all are allocated on node1 now");
        ClusterState clusterState = client().admin().cluster().state(new ClusterStateRequest()).get().getState();
        for (IndexRoutingTable indexRoutingTable : clusterState.routingTable()) {
            for (IndexShardRoutingTable indexShardRoutingTable : indexRoutingTable) {
                for (ShardRouting shardRouting : indexShardRoutingTable) {
                    assertThat(clusterState.nodes().get(shardRouting.currentNodeId()).getName(), equalTo(node_0));
                }
            }
        }

        if (closed) {
            execute("alter table test open");
            ensureGreen(tableName);
        }

        execute("select count(*) from test");
        assertThat(100L, is(response.rows()[0][0]));
    }

    @Test
    public void testAutoExpandReplicasToFilteredNodes() throws Exception {
        logger.info("--> starting 2 nodes");
        List<String> nodesIds = cluster().startNodes(2);
        final String node_0 = nodesIds.get(0);
        final String node_1 = nodesIds.get(1);
        assertThat(cluster().size(), equalTo(2));

        logger.info("--> creating an index with auto-expand replicas");
        execute("create table test(x int, value text) clustered into ? shards with (number_of_replicas='0-all')",
                new Object[]{numberOfShards()});

        String tableName = getFqn("test");

        ClusterState clusterState = client().admin().cluster().state(new ClusterStateRequest()).get().getState();
        assertThat(clusterState.metadata().index(tableName).getNumberOfReplicas(), equalTo(1));
        ensureGreen(tableName);

        logger.info("--> filter out the second node");
        if (randomBoolean()) {
            execute("set global \"cluster.routing.allocation.exclude._name\" = ?", new Object[]{node_1});
        } else {
            execute("alter table test set( \"routing.allocation.exclude._name\" = ?)", new Object[]{node_1});
        }
        ensureGreen(tableName);

        logger.info("--> verify all are allocated on node1 now");
        final var cs = client().admin().cluster().state(new ClusterStateRequest()).get().getState();
        assertThat(cs.metadata().index(tableName).getNumberOfReplicas(), equalTo(0));
        for (IndexRoutingTable indexRoutingTable : cs.routingTable()) {
            for (IndexShardRoutingTable indexShardRoutingTable : indexRoutingTable) {
                for (ShardRouting shardRouting : indexShardRoutingTable) {
                    assertThat(cs.nodes().get(shardRouting.currentNodeId()).getName(), equalTo(node_0));
                }
            }
        }
    }

    @Test
    public void testDisablingAllocationFiltering() throws Exception {
        logger.info("--> starting 2 nodes");
        List<String> nodesIds = cluster().startNodes(2);
        final String node_0 = nodesIds.get(0);
        final String node_1 = nodesIds.get(1);
        assertThat(cluster().size(), equalTo(2));

        logger.info("--> creating an index with no replicas");

        execute("create table test(x int, value text) clustered into 2 shards with (number_of_replicas='0')");

        String tableName = getFqn("test");
        ensureGreen(tableName);

        logger.info("--> index some data");
        for (int i = 0; i < 100; i++) {
            execute("insert into test(x, value) values(?,?)", new Object[]{i, Integer.toString(i)});
        }

        execute("refresh table test");
        execute("select count(*) from test");
        assertThat(100L, is(response.rows()[0][0]));

        final boolean closed = randomBoolean();
        if (closed) {
            execute("alter table test close");
            ensureGreen(tableName);
        }

        ClusterState clusterState = client().admin().cluster().state(new ClusterStateRequest()).get().getState();
        IndexRoutingTable indexRoutingTable = clusterState.routingTable().index(tableName);
        int numShardsOnNode1 = 0;
        for (IndexShardRoutingTable indexShardRoutingTable : indexRoutingTable) {
            for (ShardRouting shardRouting : indexShardRoutingTable) {
                if (node_1.equals(clusterState.nodes().get(shardRouting.currentNodeId()).getName())) {
                    numShardsOnNode1++;
                }
            }
        }

        if (numShardsOnNode1 > ThrottlingAllocationDecider.DEFAULT_CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_RECOVERIES) {
            execute("set global \"cluster.routing.allocation.node_concurrent_recoveries\" = ?", new Object[]{numShardsOnNode1});
            // make sure we can recover all the nodes at once otherwise we might run into a state where
            // one of the shards has not yet started relocating but we already fired up the request to wait for 0 relocating shards.
        }
        logger.info("--> remove index from the first node");

        if (closed) {
            execute("alter table test open");
        }
        execute("alter table test set( \"routing.allocation.exclude._name\" = ?)", new Object[]{node_0});
        ensureGreen(tableName);

        logger.info("--> verify all shards are allocated on node_1 now");
        var state = client().admin().cluster().state(new ClusterStateRequest()).get().getState();
        for (IndexShardRoutingTable indexShardRoutingTable : state.routingTable().index(tableName)) {
            for (ShardRouting shardRouting : indexShardRoutingTable) {
                assertThat(state.nodes().get(shardRouting.currentNodeId()).getName(), equalTo(node_1));
            }
        }

        logger.info("--> disable allocation filtering ");
        execute(" alter table test reset( \"routing.allocation.exclude._name\")");

        ensureGreen(tableName);

        logger.info("--> verify that there are shards allocated on both nodes now");
        state = client().admin().cluster().state(new ClusterStateRequest()).get().getState();
        assertThat(state.routingTable().index(tableName).numberOfNodesShardsAreAllocatedOn(), equalTo(2));
    }

    @Test
    public void testInvalidIPFilterClusterSettings() {
        String ipKey = randomFrom("_ip", "_host_ip", "_publish_ip");
        Setting<String> filterSetting = randomFrom(FilterAllocationDecider.CLUSTER_ROUTING_REQUIRE_GROUP_SETTING,
                                                   FilterAllocationDecider.CLUSTER_ROUTING_INCLUDE_GROUP_SETTING, FilterAllocationDecider.CLUSTER_ROUTING_EXCLUDE_GROUP_SETTING);
        assertThatThrownBy(() -> FutureUtils.get(
                client().admin().cluster().execute(ClusterUpdateSettingsAction.INSTANCE, new ClusterUpdateSettingsRequest()
                    .transientSettings(Settings.builder().put(filterSetting.getKey() + ipKey, "192.168.1.1."))
                )))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("invalid IP address [192.168.1.1.] for [" + filterSetting.getKey() + ipKey + "]");
    }

    public void testTransientSettingsStillApplied() throws Exception {
        List<String> nodes = cluster().startNodes(6);
        Set<String> excludeNodes = new HashSet<>(nodes.subList(0, 3));
        Set<String> includeNodes = new HashSet<>(nodes.subList(3, 6));
        String excludeNodeIdsAsString = String.join(",", excludeNodes);
        logger.info("--> exclude: [{}], include: [{}]",
                    excludeNodeIdsAsString,
                    String.join(", ", includeNodes));
        ensureStableCluster(6);

        execute("create table test(x int, value text) clustered into ? shards with (number_of_replicas='0')",
                new Object[]{numberOfShards()});

        String tableName = getFqn("test");
        ensureGreen(tableName);


        if (randomBoolean()) {
            execute("alter table test close");
        }

        logger.info("--> updating settings");
        execute(" set global transient \"cluster.routing.allocation.exclude._name\" = ?",
                new Object[]{excludeNodeIdsAsString});

        logger.info("--> waiting for relocation");
        waitForRelocation(ClusterHealthStatus.GREEN);

        ClusterState state = client().admin().cluster().state(new ClusterStateRequest()).get().getState();

        for (ShardRouting shard : state.routingTable().shardsWithState(ShardRoutingState.STARTED)) {
            String node = state.getRoutingNodes().node(shard.currentNodeId()).node().getName();
            logger.info("--> shard on {} - {}", node, shard);
            assertTrue("shard on " + node + " but should only be on the include node list: " +
                       String.join(", ", includeNodes),
                       includeNodes.contains(node));
        }

        logger.info("--> updating settings with random persistent setting");
        execute(" set global persistent \"memory.allocation.type\" = 'off-heap'");
        execute(" set global transient \"cluster.routing.allocation.exclude._name\" = ?",
                new Object[]{excludeNodeIdsAsString});

        logger.info("--> waiting for relocation");
        waitForRelocation(ClusterHealthStatus.GREEN);

        state = client().admin().cluster().state(new ClusterStateRequest()).get().getState();

        // The transient settings still exist in the state
        assertThat(state.metadata().transientSettings(),
                   equalTo(Settings.builder().put("cluster.routing.allocation.exclude._name",
                                                  excludeNodeIdsAsString).build()));

        for (ShardRouting shard : state.routingTable().shardsWithState(ShardRoutingState.STARTED)) {
            String node = state.getRoutingNodes().node(shard.currentNodeId()).node().getName();
            logger.info("--> shard on {} - {}", node, shard);
            assertTrue("shard on " + node + " but should only be on the include node list: " +
                       String.join(", ", includeNodes),
                       includeNodes.contains(node));
        }
    }
}

