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

package io.crate.integrationtests;

import org.elasticsearch.cluster.ClusterInfoService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.MockInternalClusterInfoService;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import io.crate.common.unit.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.monitor.fs.FsInfo;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.StreamSupport;

import static java.util.stream.Collectors.toList;
import static org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_REROUTE_INTERVAL_SETTING;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class DiskUsagesITest extends SQLIntegrationTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        ArrayList<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        // Use the mock internal cluster info service, which has fake-able disk usages
        plugins.add(MockInternalClusterInfoService.TestPlugin.class);
        return plugins;
    }

    @Test
    public void testRerouteOccursOnDiskPassingHighWatermark() throws Exception {
        for (int i = 0; i < 3; i++) {
            // ensure that each node has a single data path
            internalCluster().startNode(
                Settings.builder()
                    .put(Environment.PATH_DATA_SETTING.getKey(), createTempDir())
                    .put(CLUSTER_ROUTING_ALLOCATION_REROUTE_INTERVAL_SETTING.getKey(), "1ms"));
        }
        List<String> nodeIds = getNodeIds();

        var clusterInfoService = getMockInternalClusterInfoService();
        clusterInfoService.setUpdateFrequency(TimeValue.timeValueMillis(200));
        clusterInfoService.onMaster();

        // prevent any effects from in-flight recoveries, since we are only simulating a 100-byte disk
        clusterInfoService.shardSizeFunction = shardRouting -> 0L;
        // start with all nodes below the watermark
        clusterInfoService.diskUsageFunction = (discoveryNode, fsInfoPath) ->
            setDiskUsage(fsInfoPath, 100, between(10, 100));

        execute("SET GLOBAL TRANSIENT" +
                "   cluster.routing.allocation.disk.watermark.low='80%'," +
                "   cluster.routing.allocation.disk.watermark.high='90%'," +
                "   cluster.routing.allocation.disk.watermark.flood_stage='100%'");

        execute("CREATE TABLE t (id INT PRIMARY KEY) " +
                "CLUSTERED INTO 10 SHARDS " +
                "WITH (number_of_replicas = 0)");
        ensureGreen();
        assertBusy(() -> {
            var shardCountByNodeId = getShardCountByNodeId();
            assertThat("node0 has at least 3 shards", shardCountByNodeId.get(nodeIds.get(0)), greaterThanOrEqualTo(3));
            assertThat("node1 has at least 3 shards", shardCountByNodeId.get(nodeIds.get(1)), greaterThanOrEqualTo(3));
            assertThat("node2 has at least 3 shards", shardCountByNodeId.get(nodeIds.get(2)), greaterThanOrEqualTo(3));
        });


        // move node3 above high watermark
        clusterInfoService.diskUsageFunction = (discoveryNode, fsInfoPath) -> setDiskUsage(
            fsInfoPath, 100,
            discoveryNode.getId().equals(nodeIds.get(2)) ? between(0, 9) : between(10, 100));

        assertBusy(() -> {
            var shardCountByNodeId = getShardCountByNodeId();
            assertThat("node0 has 5 shards", shardCountByNodeId.get(nodeIds.get(0)), is(5));
            assertThat("node1 has 5 shards", shardCountByNodeId.get(nodeIds.get(1)), is(5));
            assertThat("node2 has 0 shards", shardCountByNodeId.get(nodeIds.get(2)), is(0));
        });

        // move all nodes below watermark again
        clusterInfoService.diskUsageFunction = (discoveryNode, fsInfoPath) ->
            setDiskUsage(fsInfoPath, 100, between(10, 100));

        assertBusy(() -> {
            var shardCountByNodeId = getShardCountByNodeId();
            assertThat("node0 has at least 3 shards", shardCountByNodeId.get(nodeIds.get(0)), greaterThanOrEqualTo(3));
            assertThat("node1 has at least 3 shards", shardCountByNodeId.get(nodeIds.get(1)), greaterThanOrEqualTo(3));
            assertThat("node2 has at least 3 shards", shardCountByNodeId.get(nodeIds.get(2)), greaterThanOrEqualTo(3));
        });
    }

    @Test
    public void testOnlyMovesEnoughShardsToDropBelowHighWatermark() throws Exception {
        for (int i = 0; i < 3; i++) {
            // ensure that each node has a single data path
            internalCluster().startNode(
                Settings.builder()
                    .put(Environment.PATH_DATA_SETTING.getKey(), createTempDir())
                    .put(CLUSTER_ROUTING_ALLOCATION_REROUTE_INTERVAL_SETTING.getKey(), "1ms"));
        }
        List<String> nodeIds = getNodeIds();

        var clusterInfoService = getMockInternalClusterInfoService();
        AtomicReference<ClusterState> masterAppliedClusterState = new AtomicReference<>();
        internalCluster().getCurrentMasterNodeInstance(ClusterService.class).addListener(event -> {
            masterAppliedClusterState.set(event.state());
            clusterInfoService.refresh(); // so subsequent reroute sees disk usage according to the current state
        });

        // shards are 1 byte large
        clusterInfoService.shardSizeFunction = shardRouting -> 1L;
        // start with all nodes below the watermark
        clusterInfoService.diskUsageFunction = (discoveryNode, fsInfoPath) ->
            setDiskUsage(fsInfoPath, 1000L, 1000L);

        execute("SET GLOBAL TRANSIENT" +
                "   cluster.routing.allocation.disk.watermark.low='90%'," +
                "   cluster.routing.allocation.disk.watermark.high='90%'," +
                "   cluster.routing.allocation.disk.watermark.flood_stage='100%'");

        execute("CREATE TABLE t (id INT PRIMARY KEY) " +
                "CLUSTERED INTO 6 SHARDS " +
                "WITH (number_of_replicas = 0)");
        ensureGreen();

        var shardCountByNodeId = getShardCountByNodeId();
        assertThat("node0 has 2 shards", shardCountByNodeId.get(nodeIds.get(0)), is(2));
        assertThat("node1 has 2 shards", shardCountByNodeId.get(nodeIds.get(1)), is(2));
        assertThat("node2 has 2 shards", shardCountByNodeId.get(nodeIds.get(2)), is(2));

        // disable rebalancing, or else we might move too many shards away and then rebalance them back again
        execute("SET GLOBAL TRANSIENT cluster.routing.rebalance.enable='none'");

        // node2 suddenly has 99 bytes free, less than 10%, but moving one shard is enough to bring it up to 100 bytes free:
        clusterInfoService.diskUsageFunction = (discoveryNode, fsInfoPath) -> setDiskUsage(
            fsInfoPath, 1000L,
            discoveryNode.getId().equals(nodeIds.get(2))
                ? 101L - masterAppliedClusterState.get().getRoutingNodes().node(nodeIds.get(2)).numberOfOwningShards()
                : 1000L);

        clusterInfoService.refresh();

        logger.info("waiting for shards to relocate off node [{}]", nodeIds.get(2));

        // must wait for relocation to start
        assertBusy(() -> assertThat("node2 has 1 shard", getShardCountByNodeId().get(nodeIds.get(2)), is(1)));

        // ensure that relocations finished without moving any more shards
        ensureGreen();
        assertThat("node2 has 1 shard", getShardCountByNodeId().get(nodeIds.get(2)), is(1));
    }

    @Test
    public void testDoesNotExceedLowWatermarkWhenRebalancing() {
        for (int i = 0; i < 3; i++) {
            // ensure that each node has a single data path
            internalCluster().startNode(
                Settings.builder()
                    .put(Environment.PATH_DATA_SETTING.getKey(), createTempDir())
                    .put(CLUSTER_ROUTING_ALLOCATION_REROUTE_INTERVAL_SETTING.getKey(), "1ms"));
        }
        List<String> nodeIds = getNodeIds();

        var clusterInfoService = getMockInternalClusterInfoService();

        AtomicReference<ClusterState> masterAppliedClusterState = new AtomicReference<>();
        internalCluster().getCurrentMasterNodeInstance(ClusterService.class).addListener(event -> {
            assertThat(event.state().getRoutingNodes().node(nodeIds.get(2)).size(), lessThanOrEqualTo(1));
            masterAppliedClusterState.set(event.state());
            clusterInfoService.refresh(); // so a subsequent reroute sees disk usage according to the current state
        });

        // shards are 1 byte large
        clusterInfoService.shardSizeFunction = shardRouting -> 1L;
        // node 2 only has space for one shard
        clusterInfoService.diskUsageFunction = (discoveryNode, fsInfoPath) -> setDiskUsage(
            fsInfoPath, 1000L,
            discoveryNode.getId().equals(nodeIds.get(2))
                ? 150L - masterAppliedClusterState.get().getRoutingNodes().node(nodeIds.get(2)).numberOfOwningShards()
                : 1000L);

        execute("SET GLOBAL TRANSIENT" +
                "   cluster.routing.allocation.disk.watermark.low='85%'," +
                "   cluster.routing.allocation.disk.watermark.high='100%'," +
                "   cluster.routing.allocation.disk.watermark.flood_stage='100%'");

        execute("CREATE TABLE t (id INT PRIMARY KEY) " +
                "CLUSTERED INTO 6 SHARDS " +
                "WITH (" +
                "   number_of_replicas = 0, \"routing.allocation.exclude._id\" = ?)",
                new Object[]{nodeIds.get(2)});
        ensureGreen();

        var shardCountByNodeId = getShardCountByNodeId();
        assertThat("node0 has 3 shards", shardCountByNodeId.get(nodeIds.get(0)), is(3));
        assertThat("node1 has 3 shards", shardCountByNodeId.get(nodeIds.get(1)), is(3));
        assertThat("node2 has 0 shards", shardCountByNodeId.get(nodeIds.get(2)), is(0));

        execute("ALTER TABLE t RESET (\"routing.allocation.exclude._id\")");

        logger.info("waiting for shards to relocate onto node [{}]", nodeIds.get(2));

        ensureGreen();
        assertThat("node2 has 1 shard", getShardCountByNodeId().get(nodeIds.get(2)), is(1));
    }

    @Test
    public void testMovesShardsOffSpecificDataPathAboveWatermark() throws Exception {
        // start one node with two data paths
        String pathOverWatermark = createTempDir().toString();
        Settings.Builder twoPathSettings = Settings.builder()
            .put(CLUSTER_ROUTING_ALLOCATION_REROUTE_INTERVAL_SETTING.getKey(), "1ms");
        if (randomBoolean()) {
            twoPathSettings.putList(
                Environment.PATH_DATA_SETTING.getKey(),
                createTempDir().toString(),
                pathOverWatermark);
        } else {
            twoPathSettings.putList(
                Environment.PATH_DATA_SETTING.getKey(),
                pathOverWatermark,
                createTempDir().toString());
        }
        internalCluster().startNode(twoPathSettings);

        execute("SELECT id FROM sys.nodes");
        assertThat(response.rows().length, is(1));
        String nodeIdWithTwoPaths = (String) response.rows()[0][0];

        // other two nodes have one data path each
        internalCluster().startNode(
            Settings.builder()
                .put(Environment.PATH_DATA_SETTING.getKey(), createTempDir())
                .put(CLUSTER_ROUTING_ALLOCATION_REROUTE_INTERVAL_SETTING.getKey(), "1ms"));
        internalCluster().startNode(
            Settings.builder()
                .put(Environment.PATH_DATA_SETTING.getKey(), createTempDir())
                .put(CLUSTER_ROUTING_ALLOCATION_REROUTE_INTERVAL_SETTING.getKey(), "1ms"));
        List<String> nodeIds = getNodeIds();

        var clusterInfoService = getMockInternalClusterInfoService();
        // prevent any effects from in-flight recoveries, since we are only simulating a 100-byte disk
        clusterInfoService.shardSizeFunction = shardRouting -> 0L;
        // start with all paths below the watermark
        clusterInfoService.diskUsageFunction = (discoveryNode, fsInfoPath) ->
            setDiskUsage(fsInfoPath, 100, between(10, 100));

        execute("SET GLOBAL TRANSIENT" +
                "   cluster.routing.allocation.disk.watermark.low='90%'," +
                "   cluster.routing.allocation.disk.watermark.high='90%'," +
                "   cluster.routing.allocation.disk.watermark.flood_stage='100%'");

        execute("CREATE TABLE doc.t (id INT PRIMARY KEY) " +
                "CLUSTERED INTO 6 SHARDS " +
                "WITH (number_of_replicas = 0)");
        ensureGreen();

        var shardCountByNodeId = getShardCountByNodeId();
        assertThat("node0 has 2 shards", shardCountByNodeId.get(nodeIds.get(0)), is(2));
        assertThat("node1 has 2 shards", shardCountByNodeId.get(nodeIds.get(1)), is(2));
        assertThat("node2 has 2 shards", shardCountByNodeId.get(nodeIds.get(2)), is(2));

        // there should be one shard on a bad path on node0
        execute(
            "SELECT path " +
            "FROM sys.shards " +
            "WHERE table_name = 't' " +
            "  AND schema_name = 'doc'" +
            "  AND node['id'] = ?",
            new Object[]{nodeIdWithTwoPaths});
        assertThat(
            Arrays.stream(response.rows())
                .map(row -> (String) row[0])
                .filter(path -> path != null && path.startsWith(pathOverWatermark))
                .count(),
            is(1L));

        // one of the paths on node0 suddenly exceeds the high watermark
        clusterInfoService.diskUsageFunction = (discoveryNode, fsInfoPath) -> setDiskUsage(
            fsInfoPath, 100L,
            fsInfoPath.getPath().startsWith(pathOverWatermark) ? between(0, 9) : between(10, 100));

        // disable rebalancing, or else we might move shards back
        // onto the over-full path since we're not faking that
        execute("SET GLOBAL TRANSIENT cluster.routing.rebalance.enable='none'");

        clusterInfoService.refresh();

        logger.info("waiting for shards to relocate off path [{}]", pathOverWatermark);
        assertBusy(() -> {
            execute(
                "SELECT path " +
                "FROM sys.shards " +
                "WHERE table_name = 't'" +
                "  AND schema_name = 'doc'");
            assertThat(
                Arrays.stream(response.rows())
                    .map(row -> (String) row[0])
                    .filter(path -> path != null && path.startsWith(pathOverWatermark))
                    .count(),
                is(0L));
        });
    }

    private static ClusterState clusterState() {
        return client().admin().cluster().prepareState().get().getState();
    }

    private static FsInfo.Path setDiskUsage(FsInfo.Path original, long totalBytes, long freeBytes) {
        return new FsInfo.Path(original.getPath(), original.getMount(), totalBytes, freeBytes, freeBytes);
    }

    private static List<String> getNodeIds() {
        return StreamSupport.stream(clusterState().getRoutingNodes().spliterator(), false)
            .map(RoutingNode::nodeId)
            .collect(toList());
    }

    private MockInternalClusterInfoService getMockInternalClusterInfoService() {
        return (MockInternalClusterInfoService) internalCluster().getCurrentMasterNodeInstance(ClusterInfoService.class);
    }

    private HashMap<String, Integer> getShardCountByNodeId() {
        HashMap<String, Integer> shardCountByNodeId = new HashMap<>();
        var clusterState = clusterState();
        for (final RoutingNode node : clusterState.getRoutingNodes()) {
            shardCountByNodeId.put(
                node.nodeId(),
                clusterState.getRoutingNodes().node(node.nodeId()).numberOfOwningShards());
        }
        return shardCountByNodeId;
    }
}
