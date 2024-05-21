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

import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_REROUTE_INTERVAL_SETTING;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsAction;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.cluster.ClusterInfoService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.MockInternalClusterInfoService;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.env.Environment;
import org.elasticsearch.monitor.fs.FsInfo;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.IntegTestCase;
import org.junit.Test;

import io.crate.common.unit.TimeValue;
import io.crate.testing.TestingHelpers;
import io.crate.testing.UseJdbc;
import io.crate.testing.UseRandomizedOptimizerRules;
import io.crate.testing.UseRandomizedSchema;

@IntegTestCase.ClusterScope(scope = IntegTestCase.Scope.TEST, numDataNodes = 0)
public class DiskUsagesITest extends IntegTestCase {

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
            cluster().startNode(
                Settings.builder()
                    .put(Environment.PATH_DATA_SETTING.getKey(), createTempDir())
                    .put(CLUSTER_ROUTING_ALLOCATION_REROUTE_INTERVAL_SETTING.getKey(), "0ms"));
        }
        List<String> nodeIds = getNodeIds();

        var clusterInfoService = getMockInternalClusterInfoService();
        clusterInfoService.setUpdateFrequency(TimeValue.timeValueMillis(200));

        // prevent any effects from in-flight recoveries, since we are only simulating a 100-byte disk
        clusterInfoService.setShardSizeFunctionAndRefresh(shardRouting -> 0L);
        // start with all nodes below the watermark
        clusterInfoService.setDiskUsageFunctionAndRefresh((discoveryNode, fsInfoPath) ->
            setDiskUsage(fsInfoPath, 100, between(10, 100)));

        clusterInfoService.refresh();

        execute("SET GLOBAL TRANSIENT" +
                "   cluster.routing.allocation.disk.watermark.low='90%'," +
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
        clusterInfoService.setDiskUsageFunctionAndRefresh((discoveryNode, fsInfoPath) -> setDiskUsage(
            fsInfoPath, 100,
            discoveryNode.getId().equals(nodeIds.get(2)) ? between(0, 9) : between(10, 100)));

        assertBusy(() -> {
            var shardCountByNodeId = getShardCountByNodeId();
            assertThat(shardCountByNodeId.get(nodeIds.get(0))).as("node0 has 5 shards").isEqualTo(5);
            assertThat(shardCountByNodeId.get(nodeIds.get(1))).as("node1 has 5 shards").isEqualTo(5);
            assertThat(shardCountByNodeId.get(nodeIds.get(2))).as("node2 has 0 shards").isEqualTo(0);
        });

        // move all nodes below watermark again
        clusterInfoService.setDiskUsageFunctionAndRefresh((discoveryNode, fsInfoPath) ->
            setDiskUsage(fsInfoPath, 100, between(10, 100)));

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
            cluster().startNode(
                Settings.builder()
                    .put(Environment.PATH_DATA_SETTING.getKey(), createTempDir())
                    .put(CLUSTER_ROUTING_ALLOCATION_REROUTE_INTERVAL_SETTING.getKey(), "0ms"));
        }
        List<String> nodeIds = getNodeIds();

        var clusterInfoService = getMockInternalClusterInfoService();
        AtomicReference<ClusterState> masterAppliedClusterState = new AtomicReference<>();
        cluster().getCurrentMasterNodeInstance(ClusterService.class).addListener(event -> {
            masterAppliedClusterState.set(event.state());
            clusterInfoService.refresh(); // so subsequent reroute sees disk usage according to the current state
        });

        // shards are 1 byte large
        clusterInfoService.setShardSizeFunctionAndRefresh(shardRouting -> 1L);
        // start with all nodes below the watermark
        clusterInfoService.setDiskUsageFunctionAndRefresh((discoveryNode, fsInfoPath) ->
            setDiskUsage(fsInfoPath, 1000L, 1000L));

        execute("SET GLOBAL TRANSIENT" +
                "   cluster.routing.allocation.disk.watermark.low='90%'," +
                "   cluster.routing.allocation.disk.watermark.high='90%'," +
                "   cluster.routing.allocation.disk.watermark.flood_stage='100%'");

        execute("CREATE TABLE t (id INT PRIMARY KEY) " +
                "CLUSTERED INTO 6 SHARDS " +
                "WITH (number_of_replicas = 0)");
        ensureGreen();

        var shardCountByNodeId = getShardCountByNodeId();
        assertThat(shardCountByNodeId.get(nodeIds.get(0))).as("node0 has 2 shards").isEqualTo(2);
        assertThat(shardCountByNodeId.get(nodeIds.get(1))).as("node1 has 2 shards").isEqualTo(2);
        assertThat(shardCountByNodeId.get(nodeIds.get(2))).as("node2 has 2 shards").isEqualTo(2);

        // disable rebalancing, or else we might move too many shards away and then rebalance them back again
        execute("SET GLOBAL TRANSIENT cluster.routing.rebalance.enable='none'");

        // node2 suddenly has 99 bytes free, less than 10%, but moving one shard is enough to bring it up to 100 bytes free:
        clusterInfoService.setDiskUsageFunctionAndRefresh((discoveryNode, fsInfoPath) -> setDiskUsage(
            fsInfoPath, 1000L,
            discoveryNode.getId().equals(nodeIds.get(2))
                ? 101L - masterAppliedClusterState.get().getRoutingNodes().node(nodeIds.get(2)).numberOfOwningShards()
                : 1000L));

        clusterInfoService.refresh();

        logger.info("waiting for shards to relocate off node [{}]", nodeIds.get(2));

        // must wait for relocation to start
        assertBusy(() -> assertThat("node2 has 1 shard", getShardCountByNodeId().get(nodeIds.get(2)), is(1)));

        // ensure that relocations finished without moving any more shards
        ensureGreen();
        assertThat(getShardCountByNodeId().get(nodeIds.get(2))).as("node2 has 1 shard").isEqualTo(1);
    }

    @Test
    public void testDoesNotExceedLowWatermarkWhenRebalancing() {
        for (int i = 0; i < 3; i++) {
            // ensure that each node has a single data path
            cluster().startNode(
                Settings.builder()
                    .put(Environment.PATH_DATA_SETTING.getKey(), createTempDir())
                    .put(CLUSTER_ROUTING_ALLOCATION_REROUTE_INTERVAL_SETTING.getKey(), "1ms"));
        }
        List<String> nodeIds = getNodeIds();

        var clusterInfoService = getMockInternalClusterInfoService();

        AtomicReference<ClusterState> masterAppliedClusterState = new AtomicReference<>();
        cluster().getCurrentMasterNodeInstance(ClusterService.class).addListener(event -> {
            assertThat(event.state().getRoutingNodes().node(nodeIds.get(2)).size(), lessThanOrEqualTo(1));
            masterAppliedClusterState.set(event.state());
            clusterInfoService.refresh(); // so a subsequent reroute sees disk usage according to the current state
        });

        // shards are 1 byte large
        clusterInfoService.setShardSizeFunctionAndRefresh(shardRouting -> 1L);
        // node 2 only has space for one shard
        clusterInfoService.setDiskUsageFunctionAndRefresh((discoveryNode, fsInfoPath) -> setDiskUsage(
            fsInfoPath, 1000L,
            discoveryNode.getId().equals(nodeIds.get(2))
                ? 150L - masterAppliedClusterState.get().getRoutingNodes().node(nodeIds.get(2)).numberOfOwningShards()
                : 1000L));

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
        assertThat(shardCountByNodeId.get(nodeIds.get(0))).as("node0 has 3 shards").isEqualTo(3);
        assertThat(shardCountByNodeId.get(nodeIds.get(1))).as("node1 has 3 shards").isEqualTo(3);
        assertThat(shardCountByNodeId.get(nodeIds.get(2))).as("node2 has 0 shards").isEqualTo(0);

        execute("ALTER TABLE t RESET (\"routing.allocation.exclude._id\")");

        logger.info("waiting for shards to relocate onto node [{}]", nodeIds.get(2));

        ensureGreen();
        assertThat(getShardCountByNodeId().get(nodeIds.get(2))).as("node2 has 1 shard").isEqualTo(1);
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
        cluster().startNode(twoPathSettings);

        execute("SELECT id FROM sys.nodes");
        assertThat(response.rows().length).isEqualTo(1);
        String nodeIdWithTwoPaths = (String) response.rows()[0][0];

        // other two nodes have one data path each
        cluster().startNode(
            Settings.builder()
                .put(Environment.PATH_DATA_SETTING.getKey(), createTempDir())
                .put(CLUSTER_ROUTING_ALLOCATION_REROUTE_INTERVAL_SETTING.getKey(), "1ms"));
        cluster().startNode(
            Settings.builder()
                .put(Environment.PATH_DATA_SETTING.getKey(), createTempDir())
                .put(CLUSTER_ROUTING_ALLOCATION_REROUTE_INTERVAL_SETTING.getKey(), "1ms"));
        List<String> nodeIds = getNodeIds();

        var clusterInfoService = getMockInternalClusterInfoService();
        // prevent any effects from in-flight recoveries, since we are only simulating a 100-byte disk
        clusterInfoService.setShardSizeFunctionAndRefresh(shardRouting -> 0L);
        // start with all paths below the watermark
        clusterInfoService.setDiskUsageFunctionAndRefresh((discoveryNode, fsInfoPath) ->
            setDiskUsage(fsInfoPath, 100, between(10, 100)));

        execute("SET GLOBAL TRANSIENT" +
                "   cluster.routing.allocation.disk.watermark.low='90%'," +
                "   cluster.routing.allocation.disk.watermark.high='90%'," +
                "   cluster.routing.allocation.disk.watermark.flood_stage='100%'");

        execute("CREATE TABLE doc.t (id INT PRIMARY KEY) " +
                "CLUSTERED INTO 6 SHARDS " +
                "WITH (number_of_replicas = 0)");
        ensureGreen();

        var shardCountByNodeId = getShardCountByNodeId();
        assertThat(shardCountByNodeId.get(nodeIds.get(0))).as("node0 has 2 shards").isEqualTo(2);
        assertThat(shardCountByNodeId.get(nodeIds.get(1))).as("node1 has 2 shards").isEqualTo(2);
        assertThat(shardCountByNodeId.get(nodeIds.get(2))).as("node2 has 2 shards").isEqualTo(2);

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
                .count()).isEqualTo(1L);

        // disable rebalancing, or else we might move shards back
        // onto the over-full path since we're not faking that
        execute("SET GLOBAL TRANSIENT cluster.routing.rebalance.enable='none'");

        // one of the paths on node0 suddenly exceeds the high watermark
        clusterInfoService.setDiskUsageFunctionAndRefresh((discoveryNode, fsInfoPath) -> setDiskUsage(
            fsInfoPath, 100L,
            fsInfoPath.getPath().startsWith(pathOverWatermark) ? between(0, 9) : between(10, 100)));

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
                    .count()).isEqualTo(0L);
        });
    }

    @UseRandomizedOptimizerRules(0)
    @Test
    @UseRandomizedSchema(random = false)
    @UseJdbc(0) // need execute to throw ClusterBlockException instead of PGException
    public void testAutomaticReleaseOfIndexBlock() throws Exception {
        for (int i = 0; i < 3; i++) {
            // ensure that each node has a single data path
            cluster().startNode(Settings.builder().put(Environment.PATH_DATA_SETTING.getKey(), createTempDir()));
        }

        ClusterStateResponse clusterStateResponse = client().admin().cluster().state(new ClusterStateRequest()).get();
        final List<String> nodeIds = StreamSupport.stream(clusterStateResponse.getState()
            .getRoutingNodes().spliterator(), false).map(RoutingNode::nodeId).collect(Collectors.toList());

        // Start with all nodes at 50% usage
        final MockInternalClusterInfoService cis = (MockInternalClusterInfoService)
            cluster().getInstance(ClusterInfoService.class, cluster().getMasterName());
        cis.setUpdateFrequency(TimeValue.timeValueMillis(100));

        // prevent any effects from in-flight recoveries, since we are only simulating a 100-byte disk
        cis.setShardSizeFunctionAndRefresh(shardRouting -> 0L);

        // start with all nodes below the low watermark
        cis.setDiskUsageFunctionAndRefresh((discoveryNode, fsInfoPath) -> setDiskUsage(fsInfoPath, 100, between(15, 100)));

        final boolean watermarkBytes = randomBoolean(); // we have to consistently use bytes or percentage for the disk watermark settings
        execute("""
            set global transient
                cluster.routing.allocation.disk.watermark.low = ?,
                cluster.routing.allocation.disk.watermark.high = ?,
                cluster.routing.allocation.disk.watermark.flood_stage = ?
            """,
            new Object[] {
                watermarkBytes ? "15b" : "85%",
                watermarkBytes ? "10b" : "90%",
                watermarkBytes ? "5b" : "95%"
            }
        );
        // reroute_interval is not exposed
        var updateSettingsRequest = new ClusterUpdateSettingsRequest()
            .transientSettings(Settings.builder()
                .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_REROUTE_INTERVAL_SETTING.getKey(), "150ms")
            );
        client().admin().cluster().execute(ClusterUpdateSettingsAction.INSTANCE, updateSettingsRequest).get();


        // Create an index with 6 shards so we can check allocation for it
        execute("create table test (id int primary key, foo text) " +
                "clustered into 6 shards with (number_of_replicas = 0)");
        ensureGreen();


        {
            final Map<String, Integer> shardCountByNodeId = getShardCountByNodeId();
            assertThat(shardCountByNodeId.get(nodeIds.get(0))).as("node0 has 2 shards").isEqualTo(2);
            assertThat(shardCountByNodeId.get(nodeIds.get(1))).as("node1 has 2 shards").isEqualTo(2);
            assertThat(shardCountByNodeId.get(nodeIds.get(2))).as("node2 has 2 shards").isEqualTo(2);
        }

        execute("insert into test (id, foo) values (1, 'bar')");
        execute("refresh table test");
        assertThat(execute("select * from test").rowCount()).isEqualTo(1L);

        // Move all nodes above the low watermark so no shard movement can occur, and at least one node above the flood stage watermark so
        // the index is blocked
        cis.setDiskUsageFunctionAndRefresh((discoveryNode, fsInfoPath) -> setDiskUsage(
            fsInfoPath,
            100,
            discoveryNode.getId().equals(nodeIds.get(2)) ? between(0, 4) : between(0, 9)
        ));

        assertBusy(() -> {
            assertBlocked(
                () -> execute("insert into test (id, foo) values (1, 'bar') on conflict (id) do nothing"),
                IndexMetadata.INDEX_READ_ONLY_ALLOW_DELETE_BLOCK
            );
        });

        assertThat(FutureUtils.get(client().admin().cluster()
            .health(new ClusterHealthRequest("test").waitForEvents(Priority.LANGUID))).isTimedOut()).isFalse();

        // Cannot add further documents
        assertBlocked(
            () -> execute("insert into test (id, foo) values (2, 'bar') on conflict (id) do nothing"),
            IndexMetadata.INDEX_READ_ONLY_ALLOW_DELETE_BLOCK
        );
        assertThat(execute("select * from test").rowCount()).isEqualTo(1L);

        // Move all nodes below the high watermark so that the index is unblocked
        cis.setDiskUsageFunctionAndRefresh((discoveryNode, fsInfoPath) -> setDiskUsage(fsInfoPath, 100, between(10, 100)));

        // Attempt to create a new document until DiskUsageMonitor unblocks the index
        assertBusy(() -> {
            try {
                execute("insert into test (id, foo) values (3, 'bar') on conflict (id) do nothing");
            } catch (ClusterBlockException e) {
                throw new AssertionError("retrying", e);
            }
            execute("refresh table test");
        });
        assertThat(TestingHelpers.printedTable(execute("select id from test order by 1 asc").rows())).isEqualTo(
            "1\n" +
            "3\n"
        );
    }

    private void assertBlocked(Runnable runnable, ClusterBlock clusterBlock) {
        try {
            runnable.run();
            fail("Expected ClusterBlockException");
        } catch (ClusterBlockException e) {
            assertThat(e.blocks().size(), greaterThan(0));
            for (var block : e.blocks()) {
                assertThat(block.id()).isEqualTo(clusterBlock.id());
            }
        }
    }

    private static ClusterState clusterState() {
        return FutureUtils.get(client().admin().cluster().state(new ClusterStateRequest())).getState();
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
        return (MockInternalClusterInfoService) cluster().getCurrentMasterNodeInstance(ClusterInfoService.class);
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
