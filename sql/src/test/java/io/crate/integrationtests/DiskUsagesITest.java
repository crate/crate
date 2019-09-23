/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.integrationtests;

import org.elasticsearch.cluster.ClusterInfoService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.DiskUsage;
import org.elasticsearch.cluster.MockInternalClusterInfoService;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.stream.StreamSupport;

import static java.util.stream.Collectors.toList;
import static org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_REROUTE_INTERVAL_SETTING;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class DiskUsagesITest extends SQLTransportIntegrationTest {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        ArrayList<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        // Use the mock internal cluster info service, which has fake-able disk usages
        plugins.add(MockInternalClusterInfoService.TestPlugin.class);
        return plugins;
    }

    public void testRerouteOccursOnDiskPassingHighWatermark() throws Exception {
        for (int i = 0; i < 3; i++) {
            // ensure that each node has a single data path
            internalCluster().startNode(
                Settings.builder()
                    .put(Environment.PATH_DATA_SETTING.getKey(), createTempDir())
                    .put(CLUSTER_ROUTING_ALLOCATION_REROUTE_INTERVAL_SETTING.getKey(), "1ms"));
        }

        List<String> nodeIds = StreamSupport.stream(clusterState().getRoutingNodes().spliterator(), false)
            .map(RoutingNode::nodeId)
            .collect(toList());

        // Start with all nodes at 50% usage
        var clusterInfoService = (MockInternalClusterInfoService)
            internalCluster().getInstance(ClusterInfoService.class, internalCluster().getMasterName());
        clusterInfoService.setUpdateFrequency(TimeValue.timeValueMillis(200));
        clusterInfoService.onMaster();
        clusterInfoService.setN1Usage(nodeIds.get(0), new DiskUsage(nodeIds.get(0), "n1", "/dev/null", 100, 50));
        clusterInfoService.setN2Usage(nodeIds.get(1), new DiskUsage(nodeIds.get(1), "n2", "/dev/null", 100, 50));
        clusterInfoService.setN3Usage(nodeIds.get(2), new DiskUsage(nodeIds.get(2), "n3", "/dev/null", 100, 50));

        execute("SET GLOBAL TRANSIENT" +
                "   cluster.routing.allocation.disk.watermark.low='80%'," +
                "   cluster.routing.allocation.disk.watermark.high='90%'," +
                "   cluster.routing.allocation.disk.watermark.flood_stage='100%'");

        // Create a table with 10 shards so we can check allocation for it
        execute("CREATE TABLE t (id INT PRIMARY KEY) " +
                "CLUSTERED INTO 10 SHARDS " +
                "WITH (number_of_replicas = 0)");
        ensureGreen();

        assertBusy(() -> {
            HashMap<String, Integer> shardCountByNodeId = getShardCountByNodeId();
            assertThat("node0 has at least 3 shards", shardCountByNodeId.get(nodeIds.get(0)), greaterThanOrEqualTo(3));
            assertThat("node1 has at least 3 shards", shardCountByNodeId.get(nodeIds.get(1)), greaterThanOrEqualTo(3));
            assertThat("node2 has at least 3 shards", shardCountByNodeId.get(nodeIds.get(2)), greaterThanOrEqualTo(3));
        });

        // move node3 above high watermark
        clusterInfoService.setN1Usage(nodeIds.get(0), new DiskUsage(nodeIds.get(0), "n1", "_na_", 100, 50));
        clusterInfoService.setN2Usage(nodeIds.get(1), new DiskUsage(nodeIds.get(1), "n2", "_na_", 100, 50));
        clusterInfoService.setN3Usage(nodeIds.get(2), new DiskUsage(nodeIds.get(2), "n3", "_na_", 100, 0));

        assertBusy(() -> {
            HashMap<String, Integer> shardCountByNodeId = getShardCountByNodeId();
            assertThat("node0 has 5 shards", shardCountByNodeId.get(nodeIds.get(0)), equalTo(5));
            assertThat("node1 has 5 shards", shardCountByNodeId.get(nodeIds.get(1)), equalTo(5));
            assertThat("node2 has 0 shards", shardCountByNodeId.get(nodeIds.get(2)), equalTo(0));
        });

        // move all nodes below watermark again
        clusterInfoService.setN1Usage(nodeIds.get(0), new DiskUsage(nodeIds.get(0), "n1", "_na_", 100, 50));
        clusterInfoService.setN2Usage(nodeIds.get(1), new DiskUsage(nodeIds.get(1), "n2", "_na_", 100, 50));
        clusterInfoService.setN3Usage(nodeIds.get(2), new DiskUsage(nodeIds.get(2), "n3", "_na_", 100, 50));

        assertBusy(() -> {
            HashMap<String, Integer> shardCountByNodeId = getShardCountByNodeId();
            assertThat("node0 has at least 3 shards", shardCountByNodeId.get(nodeIds.get(0)), greaterThanOrEqualTo(3));
            assertThat("node1 has at least 3 shards", shardCountByNodeId.get(nodeIds.get(1)), greaterThanOrEqualTo(3));
            assertThat("node2 has at least 3 shards", shardCountByNodeId.get(nodeIds.get(2)), greaterThanOrEqualTo(3));
        });
    }

    static private ClusterState clusterState() {
        return client().admin().cluster().prepareState().get().getState();
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
