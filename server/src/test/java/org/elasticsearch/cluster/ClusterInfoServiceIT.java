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

package org.elasticsearch.cluster;

import static org.assertj.core.api.Assertions.assertThat;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.IntegTestCase;
import org.elasticsearch.test.TestCluster;
import org.junit.Test;

import com.carrotsearch.hppc.cursors.ObjectCursor;

import io.crate.common.unit.TimeValue;
import io.crate.testing.UseRandomizedSchema;

/**
 * Integration tests for the ClusterInfoService collecting information
 */
@IntegTestCase.ClusterScope(scope = IntegTestCase.Scope.TEST, numDataNodes = 0)
public class ClusterInfoServiceIT extends IntegTestCase {

    @Test
    @UseRandomizedSchema(random = false)
    public void testClusterInfoServiceCollectsInformation() throws Exception {
        cluster().startNodes(2);
        execute("""
            create table doc.test (x int)
            clustered into 1 shards
            with (
                "store.stats_refresh_interval" = 0
            )
            """
        );
        if (randomBoolean()) {
            execute("alter table test close");
        }
        ensureGreen("test");
        TestCluster internalTestCluster = cluster();
        // Get the cluster info service on the master node
        final InternalClusterInfoService infoService = (InternalClusterInfoService) internalTestCluster
            .getInstance(ClusterInfoService.class, internalTestCluster.getMasterName());
        infoService.setUpdateFrequency(TimeValue.timeValueMillis(200));
        ClusterInfo info = infoService.refresh();
        assertThat(info).as("info should not be null").isNotNull();
        ImmutableOpenMap<String, DiskUsage> leastUsages = info.getNodeLeastAvailableDiskUsages();
        ImmutableOpenMap<String, DiskUsage> mostUsages = info.getNodeMostAvailableDiskUsages();
        ImmutableOpenMap<String, Long> shardSizes = info.shardSizes;
        assertThat(leastUsages).isNotNull();
        assertThat(shardSizes).isNotNull();
        assertThat(leastUsages.values())
            .as("some usages are populated")
            .hasSize(2);
        assertThat(shardSizes.values().size()).as("some shard sizes are populated").isGreaterThan(0);
        for (ObjectCursor<DiskUsage> usage : leastUsages.values()) {
            logger.info("--> usage: {}", usage.value);
            assertThat(usage.value.getFreeBytes()).as("usage has be retrieved").isGreaterThan(0L);
        }
        for (ObjectCursor<DiskUsage> usage : mostUsages.values()) {
            logger.info("--> usage: {}", usage.value);
            assertThat(usage.value.getFreeBytes()).as("usage has be retrieved").isGreaterThan(0L);
        }
        for (ObjectCursor<Long> size : shardSizes.values()) {
            logger.info("--> shard size: {}", size.value);
            assertThat(size.value).as("shard size is greater than 0:").isGreaterThanOrEqualTo(0L);
        }
        ClusterService clusterService = internalTestCluster.getInstance(ClusterService.class, internalTestCluster.getMasterName());
        ClusterState state = clusterService.state();
        for (ShardRouting shard : state.routingTable().allShards()) {
            String dataPath = info.getDataPath(shard);
            assertThat(dataPath).isNotNull();

            String nodeId = shard.currentNodeId();
            DiscoveryNode discoveryNode = state.nodes().get(nodeId);
            IndicesService indicesService = internalTestCluster.getInstance(IndicesService.class, discoveryNode.getName());
            IndexService indexService = indicesService.indexService(shard.index());
            IndexShard indexShard = indexService.getShardOrNull(shard.id());
            assertThat(dataPath).isEqualTo(indexShard.shardPath().getRootDataPath().toString());
        }
    }
}
