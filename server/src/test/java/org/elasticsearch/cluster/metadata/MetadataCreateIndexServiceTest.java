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

package org.elasticsearch.cluster.metadata;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Collections;
import java.util.List;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.shrink.ResizeRequest;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.EmptyClusterInfoService;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.routing.allocation.decider.MaxRetryAllocationDecider;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.ShardLimitValidator;
import org.elasticsearch.snapshots.EmptySnapshotsInfoService;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.test.gateway.TestGatewayAllocator;
import org.junit.Test;
import org.mockito.Mockito;

import io.crate.metadata.RelationName;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;

public class MetadataCreateIndexServiceTest extends CrateDummyClusterServiceUnitTest {


    public AllocationService createAllocationService() throws Exception {
        return new AllocationService(
            new AllocationDeciders(Collections.singleton(new MaxRetryAllocationDecider())),
            new TestGatewayAllocator(),
            new BalancedShardsAllocator(Settings.EMPTY),
            EmptyClusterInfoService.INSTANCE,
            EmptySnapshotsInfoService.INSTANCE);
    }

    @Test
    public void testCalculateNumRoutingShards() {
        assertThat(MetadataCreateIndexService.calculateNumRoutingShards(1, Version.CURRENT)).isEqualTo(1024);
        assertThat(MetadataCreateIndexService.calculateNumRoutingShards(2, Version.CURRENT)).isEqualTo(1024);
        assertThat(MetadataCreateIndexService.calculateNumRoutingShards(3, Version.CURRENT)).isEqualTo(768);
        assertThat(MetadataCreateIndexService.calculateNumRoutingShards(9, Version.CURRENT)).isEqualTo(576);
        assertThat(MetadataCreateIndexService.calculateNumRoutingShards(512, Version.CURRENT)).isEqualTo(1024);
        assertThat(MetadataCreateIndexService.calculateNumRoutingShards(1024, Version.CURRENT)).isEqualTo(2048);
        assertThat(MetadataCreateIndexService.calculateNumRoutingShards(2048, Version.CURRENT)).isEqualTo(4096);

        Version latestBeforeChange = VersionUtils.getPreviousVersion(Version.V_5_8_0);
        int numShards = randomIntBetween(1, 1000);
        assertThat(MetadataCreateIndexService.calculateNumRoutingShards(numShards, latestBeforeChange)).isEqualTo(numShards);
        assertThat(MetadataCreateIndexService.calculateNumRoutingShards(numShards,
            VersionUtils.randomVersionBetween(random(), VersionUtils.getFirstVersion(), latestBeforeChange))).isEqualTo(numShards);

        for (int i = 0; i < 1000; i++) {
            int randomNumShards = randomIntBetween(1, 10000);
            int numRoutingShards = MetadataCreateIndexService.calculateNumRoutingShards(randomNumShards, Version.CURRENT);
            if (numRoutingShards <= 1024) {
                assertThat(randomNumShards).isLessThanOrEqualTo(512);
                assertThat(numRoutingShards).isGreaterThan(512);
            } else {
                assertThat(numRoutingShards / 2).isEqualTo(randomNumShards);
            }

            double ratio = numRoutingShards / (double) randomNumShards;
            int intRatio = (int) ratio;
            assertThat((double)(intRatio)).isEqualTo(ratio);
            assertThat(ratio).isGreaterThan(1);
            assertThat(ratio).isLessThanOrEqualTo(1024);
            assertThat(intRatio % 2).isZero();
            assertThat(intRatio).as("ratio is not a power of two").isEqualTo(Integer.highestOneBit(intRatio));
        }
    }

    @Test
    public void test_resize_with_too_many_docs_fails() throws Exception {
        SQLExecutor e = SQLExecutor.of(clusterService);
        e.addTable("create table doc.srctbl (x int) clustered into 2 shards with (\"blocks.write\" = true)");
        var state = clusterService.state();
        var allocationService = createAllocationService();
        var routingTable = allocationService.reroute(state, "reroute").routingTable();
        ClusterState startedShardsState = allocationService.applyStartedShards(
            state,
            routingTable.index("srctbl").shardsWithState(ShardRoutingState.INITIALIZING)
        );

        RelationName tbl = new RelationName("doc", "srctbl");
        int newNumShards = 1;
        var resizeIndexTask = new MetadataCreateIndexService.ResizeIndexTask(
            Mockito.mock(AllocationService.class),
            new ResizeRequest(tbl, List.of(), newNumShards),
            Mockito.mock(IndicesService.class),
            _ -> Integer.MAX_VALUE,
            new ShardLimitValidator(Settings.EMPTY, clusterService),
            IndexScopedSettings.DEFAULT_SCOPED_SETTINGS
        );

        assertThatThrownBy(() -> resizeIndexTask.execute(startedShardsState))
            .isExactlyInstanceOf(IllegalStateException.class)
            .hasMessageContaining("Can't merge index with more than [2147483519] docs");
    }
}
