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

package io.crate.execution.engine.collect.collectors;

import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ClusterServiceUtils;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.is;

public class ShardStateObserverTest extends CrateDummyClusterServiceUnitTest {

    @Test
    public void test_wait_for_active_shard_completes_on_shard_state_change() throws Throwable {
        // Add 2 nodes and table to cluster state
        SQLExecutor.builder(clusterService, 2, Randomness.get(), List.of())
            .addTable("create table t1 (x int) clustered into 1 shards");

        var observer = new ShardStateObserver(clusterService);
        IndexShardRoutingTable routingTable = clusterService.state().routingTable().shardRoutingTable("t1", 0);
        ShardId shardId = routingTable.shardId();
        CompletableFuture<ShardRouting> shard0Active = observer.waitForActiveShard(shardId);
        assertThat(shard0Active.isDone(), is(false));

        ShardRouting startedPrimaryShard = routingTable.primaryShard().moveToStarted();
        ClusterState newClusterState = ClusterState.builder(clusterService.state())
            .routingTable(
                RoutingTable.builder()
                    .add(IndexRoutingTable.builder(shardId.getIndex()).addShard(startedPrimaryShard).build())
                    .build()
            ).build();
        ClusterServiceUtils.setState(clusterService, newClusterState);

        // This now shouldn't timeout
        shard0Active.get(5, TimeUnit.SECONDS);
    }
}
