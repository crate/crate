/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.elasticsearch.action.bulk;

import io.crate.test.integration.CrateUnitTest;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardNotFoundException;
import org.elasticsearch.test.cluster.NoopClusterService;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.elasticsearch.test.ESAllocationTestCase.createAllocationService;
import static org.elasticsearch.test.ESAllocationTestCase.newNode;
import static org.hamcrest.Matchers.*;
import static org.mockito.Mockito.mock;

public class BulkRetryCoordinatorPoolTest extends CrateUnitTest {

    private static final String[] NODE_IDS = new String[]{"node1", "node2"};

    public static final String TEST_INDEX = "test_index";

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    BulkRetryCoordinatorPool pool;
    ClusterState state;

    @Before
    public void prepare() {
        MetaData metaData = MetaData.builder()
            .put(IndexMetaData.builder(TEST_INDEX).settings(settings(Version.CURRENT)).numberOfShards(3).numberOfReplicas(0))
            .build();
        RoutingTable routingTable = RoutingTable.builder()
            .addAsNew(metaData.index(TEST_INDEX)).build();
        ClusterState state = ClusterState
            .builder(org.elasticsearch.cluster.ClusterName.DEFAULT)
            .metaData(metaData)
            .routingTable(routingTable)
            .build();

        state = ClusterState.builder(state).nodes(
            DiscoveryNodes.builder().put(newNode(NODE_IDS[0])).localNodeId(NODE_IDS[0])).build();

        AllocationService allocationService = createAllocationService();
        routingTable = allocationService.reroute(state, "test").routingTable();
        state = ClusterState.builder(state).routingTable(routingTable).build();

        ClusterService clusterService = new NoopClusterService(state);

        this.state = state;

        pool = new BulkRetryCoordinatorPool(Settings.EMPTY, clusterService, mock(ThreadPool.class));
        pool.start();
    }

    @After
    public void cleanUp() {
        pool.stop();
        pool.close();
        pool = null;
    }

    @Test
    public void testGetSameCoordinatorForSameShard() throws Exception {
        ShardId shardId = new ShardId(TEST_INDEX, 0);
        BulkRetryCoordinator coordinator = pool.coordinator(shardId);

        assertThat(coordinator, is(notNullValue()));

        BulkRetryCoordinator sameCoordinator = pool.coordinator(shardId);
        assertThat(coordinator, is(sameInstance(sameCoordinator)));
    }

    @Test
    public void testUnknownIndex() throws Exception {
        expectedException.expect(IndexNotFoundException.class);
        expectedException.expectMessage("no such index");

        ShardId shardId = new ShardId("unknown", 42);
        pool.coordinator(shardId);
    }

    @Test
    public void testUnknownShard() throws Exception {
        expectedException.expect(ShardNotFoundException.class);
        expectedException.expectMessage("no such shard");

        ShardId shardId = new ShardId(TEST_INDEX, 42);
        pool.coordinator(shardId);
    }

    @Test
    public void testReturnDifferentCoordinatorForRelocatedShardFromRemovedNode() throws Exception {
        ShardId shardId = new ShardId(TEST_INDEX, 1);
        BulkRetryCoordinator coordinator = pool.coordinator(shardId);

        ClusterState newState = ClusterState.builder(state).nodes(
            DiscoveryNodes.builder().put(newNode(NODE_IDS[1]))).build();

        AllocationService allocationService = createAllocationService();
        RoutingTable routingTable = allocationService.reroute(newState, "test").routingTable();
        newState = ClusterState.builder(newState).routingTable(routingTable).build();

        pool.clusterChanged(new ClusterChangedEvent("bla", newState, state));

        BulkRetryCoordinator otherCoordinator = pool.coordinator(shardId);
        assertThat(coordinator, not(sameInstance(otherCoordinator)));
    }
}
