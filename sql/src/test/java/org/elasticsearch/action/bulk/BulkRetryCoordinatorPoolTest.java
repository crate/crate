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
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.*;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.transport.DummyTransportAddress;
import org.elasticsearch.index.IndexShardMissingException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndexMissingException;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static org.hamcrest.Matchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BulkRetryCoordinatorPoolTest extends CrateUnitTest {

    private static final String[] NODE_IDS = new String[] {"node1", "node2"};

    public static final String TEST_INDEX = "test_index";

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    BulkRetryCoordinatorPool pool;
    ClusterState state;

    @Before
    public void prepare() {
        state = mock(ClusterState.class);
        IndexRoutingTable.Builder builder = IndexRoutingTable.builder(TEST_INDEX);
        for (int i = 0; i < 3; i++) {
            builder.addIndexShard(new IndexShardRoutingTable.Builder(new ShardId(TEST_INDEX, i), true)
                    .addShard(new ImmutableShardRouting(TEST_INDEX, i, NODE_IDS[i%2], true, ShardRoutingState.STARTED, 1))
                    .build()).addReplica();
        }
        RoutingTable routingTable = RoutingTable.builder().add(builder).build();
        DiscoveryNodes discoveryNodes = DiscoveryNodes.builder()
                .localNodeId(NODE_IDS[0])
                .put(new DiscoveryNode(NODE_IDS[0], DummyTransportAddress.INSTANCE, Version.CURRENT))
                .put(new DiscoveryNode(NODE_IDS[1], DummyTransportAddress.INSTANCE, Version.CURRENT))
                .build();
        when(state.nodes()).thenReturn(discoveryNodes);
        when(state.routingTable()).thenReturn(routingTable);
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenAnswer(new Answer<ClusterState>() {
            @Override
            public ClusterState answer(InvocationOnMock invocation) throws Throwable {
                return state;
            }
        });
        pool = new BulkRetryCoordinatorPool(ImmutableSettings.EMPTY, clusterService);
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
        expectedException.expect(IndexMissingException.class);
        expectedException.expectMessage("[unknown] missing");

        ShardId shardId = new ShardId("unknown", 42);
        pool.coordinator(shardId);
    }

    @Test
    public void testUnknownShard() throws Exception {
        expectedException.expect(IndexShardMissingException.class);
        expectedException.expectMessage("[test_index][42] missing");

        ShardId shardId = new ShardId(TEST_INDEX, 42);
        pool.coordinator(shardId);
    }

    @Test
    public void testReturnDifferentCoordinatorForRelocatedShardFromRemovedNode() throws Exception {
        ShardId shardId = new ShardId(TEST_INDEX, 1);
        BulkRetryCoordinator coordinator = pool.coordinator(shardId);

        ClusterState oldState = state;

        state = mock(ClusterState.class);
        IndexRoutingTable.Builder builder = IndexRoutingTable.builder(TEST_INDEX);
        for (int i = 0; i < 3; i++) {
            builder.addIndexShard(new IndexShardRoutingTable.Builder(new ShardId(TEST_INDEX, i), true)
                    .addShard(new ImmutableShardRouting(TEST_INDEX, i, NODE_IDS[0], true, ShardRoutingState.STARTED, 1))
                    .build()).addReplica();
        }
        RoutingTable routingTable = RoutingTable.builder().add(builder).build();
        DiscoveryNodes discoveryNodes = DiscoveryNodes.builder()
                .localNodeId(NODE_IDS[0])
                .put(new DiscoveryNode(NODE_IDS[0], DummyTransportAddress.INSTANCE, Version.CURRENT))
                .build();
        when(state.nodes()).thenReturn(discoveryNodes);
        when(state.routingTable()).thenReturn(routingTable);

        pool.clusterChanged(new ClusterChangedEvent("bla", state, oldState));

        BulkRetryCoordinator otherCoordinator = pool.coordinator(shardId);
        assertThat(coordinator, is(not(otherCoordinator)));
    }
}
