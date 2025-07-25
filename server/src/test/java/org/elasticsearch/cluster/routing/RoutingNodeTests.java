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

package org.elasticsearch.cluster.routing;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.assertj.core.api.Assertions.assertThat;

import java.net.InetAddress;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;

public class RoutingNodeTests extends ESTestCase {
    private ShardRouting unassignedShard0 =
        TestShardRouting.newShardRouting("test", 0, "node-1", false, ShardRoutingState.STARTED);
    private ShardRouting initializingShard0 =
        TestShardRouting.newShardRouting("test", 1, "node-1", false, ShardRoutingState.INITIALIZING);
    private ShardRouting relocatingShard0 =
        TestShardRouting.newShardRouting("test", 2, "node-1", "node-2", false, ShardRoutingState.RELOCATING);
    private RoutingNode routingNode;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        InetAddress inetAddress = InetAddress.getByAddress("name1", new byte[] { (byte) 192, (byte) 168, (byte) 0, (byte) 1});
        TransportAddress transportAddress = new TransportAddress(inetAddress, randomIntBetween(0, 65535));
        DiscoveryNode discoveryNode = new DiscoveryNode("name1", "node-1", transportAddress, emptyMap(), emptySet(), Version.CURRENT);
        routingNode = new RoutingNode("node1", discoveryNode, unassignedShard0, initializingShard0, relocatingShard0);
    }

    public void testAdd() {
        ShardRouting initializingShard1 =
            TestShardRouting.newShardRouting("test", 3, "node-1", false, ShardRoutingState.INITIALIZING);
        ShardRouting relocatingShard0 =
            TestShardRouting.newShardRouting("test", 4, "node-1", "node-2",false, ShardRoutingState.RELOCATING);
        routingNode.add(initializingShard1);
        routingNode.add(relocatingShard0);
        assertThat(routingNode.getByShardId(new ShardId(IndexMetadata.INDEX_NAME_NA_VALUE, "test", 3))).isEqualTo(initializingShard1);
        assertThat(routingNode.getByShardId(new ShardId(IndexMetadata.INDEX_NAME_NA_VALUE, "test", 4))).isEqualTo(relocatingShard0);
    }

    public void testUpdate() {
        ShardRouting startedShard0 =
            TestShardRouting.newShardRouting("test", 0, "node-1", false, ShardRoutingState.STARTED);
        ShardRouting startedShard1 =
            TestShardRouting.newShardRouting("test", 1, "node-1", "node-2",false, ShardRoutingState.RELOCATING);
        ShardRouting startedShard2 =
            TestShardRouting.newShardRouting("test", 2, "node-1", false, ShardRoutingState.INITIALIZING);
        routingNode.update(unassignedShard0, startedShard0);
        routingNode.update(initializingShard0, startedShard1);
        routingNode.update(relocatingShard0, startedShard2);
        assertThat(routingNode.getByShardId(new ShardId(IndexMetadata.INDEX_NAME_NA_VALUE, "test", 0)).state()).isEqualTo(ShardRoutingState.STARTED);
        assertThat(routingNode.getByShardId(new ShardId(IndexMetadata.INDEX_NAME_NA_VALUE, "test", 1)).state()).isEqualTo(ShardRoutingState.RELOCATING);
        assertThat(routingNode.getByShardId(new ShardId(IndexMetadata.INDEX_NAME_NA_VALUE, "test", 2)).state()).isEqualTo(ShardRoutingState.INITIALIZING);
    }

    public void testRemove() {
        routingNode.remove(unassignedShard0);
        routingNode.remove(initializingShard0);
        routingNode.remove(relocatingShard0);
        assertThat(routingNode.getByShardId(new ShardId(IndexMetadata.INDEX_NAME_NA_VALUE, "test", 0))).isNull();
        assertThat(routingNode.getByShardId(new ShardId(IndexMetadata.INDEX_NAME_NA_VALUE, "test", 1))).isNull();
        assertThat(routingNode.getByShardId(new ShardId(IndexMetadata.INDEX_NAME_NA_VALUE, "test", 2))).isNull();
    }

    public void testNumberOfShardsWithState() {
        assertThat(routingNode.numberOfShardsWithState(ShardRoutingState.INITIALIZING, ShardRoutingState.STARTED)).isEqualTo(2);
        assertThat(routingNode.numberOfShardsWithState(ShardRoutingState.STARTED)).isEqualTo(1);
        assertThat(routingNode.numberOfShardsWithState(ShardRoutingState.RELOCATING)).isEqualTo(1);
        assertThat(routingNode.numberOfShardsWithState(ShardRoutingState.INITIALIZING)).isEqualTo(1);
    }

    public void testShardsWithState() {
        assertThat(routingNode.shardsWithState(ShardRoutingState.INITIALIZING, ShardRoutingState.STARTED)).hasSize(2);
        assertThat(routingNode.shardsWithState(ShardRoutingState.STARTED)).hasSize(1);
        assertThat(routingNode.shardsWithState(ShardRoutingState.RELOCATING)).hasSize(1);
        assertThat(routingNode.shardsWithState(ShardRoutingState.INITIALIZING)).hasSize(1);
    }

    public void testShardsWithStateInIndex() {
        assertThat(routingNode.shardsWithState("test", ShardRoutingState.INITIALIZING, ShardRoutingState.STARTED)).hasSize(2);
        assertThat(routingNode.shardsWithState("test", ShardRoutingState.STARTED)).hasSize(1);
        assertThat(routingNode.shardsWithState("test", ShardRoutingState.RELOCATING)).hasSize(1);
        assertThat(routingNode.shardsWithState("test", ShardRoutingState.INITIALIZING)).hasSize(1);
    }

    public void testNumberOfOwningShards() {
        assertThat(routingNode.numberOfOwningShards()).isEqualTo(2);
    }

    public void testNumberOfOwningShardsForIndex() {
        final ShardRouting test1Shard0 =
            TestShardRouting.newShardRouting("test1", 0, "node-1", false, ShardRoutingState.STARTED);
        final ShardRouting test2Shard0 =
            TestShardRouting.newShardRouting("test2", 0, "node-1", "node-2", false, ShardRoutingState.RELOCATING);
        routingNode.add(test1Shard0);
        routingNode.add(test2Shard0);
        assertThat(routingNode.numberOfOwningShardsForIndex(new Index(IndexMetadata.INDEX_NAME_NA_VALUE, "test"))).isEqualTo(2);
        assertThat(routingNode.numberOfOwningShardsForIndex(new Index(IndexMetadata.INDEX_NAME_NA_VALUE, "test1"))).isEqualTo(1);
        assertThat(routingNode.numberOfOwningShardsForIndex(new Index(IndexMetadata.INDEX_NAME_NA_VALUE, "test2"))).isEqualTo(0);
        assertThat(routingNode.numberOfOwningShardsForIndex(new Index(IndexMetadata.INDEX_NAME_NA_VALUE, "test3"))).isEqualTo(0);
    }

}
