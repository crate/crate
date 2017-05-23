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

package io.crate.test.integration;

import com.google.common.collect.ImmutableSet;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.NodeConnectionsService;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.LocalTransportAddress;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.mockito.Mockito;

import java.util.*;
import java.util.concurrent.TimeUnit;

public class CrateDummyClusterServiceUnitTest extends CrateUnitTest {

    public static final String NODE_ID = "node";

    private static final Set<Setting<?>> EMPTY_CLUSTER_SETTINGS = ImmutableSet.of();

    protected static ThreadPool THREAD_POOL;
    protected ClusterService clusterService;

    @BeforeClass
    public static void setupThreadPool() {
        THREAD_POOL = new TestThreadPool(Thread.currentThread().getName());
    }

    @AfterClass
    public static void shutdownThreadPool() {
        ThreadPool.terminate(THREAD_POOL, 30, TimeUnit.SECONDS);
    }

    @Before
    public void setupDummyClusterService() {
        clusterService = createClusterService(additionalClusterSettings());
    }

    @After
    public void cleanup() {
        clusterService.close();
    }

    /**
     * Override this method to provide additional cluster settings.
     */
    protected Collection<Setting<?>> additionalClusterSettings() {
        return EMPTY_CLUSTER_SETTINGS;
    }

    /**
     * Updates the routing state of the ClusterService to make the indices available.
     * @param indices indices to create routings for
     */
    public void prepareRoutingForIndices(List<String> indices) {

        final String node2 = "node2";

        final RoutingTable.Builder routingTableBuilder = RoutingTable.builder();
        final MetaData.Builder metadataBuilder = MetaData.builder();
        for (String indexName : indices) {

            String indexUUID = UUID.randomUUID().toString();
            ShardId shardId1 = new ShardId(indexName, indexUUID, 0);
            ShardId shardId2 = new ShardId(indexName, indexUUID, 1);

            ShardRouting shardRouting1 = ShardRouting.newUnassigned(
                shardId1,
                true,
                Mockito.mock(RecoverySource.class),
                new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "bla"))
                .initialize(node2, null, 1);

            IndexShardRoutingTable shardRouting =
                new IndexShardRoutingTable.Builder(shardId1)
                    .addShard(shardRouting1)
                    .build();

            Index index = new Index(indexName, indexUUID);
            IndexRoutingTable indexRoutingTable =
                IndexRoutingTable
                    .builder(index)
                    .addIndexShard(shardRouting)
                    .build();

            Map<String, IndexRoutingTable> objectObjectHashMap = new HashMap<>();
            objectObjectHashMap.put(indexName, indexRoutingTable);
            routingTableBuilder.indicesRouting(objectObjectHashMap);

            IndexMetaData indexMetaData = IndexMetaData
                .builder(indexName)
                .settings(Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT).build())
                .numberOfShards(1)
                .numberOfReplicas(5)
                .creationDate(0)
                .build();

            metadataBuilder.put(indexMetaData, false);
        }

        // update cluster state for primary key lookup tests
        ClusterServiceUtils.setState(
            clusterService,
            ClusterState
                .builder(clusterService.state())
                .routingTable(routingTableBuilder.build())
                .metaData(metadataBuilder.build())
                .build()
        );
    }

    private ClusterService createClusterService(Collection<Setting<?>> additionalClusterSettings) {
        Set<Setting<?>> clusterSettings = Sets.newHashSet(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        clusterSettings.addAll(additionalClusterSettings);
        DiscoveryNode discoveryNode = new DiscoveryNode(
            "node-name",
            NODE_ID,
            LocalTransportAddress.buildUnique(),
            Collections.emptyMap(),
            new HashSet<>(Arrays.asList(DiscoveryNode.Role.values())),
            Version.CURRENT
        );
        ClusterService clusterService = new ClusterService(Settings.builder().put("cluster.name", "ClusterServiceTests").build(),
            new ClusterSettings(Settings.EMPTY, clusterSettings),
            THREAD_POOL);
        clusterService.setLocalNode(discoveryNode);
        clusterService.setNodeConnectionsService(new NodeConnectionsService(Settings.EMPTY, null, null) {
            @Override
            public void connectToNodes(List<DiscoveryNode> addedNodes) {
                // skip
            }

            @Override
            public void disconnectFromNodes(List<DiscoveryNode> removedNodes) {
                // skip
            }
        });
        clusterService.setClusterStatePublisher((event, ackListener) -> {
        });
        clusterService.start();
        final DiscoveryNodes.Builder nodes = DiscoveryNodes.builder(clusterService.state().nodes());
        nodes.masterNodeId(clusterService.localNode().getId());

        ClusterServiceUtils.setState(
            clusterService,
            ClusterState
                .builder(clusterService.state())
                .nodes(nodes));
        return clusterService;
    }
}
