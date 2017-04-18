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
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.LocalTransportAddress;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.discovery.DiscoverySettings;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

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
            THREAD_POOL,
            () -> discoveryNode);
        clusterService.setNodeConnectionsService(new NodeConnectionsService(Settings.EMPTY, null, null) {
            @Override
            public void connectToNodes(Iterable<DiscoveryNode> discoveryNodes) {
                // skip
            }

            @Override
            public void disconnectFromNodesExcept(Iterable<DiscoveryNode> nodesToKeep) {
                // skip
            }
        });
        clusterService.setClusterStatePublisher((event, ackListener) -> {
        });
        clusterService.setDiscoverySettings(new DiscoverySettings(Settings.EMPTY,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)));
        clusterService.start();
        final DiscoveryNodes.Builder nodes = DiscoveryNodes.builder(clusterService.state().nodes());
        nodes.masterNodeId(clusterService.localNode().getId());
        ClusterServiceUtils.setState(clusterService, ClusterState.builder(clusterService.state()).nodes(nodes));
        return clusterService;
    }
}
