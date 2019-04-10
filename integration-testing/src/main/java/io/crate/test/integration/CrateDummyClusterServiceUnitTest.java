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
import org.elasticsearch.cluster.service.ClusterApplierService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.node.Node;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.ClusterServiceUtils.createClusterStatePublisher;

public class CrateDummyClusterServiceUnitTest extends CrateUnitTest {

    public static final String NODE_ID = "n1";
    public static final String NODE_NAME = "node-name";

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
        Set<Setting<?>> clusterSettingsSet = Sets.newHashSet(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        clusterSettingsSet.addAll(additionalClusterSettings);
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, clusterSettingsSet);
        ClusterService clusterService = new ClusterService(
            Settings.builder()
                .put("cluster.name", "ClusterServiceTests")
                .put(Node.NODE_NAME_SETTING.getKey(), NODE_NAME)
                .build(),
            clusterSettings,
            THREAD_POOL
        );
        clusterService.setNodeConnectionsService(new NodeConnectionsService(Settings.EMPTY, null, null) {

            @Override
            public void connectToNodes(DiscoveryNodes discoveryNodes, Runnable onCompletion) {
                onCompletion.run();
            }

            @Override
            public void disconnectFromNodesExcept(DiscoveryNodes nodesToKeep) {
                // skip
            }
        });
        DiscoveryNode discoveryNode = new DiscoveryNode(
            NODE_NAME,
            NODE_ID,
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            new HashSet<>(Arrays.asList(DiscoveryNode.Role.values())),
            Version.CURRENT
        );
        DiscoveryNodes nodes = DiscoveryNodes.builder()
            .add(discoveryNode)
            .localNodeId(NODE_ID)
            .masterNodeId(NODE_ID)
            .build();
        ClusterState clusterState = ClusterState.builder(ClusterState.EMPTY_STATE).nodes(nodes).build();

        ClusterApplierService clusterApplierService = clusterService.getClusterApplierService();
        clusterApplierService.setInitialState(clusterState);

        MasterService masterService = clusterService.getMasterService();
        masterService.setClusterStatePublisher(createClusterStatePublisher(clusterApplierService));
        masterService.setClusterStateSupplier(clusterApplierService::state);

        clusterService.start();
        return clusterService;
    }
}
