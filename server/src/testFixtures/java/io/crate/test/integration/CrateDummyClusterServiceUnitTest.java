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

package io.crate.test.integration;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterApplierService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.node.Node;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.ClusterServiceUtils.createClusterStatePublisher;
import static org.elasticsearch.test.ClusterServiceUtils.createNoOpNodeConnectionsService;

public class CrateDummyClusterServiceUnitTest extends ESTestCase {

    public static final String NODE_ID = "n1";
    public static final String NODE_NAME = "node-name";

    private static final Set<Setting<?>> EMPTY_CLUSTER_SETTINGS = Set.of();

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
        clusterService = createClusterService(additionalClusterSettings().stream().filter(Setting::hasNodeScope).toList(), Version.CURRENT);
    }

    @After
    public void cleanup() {
        clusterService.close();
    }

    /**
     * Reset the current cluster service (state) to be able to re-build existing tables.
     */
    public void resetClusterService() {
        cleanup();
        setupDummyClusterService();
    }

    /**
     * Override this method to provide additional cluster settings.
     */
    protected Collection<Setting<?>> additionalClusterSettings() {
        return EMPTY_CLUSTER_SETTINGS;
    }

    protected ClusterService createClusterService(Collection<Setting<?>> additionalClusterSettings, Version version) {
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
        clusterService.setNodeConnectionsService(createNoOpNodeConnectionsService());
        DiscoveryNode discoveryNode = new DiscoveryNode(
            NODE_NAME,
            NODE_ID,
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            DiscoveryNodeRole.BUILT_IN_ROLES,
            version
        );
        DiscoveryNodes nodes = DiscoveryNodes.builder()
            .add(discoveryNode)
            .localNodeId(NODE_ID)
            .masterNodeId(NODE_ID)
            .build();
        ClusterState clusterState = ClusterState.builder(new ClusterName(this.getClass().getSimpleName()))
            .nodes(nodes).blocks(ClusterBlocks.EMPTY_CLUSTER_BLOCK).build();

        ClusterApplierService clusterApplierService = clusterService.getClusterApplierService();
        clusterApplierService.setInitialState(clusterState);

        MasterService masterService = clusterService.getMasterService();
        masterService.setClusterStatePublisher(createClusterStatePublisher(clusterApplierService));
        masterService.setClusterStateSupplier(clusterApplierService::state);

        clusterService.start();
        return clusterService;
    }
}
