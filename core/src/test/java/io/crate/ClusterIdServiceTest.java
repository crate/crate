/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate;

import io.crate.plugin.CrateCorePlugin;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;

@ElasticsearchIntegrationTest.ClusterScope(scope = ElasticsearchIntegrationTest.Scope.TEST, numDataNodes = 0)
public class ClusterIdServiceTest extends ElasticsearchIntegrationTest  {

    static {
        ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(true);
    }

    @Test
    public void testClusterIdGeneration() throws Exception {
        Settings localSettings = settingsBuilder()
                .put("plugin.types", CrateCorePlugin.class.getName())
                .put("discovery.type", "local").build();
        String node_0 = internalCluster().startNode(localSettings);

        ClusterIdService clusterIdService = internalCluster().getInstance(ClusterIdService.class, node_0);
        assertNotNull(clusterIdService.clusterId().get());
    }

    @Test
    public void testClusterIdTransient() throws Exception {
        Settings localSettings = settingsBuilder()
                .put("plugin.types", CrateCorePlugin.class.getName())
                .put("discovery.type", "local").build();
        String node_0 = internalCluster().startNode(localSettings);

        ClusterIdService clusterIdService = internalCluster().getInstance(ClusterIdService.class, node_0);
        String clusterId = clusterIdService.clusterId().get().value().toString();

        internalCluster().stopRandomDataNode();
        node_0 = internalCluster().startNode(localSettings);

        clusterIdService = internalCluster().getInstance(ClusterIdService.class, node_0);
        String clusterId2 = clusterIdService.clusterId().get().value().toString();
        assertNotNull(clusterId2);

        assertNotSame(clusterId, clusterId2);
    }

    @Test
    public void testClusterIdDistribution() throws Exception {
        Settings localSettings = settingsBuilder()
                .put("plugin.types", CrateCorePlugin.class.getName())
                .put("discovery.type", "zen").build();
        String node_0 = internalCluster().startNode(localSettings);

        ClusterIdService clusterIdServiceNode0 = internalCluster().getInstance(ClusterIdService.class, node_0);
        ClusterId clusterId = clusterIdServiceNode0.clusterId().get();
        assertNotNull(clusterId);

        String node_1 = internalCluster().startNode(localSettings);

        ClusterIdService clusterIdServiceNode1 = internalCluster().getInstance(ClusterIdService.class, node_1);
        assertNotNull(clusterIdServiceNode1.clusterId().get());

        assertEquals(clusterId, clusterIdServiceNode1.clusterId().get());

        internalCluster().stopRandomDataNode();

        assertEquals(clusterId, clusterIdServiceNode1.clusterId().get());

        String node_2 = internalCluster().startNode(localSettings);
        ClusterIdService clusterIdServiceNode2 = internalCluster().getInstance(ClusterIdService.class, node_2);
        assertEquals(clusterId, clusterIdServiceNode2.clusterId().get());
    }
}
