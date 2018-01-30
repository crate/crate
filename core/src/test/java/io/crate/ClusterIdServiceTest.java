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
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Test;

import java.util.Collection;
import java.util.Collections;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class ClusterIdServiceTest extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(CrateCorePlugin.class);
    }

    @Test
    public void testClusterIdGeneration() throws Exception {
        String node_0 = internalCluster().startNode();

        ClusterIdService clusterIdService = internalCluster().getInstance(ClusterIdService.class, node_0);
        String clusterId = clusterIdService.clusterId().get();
        assertThat(clusterId, allOf(notNullValue(), not(ClusterState.UNKNOWN_UUID)));
    }

    @Test
    public void testClusterIdTransient() throws Exception {
        String node_0 = internalCluster().startNode();

        ClusterIdService clusterIdService = internalCluster().getInstance(ClusterIdService.class, node_0);
        String clusterId = clusterIdService.clusterId().get();
        assertThat(clusterId, allOf(notNullValue(), not(ClusterState.UNKNOWN_UUID)));

        internalCluster().stopRandomDataNode();
        node_0 = internalCluster().startNode();

        clusterIdService = internalCluster().getInstance(ClusterIdService.class, node_0);
        String clusterId2 = clusterIdService.clusterId().get();
        assertThat(clusterId2, allOf(notNullValue(), not(ClusterState.UNKNOWN_UUID)));

        assertNotSame(clusterId, clusterId2);
    }

    @Test
    public void testClusterIdDistribution() throws Exception {
        String node_0 = internalCluster().startNode();

        System.out.println(clusterService().state().metaData().clusterUUID());
        ClusterIdService clusterIdServiceNode0 = internalCluster().getInstance(ClusterIdService.class, node_0);
        String clusterId = clusterIdServiceNode0.clusterId().get();
        assertThat(clusterId, allOf(notNullValue(), not(ClusterState.UNKNOWN_UUID)));

        String node_1 = internalCluster().startNode();

        ClusterIdService clusterIdServiceNode1 = internalCluster().getInstance(ClusterIdService.class, node_1);
        assertThat(clusterIdServiceNode1.clusterId().get(), allOf(notNullValue(), not(ClusterState.UNKNOWN_UUID)));

        assertEquals(clusterId, clusterIdServiceNode1.clusterId().get());

        internalCluster().stopRandomDataNode();

        assertEquals(clusterId, clusterIdServiceNode1.clusterId().get());

        String node_2 = internalCluster().startNode();
        ClusterIdService clusterIdServiceNode2 = internalCluster().getInstance(ClusterIdService.class, node_2);
        assertEquals(clusterId, clusterIdServiceNode2.clusterId().get());
    }

    @Test
    public void testUnknownClusterUUIDChanged() {
        assertThat(ClusterState.UNKNOWN_UUID, is("_na_"));
    }
}
