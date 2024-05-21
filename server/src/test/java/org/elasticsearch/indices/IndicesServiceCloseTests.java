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

package org.elasticsearch.indices;

import static org.assertj.core.api.Assertions.assertThat;
import static org.elasticsearch.cluster.coordination.ClusterBootstrapService.INITIAL_MASTER_NODES_SETTING;
import static org.elasticsearch.discovery.SettingsBasedSeedHostsProvider.DISCOVERY_SEED_HOSTS_SETTING;
import static org.junit.Assert.assertEquals;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.node.MockNode;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeValidationException;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockHttpTransport;
import org.elasticsearch.test.TestCluster;
import org.elasticsearch.transport.Netty4Plugin;
import org.junit.Test;

import io.crate.action.sql.CollectingResultReceiver;
import io.crate.action.sql.Session;
import io.crate.action.sql.Sessions;
import io.crate.data.Row;
import io.crate.role.Role;

public class IndicesServiceCloseTests extends ESTestCase {

    private Node startNode() throws NodeValidationException {
        final Path tempDir = createTempDir();
        String nodeName = "node_s_0";
        Settings settings = Settings.builder()
            .put(ClusterName.CLUSTER_NAME_SETTING.getKey(), TestCluster.clusterName("single-node-cluster", random().nextLong()))
            .put(Environment.PATH_HOME_SETTING.getKey(), tempDir)
            .put(Environment.PATH_REPO_SETTING.getKey(), tempDir.resolve("repo"))
            .put(Environment.PATH_SHARED_DATA_SETTING.getKey(), createTempDir().getParent())
            .put(Node.NODE_NAME_SETTING.getKey(), nodeName)
            .put(EsExecutors.PROCESSORS_SETTING.getKey(), 1) // limit the number of threads created
            .put(Node.NODE_DATA_SETTING.getKey(), true)
            .put(NodeEnvironment.NODE_ID_SEED_SETTING.getKey(), random().nextLong())
            // default the watermarks low values to prevent tests from failing on nodes without enough disk space
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(), "1b")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(), "1b")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.getKey(), "1b")
            .putList(DISCOVERY_SEED_HOSTS_SETTING.getKey()) // empty list disables a port scan for other nodes
            .putList(INITIAL_MASTER_NODES_SETTING.getKey(), nodeName)
            .build();

        Collection<Class<? extends Plugin>> plugins = Arrays.asList(MockHttpTransport.TestPlugin.class, Netty4Plugin.class);
        Node node = new MockNode(settings, plugins, true);
        node.start();
        return node;
    }

    @Test
    public void testCloseEmptyIndicesService() throws Exception {
        Node node = startNode();
        IndicesService indicesService = node.injector().getInstance(IndicesService.class);
        assertEquals(1, indicesService.indicesRefCount.refCount());
        assertThat(indicesService.awaitClose(0, TimeUnit.MILLISECONDS)).isFalse();
        node.close();
        assertEquals(0, indicesService.indicesRefCount.refCount());
        assertThat(indicesService.awaitClose(0, TimeUnit.MILLISECONDS)).isTrue();
    }

    @Test
    public void testCloseNonEmptyIndicesService() throws Exception {
        Node node = startNode();
        IndicesService indicesService = node.injector().getInstance(IndicesService.class);
        assertEquals(1, indicesService.indicesRefCount.refCount());

        Sessions sessions = node.injector().getInstance(Sessions.class);
        try (Session session = sessions.newSession("doc", Role.CRATE_USER)) {
            String stmt = "create table test (x int) clustered into 1 shards with (number_of_replicas = 0)";
            var resultReceiver = new CollectingResultReceiver<>(Collectors.toList());
            session.quickExec(stmt, resultReceiver, Row.EMPTY);
            assertThat(resultReceiver.completionFuture()).succeedsWithin(5, TimeUnit.SECONDS);
        }

        assertEquals(2, indicesService.indicesRefCount.refCount());
        assertThat(indicesService.awaitClose(0, TimeUnit.MILLISECONDS)).isFalse();

        node.close();
        assertEquals(0, indicesService.indicesRefCount.refCount());
        assertThat(indicesService.awaitClose(0, TimeUnit.MILLISECONDS)).isTrue();
    }

    @Test
    public void testCloseWhileOngoingRequest() throws Exception {
        Node node = startNode();
        IndicesService indicesService = node.injector().getInstance(IndicesService.class);
        assertEquals(1, indicesService.indicesRefCount.refCount());

        Sessions sessions = node.injector().getInstance(Sessions.class);
        try (Session session = sessions.newSession("doc", Role.CRATE_USER)) {
            String stmt = "create table test (x int) clustered into 1 shards with (number_of_replicas = 0)";
            var resultReceiver = new CollectingResultReceiver<>(Collectors.toList());
            session.quickExec(stmt, resultReceiver, Row.EMPTY);
            assertThat(resultReceiver.completionFuture()).succeedsWithin(5, TimeUnit.SECONDS);
        }
        assertEquals(2, indicesService.indicesRefCount.refCount());

        IndexService indexService = indicesService.iterator().next();
        IndexShard shard = indexService.getShard(0);
        shard.store().incRef();

        node.close();
        assertEquals(1, indicesService.indicesRefCount.refCount());

        shard.store().decRef();
        assertEquals(0, indicesService.indicesRefCount.refCount());
    }
}
