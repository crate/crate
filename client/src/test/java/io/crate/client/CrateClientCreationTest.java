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

package io.crate.client;

import io.crate.plugin.CrateCorePlugin;
import org.elasticsearch.client.transport.TransportClientNodesService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.transport.TransportService;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Locale;

import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.core.Is.is;

public class CrateClientCreationTest extends ElasticsearchIntegrationTest {

    private int port;
    private String serverAddress;

    private static final String TEST_SETTINGS_PATH = CrateClientCreationTest.class.getResource("crate.yml").getPath();

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return ImmutableSettings.settingsBuilder()
                .put(super.nodeSettings(nodeOrdinal))
                .put("node.mode", "network")
                .put("plugin.types", CrateCorePlugin.class.getName())
                .build();
    }

    @Before
    public void prepare() {
        System.setProperty("es.config", TEST_SETTINGS_PATH);
        InetSocketAddress address = ((InetSocketTransportAddress) internalCluster()
                .getInstance(TransportService.class)
                .boundAddress().boundAddress()).address();
        port = address.getPort();
        serverAddress = String.format(Locale.ENGLISH, "localhost:%s", port);
    }

    @After
    public void cleanUp() throws Exception {
        System.clearProperty("es.config");
    }

    @Test
    public void testNoDefaultNodes() throws Exception {
        CrateClient localClient = new CrateClient();
        try {
            assertThat(extractDiscoveryNodes(localClient), Matchers.<DiscoveryNode>empty());
        } finally {
            localClient.close();
        }
    }

    @Test
    public void testWithNode() throws Exception {
        CrateClient localClient = new CrateClient(ImmutableSettings.EMPTY, false, serverAddress);
        try {
            assertThat(extractDiscoveryNodes(localClient), hasSize(1));
        } finally {
            localClient.close();
        }
    }

    private List<DiscoveryNode> extractDiscoveryNodes(CrateClient crateClient) throws Exception {
        Field internalClientField = CrateClient.class.getDeclaredField("internalClient");
        internalClientField.setAccessible(true);
        InternalCrateClient internalCrateClient = (InternalCrateClient) internalClientField.get(crateClient);
        Field nodesServiceField = InternalCrateClient.class.getDeclaredField("nodesService");
        nodesServiceField.setAccessible(true);
        TransportClientNodesService transportClientNodesService = (TransportClientNodesService) nodesServiceField.get(internalCrateClient);
        return transportClientNodesService.connectedNodes();
    }

    @Test
    public void testCreateWithServer() throws Exception {
        CrateClient localClient = new CrateClient(ImmutableSettings.EMPTY, false, serverAddress);
        try {
            assertThat(extractDiscoveryNodes(localClient), hasSize(1));
        } finally {
            localClient.close();
        }
    }

    @Test
    public void testDefaultSettings() throws Exception {
        CrateClient localClient = new CrateClient();
        try {
            Settings settings = localClient.settings();
            assertDefaultSettings(settings);
        } finally {
            localClient.close();
        }
    }

    private void assertDefaultSettings(Settings settings) {
        assertEquals(false, settings.getAsBoolean("network.server", true));
        assertEquals(true, settings.getAsBoolean("node.client", false));
        assertEquals(true, settings.getAsBoolean("client.transport.ignore_cluster_name", false));
        assertThat(settings.get("node.name"), startsWith("crate-client-"));
    }

    @Test
    public void testWithSettings() throws Exception {
        CrateClient localClient = new CrateClient(ImmutableSettings.builder()
                .put("fancy.setting", "check") // will not be overridden
                .put("node.name", "fancy-node-name") // will be overridden
                .build(), false);

        try {
            Settings settings = localClient.settings();
            assertDefaultSettings(settings);
            assertThat(settings.get("fancy.setting"), is("check"));
        } finally {
            localClient.close();
        }
    }

    @Test
    public void testLoadFromConfig() throws Exception {
        CrateClient configClient = new CrateClient(ImmutableSettings.EMPTY, true);
        CrateClient noConfigClient = new CrateClient(ImmutableSettings.EMPTY, false);
        try {
            Settings configSettings = configClient.settings();
            Settings noConfigSettings = noConfigClient.settings();

            assertThat(configSettings.getAsBoolean("loaded.from.file", false), is(true));
            assertThat(noConfigSettings.getAsMap().containsKey("loaded.from.file"), is(false));
        } finally {
            configClient.close();
            noConfigClient.close();
        }


    }
}
