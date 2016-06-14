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

import org.elasticsearch.client.transport.TransportClientNodesService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.List;

import static org.elasticsearch.common.settings.Settings.settingsBuilder;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.core.Is.is;

public class CrateClientCreationTest extends CrateClientIntegrationTest {

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
        CrateClient localClient = new CrateClient(serverAddress());
        try {
            assertThat(extractDiscoveryNodes(localClient), hasSize(1));
        } finally {
            localClient.close();
        }
    }

    private List<DiscoveryNode> extractDiscoveryNodes(CrateClient crateClient) throws Exception {
        Field nodesServiceField = CrateClient.class.getDeclaredField("nodesService");
        nodesServiceField.setAccessible(true);
        TransportClientNodesService transportClientNodesService = (TransportClientNodesService) nodesServiceField.get(crateClient);
        return transportClientNodesService.connectedNodes();
    }

    @Test
    public void testCreateWithServer() throws Exception {
        CrateClient localClient = new CrateClient(serverAddress());
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
        CrateClient localClient = new CrateClient(settingsBuilder()
                .put("fancy.setting", "check") // will not be overridden
                .put("node.name", "fancy-node-name") // will be overridden
                .build());

        try {
            Settings settings = localClient.settings();
            assertDefaultSettings(settings);
            assertThat(settings.get("fancy.setting"), is("check"));
        } finally {
            localClient.close();
        }
    }
}
