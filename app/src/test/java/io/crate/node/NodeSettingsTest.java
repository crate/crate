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

package io.crate.node;

import io.crate.Constants;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.varia.NullAppender;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.cli.Terminal;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.node.internal.CrateSettingsPreparer;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class NodeSettingsTest {

    @Rule
    public TemporaryFolder tmp = new TemporaryFolder();

    protected Node node;
    protected Client client;
    private boolean loggingConfigured = false;

    private final String CRATE_CONFIG_PATH = getClass().getResource("/crate").getPath();

    private void doSetup() throws IOException {
        // mute log4j warning by configuring a dummy logger
        if (!loggingConfigured) {
            Logger root = Logger.getRootLogger();
            root.removeAllAppenders();
            root.setLevel(Level.OFF);
            root.addAppender(new NullAppender());
            loggingConfigured = true;
        }
        tmp.create();
        Settings.Builder builder = Settings.settingsBuilder()
            .put("node.name", "node-test")
            .put("node.data", true)
            .put("index.store.type", "default")
            .put("index.store.fs.memory.enabled", "true")
            .put("gateway.type", "default")
            .put("path.home", CRATE_CONFIG_PATH)
            .put("index.number_of_shards", "1")
            .put("index.number_of_replicas", "0")
            .put("cluster.routing.schedule", "50ms")
            .put("node.local", true);

        Terminal terminal = Terminal.DEFAULT;
        Environment environment = CrateSettingsPreparer.prepareEnvironment(builder.build(), terminal);
        node = NodeBuilder.nodeBuilder()
            .settings(environment.settings())
            .build();
        node.start();
        client = node.client();
        client.admin().indices().prepareCreate("test").execute().actionGet();
    }

    @After
    public void tearDown() throws IOException {
        if (client != null) {
            client.admin().indices().prepareDelete("test").execute().actionGet();
            client = null;
        }
        if (node != null) {
            Releasables.close(node);
            node = null;
        }


    }

    /**
     * The default cluster name is "crate" if not set differently in crate settings
     */
    @Test
    public void testClusterName() throws IOException {
        doSetup();
        assertEquals("crate",
            client.admin().cluster().prepareHealth().
                setWaitForGreenStatus().execute().actionGet().getClusterName());
    }

    /**
     * The default cluster name is "crate" if not set differently in crate settings
     */
    @Test
    public void testClusterNameSystemProp() throws IOException {
        System.setProperty("es.cluster.name", "system");
        doSetup();
        assertEquals("system",
            client.admin().cluster().prepareHealth().
                setWaitForGreenStatus().execute().actionGet().getClusterName());
        System.clearProperty("es.cluster.name");

    }

    @Test
    public void testDefaultPaths() throws Exception {
        doSetup();
        assertTrue(node.settings().get("path.data").endsWith("data"));
        assertTrue(node.settings().get("path.logs").endsWith("logs"));
    }

    @Test
    public void testCustomPaths() throws Exception {
        File data1 = new File(tmp.getRoot(), "data1");
        File data2 = new File(tmp.getRoot(), "data2");
        System.setProperty("es.path.data", data1.getAbsolutePath() + "," + data2.getAbsolutePath());
        doSetup();
        String[] paths = node.settings().getAsArray("path.data");
        assertTrue(paths[0].endsWith("data1"));
        assertTrue(paths[1].endsWith("data2"));
        System.clearProperty("es.path.data");
    }

    @Test
    public void testDefaultPorts() throws IOException {
        doSetup();

        assertEquals(
                Constants.HTTP_PORT_RANGE,
                node.settings().get("http.port")
        );
        assertEquals(
                Constants.TRANSPORT_PORT_RANGE,
                node.settings().get("transport.tcp.port")
        );
    }
}
