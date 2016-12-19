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
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.CreationException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.node.internal.CrateSettingsPreparer;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.Collections;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;


public class NodeSettingsTest {

    @Rule
    public TemporaryFolder tmp = new TemporaryFolder();

    protected CrateNode node;
    protected Client client;
    private boolean loggingConfigured = false;

    private final String CRATE_CONFIG_PATH = Paths.get(getClass().getResource("/crate").toURI()).toString();

    public NodeSettingsTest() throws URISyntaxException {
    }

    private void doSetup() throws Exception {
        // mute log4j warning by configuring a dummy logger
        if (!loggingConfigured) {
            Logger root = Logger.getRootLogger();
            root.removeAllAppenders();
            root.setLevel(Level.OFF);
            loggingConfigured = true;
        }
        tmp.create();
        Settings.Builder builder = Settings.builder()
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
        Environment environment = CrateSettingsPreparer.prepareEnvironment(builder.build(), terminal, Collections.emptyMap());
        node = new CrateNode(environment);
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
            node.close();
            node = null;
        }
    }

    /**
     * The default cluster name is "crate" if not set differently in crate settings
     */
    @Test
    public void testClusterName() throws Exception {
        doSetup();
        assertEquals("crate",
            client.admin().cluster().prepareHealth().
                setWaitForGreenStatus().execute().actionGet().getClusterName());
    }

    /**
     * The default cluster name is "crate" if not set differently in crate settings
     */
    @Test
    public void testClusterNameSystemProp() throws Exception {
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
    public void testDefaultPorts() throws Exception {
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

    @Test
    public void testInvalidUnicastHost() throws Exception {
        Settings.Builder builder = Settings.builder()
            .put("discovery.zen.ping.multicast.enabled", false)
            .put("discovery.zen.ping.unicast.hosts", "nonexistinghost:4300")
            .put("path.home", CRATE_CONFIG_PATH);
        Terminal terminal = Terminal.DEFAULT;
        Environment environment = CrateSettingsPreparer.prepareEnvironment(builder.build(), terminal, Collections.emptyMap());

        try {
            node = new CrateNode(environment);
            fail("Exception expected (failed to resolve address)");
        } catch (Throwable t) {
            assertThat(t, instanceOf(CreationException.class));
            Throwable rootCause = ((CreationException) t).getErrorMessages().iterator().next().getCause();
            assertThat(rootCause, instanceOf(IllegalArgumentException.class));
            assertThat(rootCause.getMessage(), is("Failed to resolve address for [nonexistinghost:4300]"));
        }
    }
}
