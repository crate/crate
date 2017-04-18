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

import com.carrotsearch.randomizedtesting.RandomizedTest;
import io.crate.Constants;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.CreationException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.node.CrateSettingsPreparer;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.Collections;

import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;


public class NodeSettingsTest extends RandomizedTest {

    @Rule
    public TemporaryFolder tmp = new TemporaryFolder();

    protected CrateNode node;
    protected Client client;
    private boolean loggingConfigured = false;

    private String createConfigPath() throws IOException {
        File home = tmp.newFolder("crate");
        File config = tmp.newFolder("crate", "config");

        Settings pathSettings = Settings.builder()
            .put("path.data", tmp.newFolder("crate", "data").getPath())
            .put("path.logs", tmp.newFolder("crate", "logs").getPath())
            .build();

        try (Writer writer = new FileWriter(Paths.get(config.getPath(), "crate.yml").toFile())) {
            Yaml yaml = new Yaml();
            yaml.dump(pathSettings.getAsMap(), writer);
        }

        return home.getPath();
    }

    public NodeSettingsTest() throws URISyntaxException {
    }

    private void doSetup() throws Exception {
        doSetup(Settings.EMPTY);
    }

    private void doSetup(Settings settings) throws Exception {
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
            .put("path.home", createConfigPath())
            .put();

        Terminal terminal = Terminal.DEFAULT;
        Environment environment = CrateSettingsPreparer.prepareEnvironment(builder.build(), terminal, Collections.emptyMap());
        node = new CrateNode(environment);
        node.start();
        client = node.client();
        client.admin().indices().prepareCreate("test")
            .setSettings(SETTING_NUMBER_OF_REPLICAS, 0, SETTING_NUMBER_OF_SHARDS, 1)
            .execute().actionGet();
    }

    @After
    public void shutDownNodeAndClient() throws IOException {
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

    @Test
    public void testDefaultPaths() throws Exception {
        doSetup();
        assertTrue(node.settings().getAsArray("path.data")[0].endsWith("data"));
        assertTrue(node.settings().get("path.logs").endsWith("logs"));
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
            .put("discovery.zen.ping.unicast.hosts", "nonexistinghost:4300")
            .put("path.home", createConfigPath());
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
