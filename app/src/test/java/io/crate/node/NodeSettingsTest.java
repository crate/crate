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
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.node.internal.InternalSettingsPreparer;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import static org.junit.Assert.assertEquals;


public class NodeSettingsTest {

    @Rule
    public TemporaryFolder tmp = new TemporaryFolder();

    static {
        ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(true);
    }

    protected Node node;
    protected Client client;
    private boolean loggingConfigured = false;

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
        ImmutableSettings.Builder builder = ImmutableSettings.settingsBuilder()
            .put("node.name", "node-test")
            .put("node.data", true)
            .put("index.store.type", "memory")
            .put("index.store.fs.memory.enabled", "true")
            .put("gateway.type", "none")
            .put("path.data", new File(tmp.getRoot(), "data"))
            .put("path.work", new File(tmp.getRoot(), "work"))
            .put("path.logs", new File(tmp.getRoot(), "logs"))
            .put("index.number_of_shards", "1")
            .put("index.number_of_replicas", "0")
            .put("cluster.routing.schedule", "50ms")
            .put("node.local", true);
        Tuple<Settings,Environment> settingsEnvironmentTuple = InternalSettingsPreparer.prepareSettings(builder.build(), true);
        node = NodeBuilder.nodeBuilder()
            .settings(settingsEnvironmentTuple.v1())
            .loadConfigSettings(false)
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
            node.stop();
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

    /**
     * The location of the used config file might be defined with the system
     * property crate.config. The configuration located at crate's default
     * location will get ignored.
     *
     * @throws IOException
     */
    @Test
    public void testCustomYMLSettings() throws IOException {

        File custom = new File("custom");
        custom.mkdir();
        File file = new File(custom, "custom.yml");
        try (FileWriter customWriter = new FileWriter(file, false)) {
            customWriter.write("cluster.name: custom");
        }

        System.setProperty("es.config", "custom/custom.yml");

        doSetup();

        file.delete();
        custom.delete();
        System.clearProperty("es.config");

        assertEquals("custom",
            client.admin().cluster().prepareHealth().
                setWaitForGreenStatus().execute().actionGet().getClusterName());
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
