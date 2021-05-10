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

package io.crate.node;

import io.crate.action.sql.SQLOperations;
import io.crate.action.sql.Session;
import org.elasticsearch.test.ESTestCase;
import io.crate.testing.SQLResponse;
import io.crate.testing.SQLTransportExecutor;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.node.InternalSettingsPreparer;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.env.Environment.PATH_DATA_SETTING;
import static org.elasticsearch.env.Environment.PATH_HOME_SETTING;
import static org.elasticsearch.env.Environment.PATH_LOGS_SETTING;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;

public class NodeSettingsTest extends ESTestCase {

    @Rule
    public TemporaryFolder tmp = new TemporaryFolder();

    private CrateNode node;
    private SQLOperations sqlOperations;

    private Path createConfigPath() throws IOException {
        File config = tmp.newFolder("crate", "config");

        HashMap<String, String> pathSettings = new HashMap<>();
        pathSettings.put(PATH_DATA_SETTING.getKey(), tmp.newFolder("crate", "data").getPath());
        pathSettings.put(PATH_LOGS_SETTING.getKey(), tmp.newFolder("crate", "logs").getPath());

        try (Writer writer = new FileWriter(Paths.get(config.getPath(), "crate.yml").toFile())) {
            Yaml yaml = new Yaml();
            yaml.dump(pathSettings, writer);
        }

        new FileOutputStream(new File(config.getPath(), "log4j2.properties")).close();

        return config.toPath();
    }

    @Before
    public void doSetup() throws Exception {
        tmp.create();
        Path configPath = createConfigPath();
        Map<String, String> settings = new HashMap<>();
        settings.put("node.name", "node-test");
        settings.put("node.data", "true");
        settings.put(PATH_HOME_SETTING.getKey(), configPath.toString());
        // Avoid connecting to other test nodes
        settings.put("discovery.type", "single-node");

        Environment environment = InternalSettingsPreparer.prepareEnvironment(Settings.EMPTY, settings, configPath, () -> "node-test");
        node = new CrateNode(environment);
        node.start();
        sqlOperations = node.injector().getInstance(SQLOperations.class);
    }

    @After
    public void shutDownNodeAndClient() throws IOException {
        if (sqlOperations != null) {
            sqlOperations = null;
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
    public void testClusterName() {
        try (Session session = sqlOperations.newSystemSession()) {
            SQLResponse response = SQLTransportExecutor.execute(
                "select name from sys.cluster",
                new Object[0],
                session)
                .actionGet(SQLTransportExecutor.REQUEST_TIMEOUT);
            assertThat(response.rows()[0][0], is("crate"));
        }
    }

    @Test
    public void testDefaultPaths() {
        assertThat(PATH_DATA_SETTING.get(node.settings()), contains(
            Matchers.endsWith("data")
        ));
        assertTrue(node.settings().get(PATH_LOGS_SETTING.getKey()).endsWith("logs"));
    }
}
