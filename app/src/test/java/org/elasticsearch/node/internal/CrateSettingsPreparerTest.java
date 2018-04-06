/*
 * Licensed to Crate.io Inc. or its affiliates ("Crate.io") under one or
 * more contributor license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Crate.io licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * However, if you have executed another commercial license agreement with
 * Crate.io these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package org.elasticsearch.node.internal;

import io.crate.Constants;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.http.HttpTransportSettings;
import org.elasticsearch.node.Node;
import org.elasticsearch.transport.Netty4Plugin;
import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.nio.file.Path;
import java.util.HashMap;

import static org.elasticsearch.transport.TcpTransport.PORT;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isIn;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class CrateSettingsPreparerTest {

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testValidateKnownSettings() {
        Settings.Builder builder = Settings.builder()
            .put("stats.enabled", false)
            .put("psql.port", 5432);
        CrateSettingsPreparer.validateKnownSettings(builder);
    }

    @Test
    public void testValidateKnownIncorrectSettings() {
        Settings.Builder builder = Settings.builder()
            .put("stats.enabled", "foo")
            .put("psql.port", 5432);
        expectedException.expect(RuntimeException.class);
        expectedException.expectMessage(containsString("Invalid value [foo] for the [stats.enabled] setting."));
        CrateSettingsPreparer.validateKnownSettings(builder);
    }

    @Test
    public void testValidationOfUnknownSettingsMustBeIgnored() {
        Settings.Builder builder = Settings.builder()
            .put("path.home", true);
        try {
            CrateSettingsPreparer.validateKnownSettings(builder);
        } catch (Throwable e) {
            fail();
        }
    }

    @Test
    public void testDefaultCrateSettings() throws Exception {
        Settings.Builder builder = Settings.builder();
        CrateSettingsPreparer.applyCrateDefaults(builder);

        assertThat(builder.get(NetworkModule.TRANSPORT_TYPE_DEFAULT_KEY), is(Netty4Plugin.NETTY_TRANSPORT_NAME));
        assertThat(builder.get(HttpTransportSettings.SETTING_HTTP_PORT.getKey()), is(Constants.HTTP_PORT_RANGE));
        assertThat(builder.get(PORT.getKey()), is(Constants.TRANSPORT_PORT_RANGE));
        assertThat(builder.get(NetworkService.GLOBAL_NETWORK_HOST_SETTING.getKey()), is(NetworkService.DEFAULT_NETWORK_HOST));

        assertThat(builder.get(ClusterName.CLUSTER_NAME_SETTING.getKey()), is("crate"));
        assertThat(builder.get(Node.NODE_NAME_SETTING.getKey()), isIn(CrateSettingsPreparer.nodeNames()));
    }

    @Test
    public void testThatCommandLineArgumentsOverrideSettingsFromConfigFile() throws Exception {
        HashMap<String, String> settings = new HashMap<>();
        settings.put("path.home", ".");
        Path config = PathUtils.get(getClass().getResource("config").toURI());
        settings.put("path.conf", config.toString());
        settings.put("stats.enabled", "false");
        settings.put("cluster.name", "clusterNameOverridden");
        settings.put("path.logs", "/some/other/path");
        Settings finalSettings = CrateSettingsPreparer.prepareEnvironment(settings, config).settings();
        // Overriding value from crate.yml
        assertThat(finalSettings.getAsBoolean("stats.enabled", null), is(false));
        // Value kept from crate.yml
        assertThat(finalSettings.getAsBoolean("psql.enabled", null), is(false));
        // Overriding value from crate.yml
        assertThat(finalSettings.get("cluster.name"), is("clusterNameOverridden"));
        // Value kept from crate.yml
        assertThat(finalSettings.get("path.logs"), is("/some/other/path"));
    }

    @Test
    public void testCustomConfigMustNotContainSettingsFromDefaultCrateYml() throws Exception {
        HashMap<String, String> settings = new HashMap<>();
        Path home = PathUtils.get(getClass().getResource(".").toURI());
        settings.put("path.home", home.toString());
        Path config = PathUtils.get(getClass().getResource("config_custom").toURI());
        settings.put("path.conf", config.toString());
        Settings finalSettings = CrateSettingsPreparer.prepareEnvironment(settings, config).settings();
        // Values from crate.yml
        assertThat(finalSettings.get("cluster.name"), is("custom"));
        // path.logs is not set in config_custom/crate.yml
        // so it needs to use default value and not the value set in config/crate.yml
        assertThat(finalSettings.get("path.logs"), endsWith("/test/org/elasticsearch/node/internal/logs"));
    }

    @Test
    public void testClusterNameMissingFromConfigFile() throws Exception {
        HashMap<String, String> settings = new HashMap<>();
        settings.put("path.home", ".");
        settings.put("cluster.name", "clusterName");
        Path config = PathUtils.get(getClass().getResource("config").toURI());
        Settings finalSettings = CrateSettingsPreparer.prepareEnvironment(settings, config).settings();
        assertThat(finalSettings.get("cluster.name"), is("clusterName"));
    }

    @Test
    public void testClusterNameMissingFromBothConfigFileAndCommandLineArgs() throws Exception {
        HashMap<String, String> settings = new HashMap<>();
        settings.put("path.home", ".");
        settings.put("cluster.name", "elasticsearch");
        Path config = PathUtils.get(getClass().getResource("config").toURI());
        Settings finalSettings = CrateSettingsPreparer.prepareEnvironment(settings, config).settings();
        assertThat(finalSettings.get("cluster.name"), is("crate"));
    }

    @Test
    public void testErrorWithDuplicateSettingInConfigFile() throws Exception {
        HashMap<String, String> settings = new HashMap<>();
        settings.put("path.home", ".");
        Path config = PathUtils.get(getClass().getResource("config_invalid").toURI());
        settings.put("path.conf", config.toString());
        expectedException.expect(SettingsException.class);
        expectedException.expectMessage("Failed to load settings from");
        expectedException.expectCause(Matchers.hasProperty("message", containsString("Duplicate field 'stats.enabled'")));
        CrateSettingsPreparer.prepareEnvironment(settings, config).settings();
    }
}
