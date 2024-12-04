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

package org.elasticsearch.node;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Path;
import java.util.HashMap;

import org.assertj.core.api.Assertions;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.junit.Test;

public class InternalSettingsPreparerTest {

    @Test
    public void testThatCommandLineArgumentsOverrideSettingsFromConfigFile() throws Exception {
        HashMap<String, String> settings = new HashMap<>();
        settings.put("path.home", ".");
        Path config = PathUtils.get(getClass().getResource("config").toURI());
        settings.put("path.conf", config.toString());
        settings.put("stats.enabled", "false");
        settings.put("cluster.name", "clusterNameOverridden");
        settings.put("path.logs", "/some/other/path");
        Settings finalSettings = InternalSettingsPreparer
            .prepareEnvironment(Settings.EMPTY, settings, config, () -> "node1").settings();
        // Overriding value from crate.yml
        assertThat(finalSettings.getAsBoolean("stats.enabled", null)).isFalse();
        // Value kept from crate.yml
        assertThat(finalSettings.getAsBoolean("psql.enabled", null)).isFalse();
        // Overriding value from crate.yml
        assertThat(finalSettings.get("cluster.name")).isEqualTo("clusterNameOverridden");
        // Value kept from crate.yml
        assertThat(finalSettings.get("path.logs")).satisfiesAnyOf(
            v -> assertThat(v).isEqualTo("/some/other/path"),
            v -> assertThat(v).endsWith(":\\some\\other\\path")
        );
    }

    @Test
    public void testCustomConfigMustNotContainSettingsFromDefaultCrateYml() throws Exception {
        HashMap<String, String> settings = new HashMap<>();
        Path home = PathUtils.get(getClass().getResource(".").toURI());
        settings.put("path.home", home.toString());
        Path config = PathUtils.get(getClass().getResource("config_custom").toURI());
        settings.put("path.conf", config.toString());
        Settings finalSettings = InternalSettingsPreparer
            .prepareEnvironment(Settings.EMPTY, settings, config, () -> "node1").settings();
        // Values from crate.yml
        assertThat(finalSettings.get("cluster.name")).isEqualTo("custom");
        // path.logs is not set in config_custom/crate.yml
        // so it needs to use default value and not the value set in config/crate.yml
        assertThat(finalSettings.get("path.logs")).satisfiesAnyOf(
            v -> assertThat(v).endsWith("org/elasticsearch/node/logs"),
            v -> assertThat(v).endsWith("org\\elasticsearch\\node\\logs")
        );
    }

    @Test
    public void testClusterNameMissingFromConfigFile() throws Exception {
        HashMap<String, String> settings = new HashMap<>();
        settings.put("path.home", ".");
        settings.put("cluster.name", "clusterName");
        Path config = PathUtils.get(getClass().getResource("config").toURI());
        Settings finalSettings = InternalSettingsPreparer
            .prepareEnvironment(Settings.EMPTY, settings, config, () -> "node1").settings();
        assertThat(finalSettings.get("cluster.name")).isEqualTo("clusterName");
    }

    @Test
    public void testErrorWithDuplicateSettingInConfigFile() throws Exception {
        HashMap<String, String> settings = new HashMap<>();
        settings.put("path.home", ".");
        Path config = PathUtils.get(getClass().getResource("config_invalid").toURI());
        settings.put("path.conf", config.toString());
        assertThatThrownBy(
            () -> InternalSettingsPreparer.prepareEnvironment(Settings.EMPTY, settings, config, () -> "node1")
        ).isExactlyInstanceOf(SettingsException.class)
            .hasMessage("Failed to load settings from [crate.yml]")
            .extracting(t -> t.getCause(), Assertions.as(InstanceOfAssertFactories.THROWABLE))
            .hasMessageStartingWith("Duplicate field 'stats.enabled'");
    }
}
