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

package org.elasticsearch.node.internal;

import io.crate.Constants;
import io.crate.metadata.settings.CrateSettings;
import io.crate.metadata.settings.SettingsApplier;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.cli.Terminal;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.common.Strings.cleanPath;
import static org.elasticsearch.common.network.NetworkService.DEFAULT_NETWORK_HOST;
import static org.elasticsearch.common.settings.Settings.settingsBuilder;

public class CrateSettingsPreparer {

    public static final String IGNORE_SYSTEM_PROPERTIES_SETTING = "config.ignore_system_properties";

    /**
     * ES_COPY_OF: core/src/main/java/org/elasticsearch/node/internal/InternalSettingsPreparer.java
     * This is a copy of {@link InternalSettingsPreparer#prepareEnvironment(Settings, Terminal)}
     * <p>
     * with the addition of the "applyCrateDefaults" call.
     */
    public static Environment prepareEnvironment(Settings input, Terminal terminal) {
        // just create enough settings to build the environment, to get the config dir
        Settings.Builder output = settingsBuilder();
        InternalSettingsPreparer.initializeSettings(output, input, true);
        Environment environment = new Environment(output.build());

        Path path = environment.configFile().resolve("crate.yml");
        if (Files.exists(path)) {
            output.loadFromPath(path);
        }

        // re-initialize settings now that the config file has been loaded
        // TODO: only re-initialize if a config file was actually loaded
        InternalSettingsPreparer.initializeSettings(output, input, false);
        InternalSettingsPreparer.finalizeSettings(output, terminal, environment.configFile());

        validateKnownSettings(output);
        applyCrateDefaults(output);

        environment = new Environment(output.build());

        // we put back the path.logs so we can use it in the logging configuration file
        output.put("path.logs", cleanPath(environment.logsFile().toAbsolutePath().toString()));

        return new Environment(output.build());
    }

    static void validateKnownSettings(Settings.Builder settings) {
        for (Map.Entry<String, String> setting : settings.internalMap().entrySet()) {
            try {
                SettingsApplier applier = CrateSettings.SUPPORTED_SETTINGS.get(setting.getKey());
                if (applier != null) {
                    applier.validate(setting.getValue());
                }
            } catch (IllegalArgumentException e) {
                throw new RuntimeException(String.format(Locale.ENGLISH,
                    "Invalid value [%s] for the [%s] setting.", setting.getValue(), setting.getKey()), e);
            }
        }
    }

    private static void applyCrateDefaults(Settings.Builder settingsBuilder) {
        // read also from crate.yml by default if no other config path has been set
        // if there is also a elasticsearch.yml file this file will be read first and the settings in crate.yml
        // will overwrite them.
        putIfAbsent(settingsBuilder, "http.port", Constants.HTTP_PORT_RANGE);
        putIfAbsent(settingsBuilder, "transport.tcp.port", Constants.TRANSPORT_PORT_RANGE);
        putIfAbsent(settingsBuilder, "thrift.port", Constants.THRIFT_PORT_RANGE);
        putIfAbsent(settingsBuilder, "discovery.zen.ping.multicast.enabled", false);
        putIfAbsent(settingsBuilder, "network.host", DEFAULT_NETWORK_HOST);

        // Set the default cluster name if not explicitly defined
        if (settingsBuilder.get(ClusterName.SETTING).equals(ClusterName.DEFAULT.value())) {
            settingsBuilder.put("cluster.name", "crate");
        }
    }

    private static <T> void putIfAbsent(Settings.Builder settingsBuilder, String setting, T value) {
        if (settingsBuilder.get(setting) == null) {
            settingsBuilder.put(setting, value);
        }
    }
}
