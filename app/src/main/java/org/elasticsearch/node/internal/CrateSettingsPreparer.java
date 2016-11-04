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
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.env.Environment;
import org.elasticsearch.transport.Netty3Plugin;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.common.Strings.cleanPath;
<<<<<<< HEAD
import static org.elasticsearch.common.network.NetworkService.DEFAULT_NETWORK_HOST;
import static org.elasticsearch.common.network.NetworkService.GLOBAL_NETWORK_HOST_SETTING;
import static org.elasticsearch.common.settings.Settings.settingsBuilder;
=======
import static org.elasticsearch.common.network.NetworkModule.TRANSPORT_TYPE_DEFAULT_KEY;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_PORT;
import static org.elasticsearch.transport.TransportSettings.PORT;
>>>>>>> 1c3a6de... Upgrades elasticsearch to 5.0

public class CrateSettingsPreparer {

    /**
     * ES_COPY_OF: core/src/main/java/org/elasticsearch/node/internal/InternalSettingsPreparer.java
     * This is a copy of {@link InternalSettingsPreparer#prepareEnvironment(Settings, Terminal, Map)}
     * <p>
     * with the addition of the "applyCrateDefaults" call and resolving `crate.yml` instead of `elasticsearch.yml`.
     */
    public static Environment prepareEnvironment(Settings input, Terminal terminal, Map<String, String> properties) {
        // just create enough settings to build the environment, to get the config dir
        Settings.Builder output = Settings.builder();
        InternalSettingsPreparer.initializeSettings(output, input, true, properties);
        Environment environment = new Environment(output.build());


        Path path = environment.configFile().resolve("crate.yml");
        if (Files.exists(path)) {
            try {
                output.loadFromPath(path);
            } catch (IOException e) {
                throw new SettingsException("Failed to load settings from " + path.toString(), e);
            }
        }

        // re-initialize settings now that the config file has been loaded
        // TODO: only re-initialize if a config file was actually loaded
        InternalSettingsPreparer.initializeSettings(output, input, false, properties);
        InternalSettingsPreparer.finalizeSettings(output, terminal);

        validateKnownSettings(output);
        applyCrateDefaults(output);

        environment = new Environment(output.build());

        // we put back the path.logs so we can use it in the logging configuration file
        output.put(Environment.PATH_LOGS_SETTING.getKey(), cleanPath(environment.logsFile().toAbsolutePath().toString()));

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
        putIfAbsent(settingsBuilder, TRANSPORT_TYPE_DEFAULT_KEY, Netty3Plugin.NETTY_TRANSPORT_NAME);
        putIfAbsent(settingsBuilder, SETTING_HTTP_PORT.getKey(), Constants.HTTP_PORT_RANGE);
        putIfAbsent(settingsBuilder, PORT.getKey(), Constants.TRANSPORT_PORT_RANGE);
        putIfAbsent(settingsBuilder, GLOBAL_NETWORK_HOST_SETTING.getKey(), DEFAULT_NETWORK_HOST);

        // Set the default cluster name if not explicitly defined
        if (settingsBuilder.get(ClusterName.CLUSTER_NAME_SETTING.getKey()).equals(ClusterName.DEFAULT.value())) {
            settingsBuilder.put(ClusterName.CLUSTER_NAME_SETTING.getKey(), "crate");
        }
    }

    private static <T> void putIfAbsent(Settings.Builder settingsBuilder, String setting, T value) {
        if (settingsBuilder.get(setting) == null) {
            settingsBuilder.put(setting, value);
        }
    }
}
