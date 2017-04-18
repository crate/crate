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

package org.elasticsearch.node;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import io.crate.Constants;
import io.crate.metadata.settings.CrateSettings;
import io.crate.settings.CrateSetting;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.env.Environment;
import org.elasticsearch.transport.Netty3Plugin;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import static org.elasticsearch.common.Strings.cleanPath;
import static org.elasticsearch.common.network.NetworkModule.TRANSPORT_TYPE_DEFAULT_KEY;
import static org.elasticsearch.common.network.NetworkService.DEFAULT_NETWORK_HOST;
import static org.elasticsearch.common.network.NetworkService.GLOBAL_NETWORK_HOST_SETTING;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_PORT;
import static org.elasticsearch.transport.TransportSettings.PORT;

public class CrateSettingsPreparer {

    /**
     * ES_COPY_OF: core/src/main/java/org/elasticsearch/node/InternalSettingsPreparer.java
     * This is a copy of {@link InternalSettingsPreparer#prepareEnvironment(Settings, Terminal, Map)}
     * <p>
     * with the addition of the "applyCrateDefaults" call and resolving `crate.yml` instead of `elasticsearch.yml`.
     */
    public static Environment prepareEnvironment(Settings input, Terminal terminal, Map<String, String> properties) {
        // just create enough settings to build the environment, to get the config dir
        Settings.Builder output = Settings.builder();
        InternalSettingsPreparer.initializeSettings(output, input, properties);
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
        // TODO only re-initialize if a config file was actually loaded
        InternalSettingsPreparer.initializeSettings(output, input, properties);
        InternalSettingsPreparer.finalizeSettings(output, terminal);

        validateKnownSettings(output);
        applyCrateDefaults(output);

        environment = new Environment(output.build());

        // we put back the path.logs so we can use it in the logging configuration file
        output.put(Environment.PATH_LOGS_SETTING.getKey(), cleanPath(environment.logsFile().toAbsolutePath().toString()));

        return new Environment(output.build());
    }

    static void validateKnownSettings(Settings.Builder builder) {
        Settings settings = builder.build();
        for (CrateSetting<?> crateSetting : CrateSettings.BUILT_IN_SETTINGS) {
            try {
                crateSetting.setting().get(settings);
            } catch (IllegalArgumentException e) {
                throw new RuntimeException(String.format(Locale.ENGLISH,
                    "Invalid value [%s] for the [%s] setting.", crateSetting.setting().getRaw(settings), crateSetting.getKey()), e);
            }

        }
    }

    @VisibleForTesting
    static void applyCrateDefaults(Settings.Builder settingsBuilder) {
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

        // Set a random node name if none is explicitly defined
        if (settingsBuilder.get(Node.NODE_NAME_SETTING.getKey()) == null) {
            settingsBuilder.put(Node.NODE_NAME_SETTING.getKey(), randomNodeName());
        }
    }

    private static <T> void putIfAbsent(Settings.Builder settingsBuilder, String setting, T value) {
        if (settingsBuilder.get(setting) == null) {
            settingsBuilder.put(setting, value);
        }
    }

    private static String randomNodeName() {
        List<String> names = nodeNames();
        int index = ThreadLocalRandom.current().nextInt(names.size());
        return names.get(index);
    }

    @VisibleForTesting
    static List<String> nodeNames() {
        InputStream input = InternalSettingsPreparer.class.getResourceAsStream("/config/names.txt");

        try {
            List<String> names = new ArrayList<>();
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(input, Charsets.UTF_8))) {
                String line = reader.readLine();
                while (line != null) {
                    String[] fields = line.split("\t");
                    if (fields.length == 0) {
                        throw new RuntimeException("Failed to parse the names.txt. Malformed record: " + line);
                    }
                    names.add(fields[0]);
                    line = reader.readLine();
                }
            }
            return names;
        } catch (IOException e) {
            throw new RuntimeException("Could not read node names list", e);
        }
    }
}
