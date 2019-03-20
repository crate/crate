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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import io.crate.metadata.settings.CrateSettings;
import io.crate.settings.CrateSetting;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.env.Environment;
import org.elasticsearch.node.InternalSettingsPreparer;
import org.elasticsearch.node.Node;

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
import java.util.function.Function;

public class CrateSettingsPreparer {

    public static Environment prepareEnvironment(Map<String, String> cmdLineSettings, Path configPath) throws UserException {
        // just create enough settings to build the environment, to get the config dir
        Settings.Builder settingsBuilder = Settings.builder();
        settingsBuilder.putProperties(cmdLineSettings, Function.identity());
        settingsBuilder.replacePropertyPlaceholders();
        Environment env = new Environment(settingsBuilder.build(), configPath);
        LogConfigurator.configureWithoutConfig(settingsBuilder.build());

        // add error listener before everything else
        LogConfigurator.registerErrorListener();

        Path path = env.configFile().resolve("crate.yml");
        if (Files.exists(path)) {
            try {
                settingsBuilder.loadFromPath(path);
            } catch (IOException e) {
                throw new SettingsException("Failed to load settings from " + path.toString(), e);
            }
        }

        // re-initialize settings now that the config file has been loaded
        settingsBuilder.putProperties(cmdLineSettings, Function.identity());
        settingsBuilder.replacePropertyPlaceholders();

        // apply Crate defaults BEFORE configuring logging, because the default cluster name influences the log file name
        applyCrateDefaults(settingsBuilder);
        env = new Environment(settingsBuilder.build(), env.configFile());

        try {
            LogConfigurator.configure(env);
        } catch (IOException e) {
            throw new UserException(1, "Couldn't read log4j2.properties configuration file.", e);
        }

        // validating settings initializes loggers in the Crate code, therefore we must do it only AFTER configuration
        validateKnownSettings(settingsBuilder);

        // we put back the path.logs so we can use it in the logging configuration file
        settingsBuilder.put(
            Environment.PATH_LOGS_SETTING.getKey(),
            env.logsFile().toAbsolutePath().normalize().toString());

        return new Environment(settingsBuilder.build(), env.configFile());
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
        // Set a random node name if none is explicitly defined
        if (settingsBuilder.get(Node.NODE_NAME_SETTING.getKey()) == null) {
            settingsBuilder.put(Node.NODE_NAME_SETTING.getKey(), randomNodeName());
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
        } finally {
            IOUtils.closeWhileHandlingException(input);
        }
    }
}
