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

package org.elasticsearch.node.internal;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.cli.Terminal;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.elasticsearch.common.settings.Settings.settingsBuilder;

public final class MigrationToolSettingsPreparer {

    /**
     * ES_COPY_OF: core/src/main/java/org/elasticsearch/node/internal/InternalSettingsPreparer.java
     * This is a copy of {@link InternalSettingsPreparer#prepareEnvironment(Settings, Terminal)}
     * <p>
     * with the addition of the "applyCrateDefaults" call.
     */
    public static Environment prepareEnvironment(String configFileName) {
        // just create enough settings to build the environment, to get the config dir
        Settings.Builder output = settingsBuilder();
        InternalSettingsPreparer.initializeSettings(output, Settings.EMPTY, true);
        Environment environment = new Environment(output.build());

        Path path = environment.configFile().resolve(configFileName);
        if (Files.exists(path)) {
            output.loadFromPath(path);
        }

        // re-initialize settings now that the config file has been loaded
        // TODO: only re-initialize if a config file was actually loaded
        InternalSettingsPreparer.initializeSettings(output, Settings.EMPTY, false);

        Terminal silentTerminal = Terminal.DEFAULT;
        silentTerminal.verbosity(Terminal.Verbosity.SILENT);
        InternalSettingsPreparer.finalizeSettings(output, silentTerminal, environment.configFile());

        applyDefaults(output);
        return new Environment(output.build());
    }

    public static Environment prepareEnvironment() {
        return prepareEnvironment("crate.yml");
    }

    private static void applyDefaults(Settings.Builder settingsBuilder) {
        // Set the default cluster name if not explicitly defined
        if (settingsBuilder.get(ClusterName.SETTING).equals(ClusterName.DEFAULT.value())) {
            settingsBuilder.put("cluster.name", "crate");
        }
    }
}
