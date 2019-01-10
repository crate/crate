/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.settings;

import java.util.List;

import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.elasticsearch.cli.EnvironmentAwareCommand;
import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.env.Environment;

/**
 * A subcommand for the keystore cli to remove a setting.
 */
class RemoveSettingKeyStoreCommand extends EnvironmentAwareCommand {

    private final OptionSpec<String> arguments;

    RemoveSettingKeyStoreCommand() {
        super("Remove a setting from the keystore");
        arguments = parser.nonOptions("setting names");
    }

    @Override
    protected void execute(Terminal terminal, OptionSet options, Environment env) throws Exception {
        List<String> settings = arguments.values(options);
        if (settings.isEmpty()) {
            throw new UserException(ExitCodes.USAGE, "Must supply at least one setting to remove");
        }

        KeyStoreWrapper keystore = KeyStoreWrapper.load(env.configFile());
        if (keystore == null) {
            throw new UserException(ExitCodes.DATA_ERROR, "Elasticsearch keystore not found. Use 'create' command to create one.");
        }

        keystore.decrypt(new char[0] /* TODO: prompt for password when they are supported */);

        for (String setting : arguments.values(options)) {
            if (keystore.getSettingNames().contains(setting) == false) {
                throw new UserException(ExitCodes.CONFIG, "Setting [" + setting + "] does not exist in the keystore.");
            }
            keystore.remove(setting);
        }
        keystore.save(env.configFile(), new char[0]);
    }
}
