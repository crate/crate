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

package io.crate.server.cli;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.elasticsearch.cli.Command;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.env.Environment;
import org.junit.jupiter.api.Test;

import joptsimple.OptionSet;

public class CrateStartupTest extends CommandTestCase {

    @Test
    public void test_invalid_config_settings_kvp() throws Exception {
        assertThatThrownBy(() -> execute("-Cauth.host_based.enabled"))
            .isExactlyInstanceOf(UserException.class)
            .hasMessage("no value provided for setting [auth.host_based.enabled]");
        assertThatThrownBy(() -> execute("-Cauth.host_based.enabled="))
            .isExactlyInstanceOf(UserException.class)
            .hasMessage("no value provided for setting [auth.host_based.enabled]");
    }

    @Override
    protected Command newCommand() {
        return new EnvironmentAwareCommand("starts CrateDB", "C", () -> { }) {
            @Override
            protected void execute(Terminal terminal, OptionSet options, Environment env) throws Exception {
                execute(terminal, options);
            }
        };
    }
}
