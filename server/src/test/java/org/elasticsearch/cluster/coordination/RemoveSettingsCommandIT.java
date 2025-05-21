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
package org.elasticsearch.cluster.coordination;


import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.test.IntegTestCase;
import org.junit.Test;

import io.crate.server.cli.MockTerminal;
import joptsimple.OptionSet;

@IntegTestCase.ClusterScope(scope = IntegTestCase.Scope.TEST, numDataNodes = 0, autoManageMasterNodes = false)
public class RemoveSettingsCommandIT extends IntegTestCase {

    @Test
    public void testRemoveSettingsAbortedByUser() throws Exception {
        cluster().setBootstrapMasterNodeIndex(0);
        String node = cluster().startNode();
        execute("set global persistent cluster.routing.allocation.disk.threshold_enabled = false");
        Settings dataPathSettings = cluster().dataPathSettings(node);
        ensureStableCluster(1);
        cluster().stopRandomDataNode();

        Environment environment = TestEnvironment.newEnvironment(
            Settings.builder().put(cluster().getDefaultSettings()).put(dataPathSettings).build());
        assertThatThrownBy(() -> removeSettings(
            environment,
            true,
            new String[]{ DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING.getKey() })
        ).isExactlyInstanceOf(ElasticsearchException.class)
            .hasMessageContaining(ElasticsearchNodeCommand.ABORTED_BY_USER_MSG);
    }

    @Test
    public void testRemoveSettingsSuccessful() throws Exception {
        cluster().setBootstrapMasterNodeIndex(0);
        String node = cluster().startNode();
        execute("set global persistent cluster.routing.allocation.disk.threshold_enabled = false");
        assertThat(client().state(new ClusterStateRequest()).get().getState().metadata().persistentSettings().keySet())
            .containsExactly(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING.getKey());
        Settings dataPathSettings = cluster().dataPathSettings(node);
        ensureStableCluster(1);
        cluster().stopRandomDataNode();

        Environment environment = TestEnvironment.newEnvironment(
            Settings.builder().put(cluster().getDefaultSettings()).put(dataPathSettings).build());
        MockTerminal terminal = removeSettings(environment, false,
                                               randomBoolean() ?
                                                   new String[]{ DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING.getKey() } :
                                                   new String[]{ "cluster.routing.allocation.disk.*" }
        );
        assertThat(terminal.getOutput()).contains(RemoveSettingsCommand.SETTINGS_REMOVED_MSG);
        assertThat(terminal.getOutput()).contains("The following settings will be removed:");
        assertThat(terminal.getOutput()).contains(
            DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING.getKey() + ": "  + false);

        cluster().startNode(dataPathSettings);
        assertThat(client().state(new ClusterStateRequest()).get().getState().metadata().persistentSettings().keySet())
            .doesNotContain(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING.getKey());
    }

    @Test
    public void testSettingDoesNotMatch() throws Exception {
        cluster().setBootstrapMasterNodeIndex(0);
        String node = cluster().startNode();
        execute("set global persistent cluster.routing.allocation.disk.threshold_enabled = false");
        assertThat(client().state(new ClusterStateRequest()).get().getState().metadata().persistentSettings().keySet())
            .contains(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING.getKey());
        Settings dataPathSettings = cluster().dataPathSettings(node);
        ensureStableCluster(1);
        cluster().stopRandomDataNode();

        Environment environment = TestEnvironment.newEnvironment(
            Settings.builder().put(cluster().getDefaultSettings()).put(dataPathSettings).build());

        assertThatThrownBy(() -> removeSettings(environment, false, new String[]{ "cluster.routing.allocation.disk.bla.*" }))
            .isExactlyInstanceOf(UserException.class)
            .hasMessageContaining(
                "No persistent cluster settings matching [cluster.routing.allocation.disk.bla.*] were found on this node"
            );
    }

    private MockTerminal executeCommand(ElasticsearchNodeCommand command, Environment environment, boolean abort, String... args)
        throws Exception {
        final MockTerminal terminal = new MockTerminal();
        final OptionSet options = command.getParser().parse(args);
        final String input;

        if (abort) {
            input = randomValueOtherThanMany(c -> c.equalsIgnoreCase("y"), () -> randomAlphaOfLength(1));
        } else {
            input = randomBoolean() ? "y" : "Y";
        }

        terminal.addTextInput(input);

        try {
            command.execute(terminal, options, environment);
        } finally {
            assertThat(terminal.getOutput()).contains(ElasticsearchNodeCommand.STOP_WARNING_MSG);
        }

        return terminal;
    }

    private MockTerminal removeSettings(Environment environment, boolean abort, String... args) throws Exception {
        final MockTerminal terminal = executeCommand(new RemoveSettingsCommand(), environment, abort, args);
        assertThat(terminal.getOutput()).contains(RemoveSettingsCommand.CONFIRMATION_MSG);
        assertThat(terminal.getOutput()).contains(RemoveSettingsCommand.SETTINGS_REMOVED_MSG);
        return terminal;
    }
}
