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
package org.elasticsearch.cluster.coordination;


import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.test.IntegTestCase;
import org.junit.Test;

import io.crate.server.cli.MockTerminal;
import joptsimple.OptionSet;

@IntegTestCase.ClusterScope(scope = IntegTestCase.Scope.TEST, numDataNodes = 0, autoManageMasterNodes = false)
public class RemoveCustomsCommandIT extends IntegTestCase {

    @Test
    public void testRemoveCustomsAbortedByUser() throws Exception {
        cluster().setBootstrapMasterNodeIndex(0);
        String node = cluster().startNode();
        Settings dataPathSettings = cluster().dataPathSettings(node);
        ensureStableCluster(1);
        cluster().stopRandomDataNode();

        Environment environment = TestEnvironment.newEnvironment(
            Settings.builder().put(cluster().getDefaultSettings()).put(dataPathSettings).build());
        assertThatThrownBy(() -> removeCustoms(environment, true, new String[]{ "index-graveyard" }))
            .isExactlyInstanceOf(ElasticsearchException.class)
            .hasMessageContaining(ElasticsearchNodeCommand.ABORTED_BY_USER_MSG);
    }

    @Test
    public void testRemoveCustomsSuccessful() throws Exception {
        cluster().setBootstrapMasterNodeIndex(0);
        String node = cluster().startNode();
        execute("create table tbl (x int)");
        execute("drop table tbl");
        assertThat(client().admin().cluster().state(new ClusterStateRequest()).get().getState().metadata().indexGraveyard().getTombstones())
            .hasSize(1);
        Settings dataPathSettings = cluster().dataPathSettings(node);
        ensureStableCluster(1);
        cluster().stopRandomDataNode();

        Environment environment = TestEnvironment.newEnvironment(
            Settings.builder().put(cluster().getDefaultSettings()).put(dataPathSettings).build());
        MockTerminal terminal = removeCustoms(environment, false,
            randomBoolean() ?
                new String[]{ "index-graveyard" } :
                new String[]{ "index-*" }
            );
        assertThat(terminal.getOutput()).contains(RemoveCustomsCommand.CUSTOMS_REMOVED_MSG);
        assertThat(terminal.getOutput()).contains("The following customs will be removed:");
        assertThat(terminal.getOutput()).contains("index-graveyard");

        cluster().startNode(dataPathSettings);
        assertThat(client().admin().cluster().state(new ClusterStateRequest()).get().getState().metadata().indexGraveyard().getTombstones())
            .isEmpty();
    }

    @Test
    public void testCustomDoesNotMatch() throws Exception {
        cluster().setBootstrapMasterNodeIndex(0);
        String node = cluster().startNode();
        execute("create table tbl (x int)");
        execute("drop table tbl");
        assertThat(client().admin().cluster().state(new ClusterStateRequest()).get().getState().metadata().indexGraveyard().getTombstones())
            .hasSize(1);
        Settings dataPathSettings = cluster().dataPathSettings(node);
        ensureStableCluster(1);
        cluster().stopRandomDataNode();

        Environment environment = TestEnvironment.newEnvironment(
            Settings.builder().put(cluster().getDefaultSettings()).put(dataPathSettings).build());

        assertThatThrownBy(() -> removeCustoms(environment, false, new String[]{ "index-greveyard-with-typos" }))
            .isExactlyInstanceOf(UserException.class)
            .hasMessageContaining(
                "No custom metadata matching [index-greveyard-with-typos] were found on this node"
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

    private MockTerminal removeCustoms(Environment environment, boolean abort, String... args) throws Exception {
        final MockTerminal terminal = executeCommand(new RemoveCustomsCommand(), environment, abort, args);
        assertThat(terminal.getOutput()).contains(RemoveCustomsCommand.CONFIRMATION_MSG);
        assertThat(terminal.getOutput()).contains(RemoveCustomsCommand.CUSTOMS_REMOVED_MSG);
        return terminal;
    }
}
