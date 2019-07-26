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

package io.crate.node;

import io.crate.integrationtests.SQLTransportIntegrationTest;
import io.crate.testing.UseJdbc;
import joptsimple.OptionSet;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cli.MockTerminal;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.coordination.DetachClusterCommand;
import org.elasticsearch.cluster.coordination.ElasticsearchNodeCommand;
import org.elasticsearch.cluster.coordination.NoMasterBlockService;
import org.elasticsearch.cluster.coordination.UnsafeBootstrapMasterCommand;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import static org.elasticsearch.discovery.DiscoverySettings.INITIAL_STATE_TIMEOUT_SETTING;
import static org.hamcrest.Matchers.containsString;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numClientNodes = 0, numDataNodes = 0, autoMinMasterNodes = false)
public class UnsafeBootstrapAndDetachCommandIT extends SQLTransportIntegrationTest {

    private MockTerminal executeCommand(ElasticsearchNodeCommand command,
                                        Environment environment,
                                        boolean abort) throws Exception {
        final MockTerminal terminal = new MockTerminal();
        final OptionSet options = command.getParser().parse();
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
            assertThat(terminal.getOutput(), containsString(ElasticsearchNodeCommand.STOP_WARNING_MSG));
        }
        return terminal;
    }

    private MockTerminal unsafeBootstrap(Environment environment) throws Exception {
        final MockTerminal terminal = executeCommand(new UnsafeBootstrapMasterCommand(), environment, false);
        assertThat(terminal.getOutput(), containsString(UnsafeBootstrapMasterCommand.CONFIRMATION_MSG));
        assertThat(terminal.getOutput(), containsString(UnsafeBootstrapMasterCommand.MASTER_NODE_BOOTSTRAPPED_MSG));
        return terminal;
    }

    private MockTerminal detachCluster(Environment environment, boolean abort) throws Exception {
        final MockTerminal terminal = executeCommand(new DetachClusterCommand(), environment, abort);
        assertThat(terminal.getOutput(), containsString(DetachClusterCommand.CONFIRMATION_MSG));
        assertThat(terminal.getOutput(), containsString(DetachClusterCommand.NODE_DETACHED_MSG));
        return terminal;
    }

    private void expectThrows(ThrowingRunnable runnable, String message) {
        ElasticsearchException ex = expectThrows(ElasticsearchException.class, runnable);
        assertThat(ex.getMessage(), containsString(message));
    }

    @Test
    @UseJdbc(0)
    public void test3MasterNodes2Failed() throws Exception {
        internalCluster().setBootstrapMasterNodeIndex(2);
        List<String> masterNodes = new ArrayList<>();

        logger.info("--> start 1st master-eligible node");
        masterNodes.add(internalCluster().startMasterOnlyNode(Settings.builder()
                .put(INITIAL_STATE_TIMEOUT_SETTING.getKey(), "0s")
                .build())); // node ordinal 0

        logger.info("--> start one data-only node");
        String dataNode = internalCluster().startDataOnlyNode(Settings.builder()
                .put(INITIAL_STATE_TIMEOUT_SETTING.getKey(), "0s")
                .build()); // node ordinal 1

        logger.info("--> start 2nd and 3rd master-eligible nodes and bootstrap");
        masterNodes.addAll(internalCluster().startMasterOnlyNodes(2)); // node ordinals 2 and 3

        logger.info("--> wait for all nodes to join the cluster");
        ensureStableCluster(4);

        logger.info("--> create index test");
        execute("create table doc.test (x int)");
        ensureGreen("test");

        Settings master1DataPathSettings = internalCluster().dataPathSettings(masterNodes.get(0));
        Settings master2DataPathSettings = internalCluster().dataPathSettings(masterNodes.get(1));
        Settings master3DataPathSettings = internalCluster().dataPathSettings(masterNodes.get(2));
        Settings dataNodeDataPathSettings = internalCluster().dataPathSettings(dataNode);

        logger.info("--> stop 2nd and 3d master eligible node");
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(masterNodes.get(1)));
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(masterNodes.get(2)));

        logger.info("--> ensure NO_MASTER_BLOCK on data-only node");
        assertBusy(() -> {
            ClusterState state = internalCluster().client(dataNode).admin().cluster().prepareState().setLocal(true)
                    .execute().actionGet().getState();
            assertTrue(state.blocks().hasGlobalBlockWithId(NoMasterBlockService.NO_MASTER_BLOCK_ID));
        });

        logger.info("--> try to unsafely bootstrap 1st master-eligible node, while node lock is held");
        Environment environmentMaster1 = TestEnvironment.newEnvironment(
            Settings.builder().put(internalCluster().getDefaultSettings()).put(master1DataPathSettings).build());
        expectThrows(() -> unsafeBootstrap(environmentMaster1), UnsafeBootstrapMasterCommand.FAILED_TO_OBTAIN_NODE_LOCK_MSG);

        logger.info("--> stop 1st master-eligible node and data-only node");
        NodeEnvironment nodeEnvironment = internalCluster().getMasterNodeInstance(NodeEnvironment.class);
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(masterNodes.get(0)));
        internalCluster().stopRandomDataNode();

        logger.info("--> unsafely-bootstrap 1st master-eligible node");
        MockTerminal terminal = unsafeBootstrap(environmentMaster1);
        MetaData metaData = MetaData.FORMAT.loadLatestState(logger, xContentRegistry(), nodeEnvironment.nodeDataPaths());
        assertThat(terminal.getOutput(), containsString(
            String.format(Locale.ROOT, UnsafeBootstrapMasterCommand.CLUSTER_STATE_TERM_VERSION_MSG_FORMAT,
                          metaData.coordinationMetaData().term(), metaData.version())));

        logger.info("--> start 1st master-eligible node");
        internalCluster().startMasterOnlyNode(master1DataPathSettings);

        logger.info("--> detach-cluster on data-only node");
        Environment environmentData = TestEnvironment.newEnvironment(
            Settings.builder().put(internalCluster().getDefaultSettings()).put(dataNodeDataPathSettings).build());
        detachCluster(environmentData, false);

        logger.info("--> start data-only node");
        String dataNode2 = internalCluster().startDataOnlyNode(dataNodeDataPathSettings);

        logger.info("--> ensure there is no NO_MASTER_BLOCK and unsafe-bootstrap is reflected in cluster state");
        assertBusy(() -> {
            ClusterState state = internalCluster().client(dataNode2).admin().cluster().prepareState().setLocal(true)
                    .execute().actionGet().getState();
            assertFalse(state.blocks().hasGlobalBlockWithId(NoMasterBlockService.NO_MASTER_BLOCK_ID));
            assertTrue(state.metaData().persistentSettings().getAsBoolean(UnsafeBootstrapMasterCommand.UNSAFE_BOOTSTRAP.getKey(), false));
        });

        logger.info("--> ensure index test is green");
        ensureGreen("test");

        logger.info("--> detach-cluster on 2nd and 3rd master-eligible nodes");
        Environment environmentMaster2 = TestEnvironment.newEnvironment(
            Settings.builder().put(internalCluster().getDefaultSettings()).put(master2DataPathSettings).build());
        detachCluster(environmentMaster2, false);
        Environment environmentMaster3 = TestEnvironment.newEnvironment(
            Settings.builder().put(internalCluster().getDefaultSettings()).put(master3DataPathSettings).build());
        detachCluster(environmentMaster3, false);

        logger.info("--> start 2nd and 3rd master-eligible nodes and ensure 4 nodes stable cluster");
        internalCluster().startMasterOnlyNode(master2DataPathSettings);
        internalCluster().startMasterOnlyNode(master3DataPathSettings);
        ensureStableCluster(4);
    }
}
