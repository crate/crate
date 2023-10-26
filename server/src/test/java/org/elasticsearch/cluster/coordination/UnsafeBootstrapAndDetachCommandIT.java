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

import static io.crate.testing.Asserts.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.elasticsearch.gateway.DanglingIndicesState.AUTO_IMPORT_DANGLING_INDICES_SETTING;
import static org.elasticsearch.indices.recovery.RecoverySettings.INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.NodeMetadata;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.gateway.GatewayMetaState;
import org.elasticsearch.gateway.PersistedClusterStateService;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.node.Node;
import org.elasticsearch.test.IntegTestCase;
import org.elasticsearch.test.TestCluster;
import org.junit.Test;

import io.crate.server.cli.MockTerminal;
import io.crate.testing.UseJdbc;
import joptsimple.OptionSet;

@IntegTestCase.ClusterScope(scope = IntegTestCase.Scope.TEST, numDataNodes = 0, autoManageMasterNodes = false)
public class UnsafeBootstrapAndDetachCommandIT extends IntegTestCase {

    private MockTerminal executeCommand(ElasticsearchNodeCommand command, Environment environment, boolean abort)
        throws Exception {
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
            assertThat(terminal.getOutput()).contains(ElasticsearchNodeCommand.STOP_WARNING_MSG);
        }

        return terminal;
    }

    private MockTerminal unsafeBootstrap(Environment environment, boolean abort) throws Exception {
        final MockTerminal terminal = executeCommand(new UnsafeBootstrapMasterCommand(), environment, abort);
        assertThat(terminal.getOutput()).contains(UnsafeBootstrapMasterCommand.CONFIRMATION_MSG);
        assertThat(terminal.getOutput()).contains(UnsafeBootstrapMasterCommand.MASTER_NODE_BOOTSTRAPPED_MSG);
        return terminal;
    }

    private MockTerminal detachCluster(Environment environment, boolean abort) throws Exception {
        final MockTerminal terminal = executeCommand(new DetachClusterCommand(), environment, abort);
        assertThat(terminal.getOutput()).contains(DetachClusterCommand.CONFIRMATION_MSG);
        assertThat(terminal.getOutput()).contains(DetachClusterCommand.NODE_DETACHED_MSG);
        return terminal;
    }

    public MockTerminal unsafeBootstrap(Environment environment) throws Exception {
        return unsafeBootstrap(environment, false);
    }

    private MockTerminal detachCluster(Environment environment) throws Exception {
        return detachCluster(environment, false);
    }

    public void testBootstrapNotMasterEligible() {
        final Environment environment = TestEnvironment.newEnvironment(Settings.builder()
                                                                           .put(cluster().getDefaultSettings())
                                                                           .put(Node.NODE_MASTER_SETTING.getKey(), false)
                                                                           .build());
        assertThatThrownBy(() -> unsafeBootstrap(environment))
            .isExactlyInstanceOf(ElasticsearchException.class)
            .hasMessageContaining(UnsafeBootstrapMasterCommand.NOT_MASTER_NODE_MSG);
    }

    public void testBootstrapNoDataFolder() {
        final Environment environment = TestEnvironment.newEnvironment(cluster().getDefaultSettings());
        assertThatThrownBy(() -> unsafeBootstrap(environment))
            .isExactlyInstanceOf(ElasticsearchException.class)
            .hasMessageContaining(ElasticsearchNodeCommand.NO_NODE_FOLDER_FOUND_MSG);
    }

    public void testDetachNoDataFolder() {
        final Environment environment = TestEnvironment.newEnvironment(cluster().getDefaultSettings());
        assertThatThrownBy(() -> detachCluster(environment))
            .isExactlyInstanceOf(ElasticsearchException.class)
            .hasMessageContaining(ElasticsearchNodeCommand.NO_NODE_FOLDER_FOUND_MSG);
    }

    public void testBootstrapNodeLocked() throws IOException {
        Settings envSettings = buildEnvSettings(Settings.EMPTY);
        Environment environment = TestEnvironment.newEnvironment(envSettings);
        try (NodeEnvironment ignored = new NodeEnvironment(envSettings, environment)) {
            assertThatThrownBy(() -> unsafeBootstrap(environment))
                .isExactlyInstanceOf(ElasticsearchException.class)
                .hasMessageContaining(ElasticsearchNodeCommand.FAILED_TO_OBTAIN_NODE_LOCK_MSG);
        }
    }

    public void testDetachNodeLocked() throws IOException {
        Settings envSettings = buildEnvSettings(Settings.EMPTY);
        Environment environment = TestEnvironment.newEnvironment(envSettings);
        try (NodeEnvironment ignored = new NodeEnvironment(envSettings, environment)) {
            assertThatThrownBy(() -> detachCluster(environment))
                .isExactlyInstanceOf(ElasticsearchException.class)
                .hasMessageContaining(ElasticsearchNodeCommand.FAILED_TO_OBTAIN_NODE_LOCK_MSG);
        }
    }

    public void testBootstrapNoNodeMetadata() throws IOException {
        Settings envSettings = buildEnvSettings(Settings.EMPTY);
        Environment environment = TestEnvironment.newEnvironment(envSettings);
        try (NodeEnvironment nodeEnvironment = new NodeEnvironment(envSettings, environment)) {
            NodeMetadata.FORMAT.cleanupOldFiles(-1, nodeEnvironment.nodeDataPaths());
        }

        assertThatThrownBy(() -> unsafeBootstrap(environment))
            .isExactlyInstanceOf(ElasticsearchException.class)
            .hasMessageContaining(ElasticsearchNodeCommand.NO_NODE_METADATA_FOUND_MSG);
    }

    public void testBootstrapNotBootstrappedCluster() throws Exception {
        String node = cluster().startNode(
            Settings.builder()
                .put(Node.INITIAL_STATE_TIMEOUT_SETTING.getKey(), "0s") // to ensure quick node startup
                .build());
        assertBusy(() -> {
            ClusterState state = client().admin().cluster().state(new ClusterStateRequest().local(true))
                .get().getState();
            assertThat(state.blocks().hasGlobalBlockWithId(NoMasterBlockService.NO_MASTER_BLOCK_ID)).isTrue();
        });

        Settings dataPathSettings = cluster().dataPathSettings(node);

        cluster().stopRandomDataNode();

        Environment environment = TestEnvironment.newEnvironment(
            Settings.builder().put(cluster().getDefaultSettings()).put(dataPathSettings).build());
        assertThatThrownBy(() -> unsafeBootstrap(environment))
            .isExactlyInstanceOf(ElasticsearchException.class)
            .hasMessageContaining(UnsafeBootstrapMasterCommand.EMPTY_LAST_COMMITTED_VOTING_CONFIG_MSG);
    }

    public void testBootstrapNoClusterState() throws IOException {
        cluster().setBootstrapMasterNodeIndex(0);
        String node = cluster().startNode();
        Settings dataPathSettings = cluster().dataPathSettings(node);
        ensureStableCluster(1);
        NodeEnvironment nodeEnvironment = cluster().getMasterNodeInstance(NodeEnvironment.class);
        cluster().stopRandomDataNode();
        Environment environment = TestEnvironment.newEnvironment(
            Settings.builder().put(cluster().getDefaultSettings()).put(dataPathSettings).build());
        PersistedClusterStateService.deleteAll(nodeEnvironment.nodeDataPaths());

        assertThatThrownBy(() -> unsafeBootstrap(environment))
            .isExactlyInstanceOf(ElasticsearchException.class)
            .hasMessageContaining(ElasticsearchNodeCommand.NO_NODE_METADATA_FOUND_MSG);
    }

    public void testDetachNoClusterState() throws IOException {
        cluster().setBootstrapMasterNodeIndex(0);
        String node = cluster().startNode();
        Settings dataPathSettings = cluster().dataPathSettings(node);
        ensureStableCluster(1);
        NodeEnvironment nodeEnvironment = cluster().getMasterNodeInstance(NodeEnvironment.class);
        cluster().stopRandomDataNode();
        Environment environment = TestEnvironment.newEnvironment(
            Settings.builder().put(cluster().getDefaultSettings()).put(dataPathSettings).build());
        PersistedClusterStateService.deleteAll(nodeEnvironment.nodeDataPaths());

        assertThatThrownBy(() -> detachCluster(environment))
            .isExactlyInstanceOf(ElasticsearchException.class)
            .hasMessageContaining(ElasticsearchNodeCommand.NO_NODE_METADATA_FOUND_MSG);
    }

    public void testBootstrapAbortedByUser() throws IOException {
        cluster().setBootstrapMasterNodeIndex(0);
        String node = cluster().startNode();
        Settings dataPathSettings = cluster().dataPathSettings(node);
        ensureStableCluster(1);
        cluster().stopRandomDataNode();

        Environment environment = TestEnvironment.newEnvironment(
            Settings.builder().put(cluster().getDefaultSettings()).put(dataPathSettings).build());
        assertThatThrownBy(() -> unsafeBootstrap(environment, true))
            .isExactlyInstanceOf(ElasticsearchException.class)
            .hasMessageContaining(ElasticsearchNodeCommand.ABORTED_BY_USER_MSG);
    }

    public void testDetachAbortedByUser() throws IOException {
        cluster().setBootstrapMasterNodeIndex(0);
        String node = cluster().startNode();
        Settings dataPathSettings = cluster().dataPathSettings(node);
        ensureStableCluster(1);
        cluster().stopRandomDataNode();

        Environment environment = TestEnvironment.newEnvironment(
            Settings.builder().put(cluster().getDefaultSettings()).put(dataPathSettings).build());
        assertThatThrownBy(() -> detachCluster(environment, true))
            .isExactlyInstanceOf(ElasticsearchException.class)
            .hasMessageContaining(ElasticsearchNodeCommand.ABORTED_BY_USER_MSG);
    }

    @Test
    @UseJdbc(0)
    public void test3MasterNodes2Failed() throws Exception {
        cluster().setBootstrapMasterNodeIndex(2);
        List<String> masterNodes = new ArrayList<>();

        logger.info("--> start 1st master-eligible node");
        masterNodes.add(cluster().startMasterOnlyNode(Settings.builder()
                .put(Node.INITIAL_STATE_TIMEOUT_SETTING.getKey(), "0s")
                .build())); // node ordinal 0

        logger.info("--> start one data-only node");
        String dataNode = cluster().startDataOnlyNode(Settings.builder()
                .put(Node.INITIAL_STATE_TIMEOUT_SETTING.getKey(), "0s")
                .build()); // node ordinal 1

        logger.info("--> start 2nd and 3rd master-eligible nodes and bootstrap");
        masterNodes.addAll(cluster().startMasterOnlyNodes(2)); // node ordinals 2 and 3

        logger.info("--> wait for all nodes to join the cluster");
        ensureStableCluster(4);

        logger.info("--> create index test");
        execute("create table doc.test (x int)");
        ensureGreen("test");

        Settings master1DataPathSettings = cluster().dataPathSettings(masterNodes.get(0));
        Settings master2DataPathSettings = cluster().dataPathSettings(masterNodes.get(1));
        Settings master3DataPathSettings = cluster().dataPathSettings(masterNodes.get(2));
        Settings dataNodeDataPathSettings = cluster().dataPathSettings(dataNode);

        logger.info("--> stop 2nd and 3d master eligible node");
        cluster().stopRandomNode(TestCluster.nameFilter(masterNodes.get(1)));
        cluster().stopRandomNode(TestCluster.nameFilter(masterNodes.get(2)));

        logger.info("--> ensure NO_MASTER_BLOCK on data-only node");
        assertBusy(() -> {
            ClusterState state = cluster().client(dataNode).admin().cluster().state(new ClusterStateRequest().local(true))
                    .get().getState();
            assertThat(state.blocks().hasGlobalBlockWithId(NoMasterBlockService.NO_MASTER_BLOCK_ID)).isTrue();
        });

        logger.info("--> try to unsafely bootstrap 1st master-eligible node, while node lock is held");
        Environment environmentMaster1 = TestEnvironment.newEnvironment(
            Settings.builder().put(cluster().getDefaultSettings()).put(master1DataPathSettings).build());
        assertThatThrownBy(() -> unsafeBootstrap(environmentMaster1))
            .isExactlyInstanceOf(ElasticsearchException.class)
            .hasMessageContaining(UnsafeBootstrapMasterCommand.FAILED_TO_OBTAIN_NODE_LOCK_MSG);

        logger.info("--> stop 1st master-eligible node and data-only node");
        NodeEnvironment nodeEnvironment = cluster().getMasterNodeInstance(NodeEnvironment.class);
        cluster().stopRandomNode(TestCluster.nameFilter(masterNodes.get(0)));
        cluster().stopRandomDataNode();

        logger.info("--> unsafely-bootstrap 1st master-eligible node");
        MockTerminal terminal = unsafeBootstrap(environmentMaster1);
        Metadata metadata = ElasticsearchNodeCommand.createPersistedClusterStateService(Settings.EMPTY, nodeEnvironment.nodeDataPaths())
            .loadBestOnDiskState().metadata;
        assertThat(terminal.getOutput()).contains(
            String.format(Locale.ROOT, UnsafeBootstrapMasterCommand.CLUSTER_STATE_TERM_VERSION_MSG_FORMAT,
                          metadata.coordinationMetadata().term(), metadata.version()));

        logger.info("--> start 1st master-eligible node");
        cluster().startMasterOnlyNode(master1DataPathSettings);

        logger.info("--> detach-cluster on data-only node");
        Environment environmentData = TestEnvironment.newEnvironment(
            Settings.builder().put(cluster().getDefaultSettings()).put(dataNodeDataPathSettings).build());
        detachCluster(environmentData, false);

        logger.info("--> start data-only node");
        String dataNode2 = cluster().startDataOnlyNode(dataNodeDataPathSettings);

        logger.info("--> ensure there is no NO_MASTER_BLOCK and unsafe-bootstrap is reflected in cluster state");
        assertBusy(() -> {
            ClusterState state = cluster().client(dataNode2).admin().cluster().state(new ClusterStateRequest().local(true))
                    .get().getState();
            assertThat(state.blocks().hasGlobalBlockWithId(NoMasterBlockService.NO_MASTER_BLOCK_ID)).isFalse();
            assertThat(state.metadata().persistentSettings().getAsBoolean(UnsafeBootstrapMasterCommand.UNSAFE_BOOTSTRAP.getKey(), false))
                .isTrue();
        });

        logger.info("--> ensure index test is green");
        ensureGreen("test");
        IndexMetadata indexMetadata = clusterService().state().metadata().index("test");
        assertThat(indexMetadata.getSettings().get(IndexMetadata.SETTING_HISTORY_UUID)).isNotNull();

        logger.info("--> detach-cluster on 2nd and 3rd master-eligible nodes");
        Environment environmentMaster2 = TestEnvironment.newEnvironment(
            Settings.builder().put(cluster().getDefaultSettings()).put(master2DataPathSettings).build());
        detachCluster(environmentMaster2, false);
        Environment environmentMaster3 = TestEnvironment.newEnvironment(
            Settings.builder().put(cluster().getDefaultSettings()).put(master3DataPathSettings).build());
        detachCluster(environmentMaster3, false);

        logger.info("--> start 2nd and 3rd master-eligible nodes and ensure 4 nodes stable cluster");
        cluster().startMasterOnlyNode(master2DataPathSettings);
        cluster().startMasterOnlyNode(master3DataPathSettings);
        ensureStableCluster(4);
    }

    public void testAllMasterEligibleNodesFailedDanglingIndexImport() throws Exception {
        cluster().setBootstrapMasterNodeIndex(0);

        Settings settings = Settings.builder()
            .put(AUTO_IMPORT_DANGLING_INDICES_SETTING.getKey(), true)
            .build();

        logger.info("--> start mixed data and master-eligible node and bootstrap cluster");
        String masterNode = cluster().startNode(settings); // node ordinal 0

        logger.info("--> start data-only node and ensure 2 nodes stable cluster");
        String dataNode = cluster().startDataOnlyNode(settings); // node ordinal 1
        ensureStableCluster(2);

        execute("create table doc.test(x int)");

        logger.info("--> index 1 doc and ensure index is green");

        execute("insert into doc.test values(1)");
        execute("refresh table doc.test");
        ensureGreen("test");

        assertBusy(() -> cluster().getInstances(IndicesService.class).forEach(
            indicesService -> assertThat(indicesService.allPendingDanglingIndicesWritten()).isTrue()));

        logger.info("--> verify 1 doc in the index");

        execute("select count(*) from doc.test");
        assertThat(response.rows()[0][0]).isEqualTo(1L);

        logger.info("--> stop data-only node and detach it from the old cluster");
        Settings dataNodeDataPathSettings = Settings.builder()
            .put(cluster().dataPathSettings(dataNode))
            .put(AUTO_IMPORT_DANGLING_INDICES_SETTING.getKey(), true)
            .build();
        assertBusy(() -> cluster().getInstance(GatewayMetaState.class, dataNode).allPendingAsyncStatesWritten());
        cluster().stopRandomNode(TestCluster.nameFilter(dataNode));
        final Environment environment = TestEnvironment.newEnvironment(
            Settings.builder()
                .put(cluster().getDefaultSettings())
                .put(dataNodeDataPathSettings)
                .put(AUTO_IMPORT_DANGLING_INDICES_SETTING.getKey(), true)
                .build());
        detachCluster(environment, false);

        logger.info("--> stop master-eligible node, clear its data and start it again - new cluster should form");
        cluster().restartNode(masterNode, new TestCluster.RestartCallback(){
            @Override
            public boolean clearData(String nodeName) {
                return true;
            }
        });

        logger.info("--> start data-only only node and ensure 2 nodes stable cluster");
        cluster().startDataOnlyNode(dataNodeDataPathSettings);
        ensureStableCluster(2);

        logger.info("--> verify that the dangling index exists and has green status");
        assertBusy(() -> {
            execute("select 1 from information_schema.tables where table_name='test'");
            assertThat(response).hasRows(
                "1"
            );
        });
        ensureGreen("test");

        logger.info("--> verify the doc is there");
        execute("select count(*) from doc.test");
        assertThat(response.rows()[0][0]).isEqualTo(1L);
    }

    public void testNoInitialBootstrapAfterDetach() throws Exception {
        cluster().setBootstrapMasterNodeIndex(0);
        String masterNode = cluster().startMasterOnlyNode();
        Settings masterNodeDataPathSettings = cluster().dataPathSettings(masterNode);
        cluster().stopCurrentMasterNode();

        final Environment environment = TestEnvironment.newEnvironment(
            Settings.builder().put(cluster().getDefaultSettings()).put(masterNodeDataPathSettings).build());
        detachCluster(environment);

        String node = cluster().startMasterOnlyNode(Settings.builder()
                                                                // give the cluster 2 seconds to elect the master (it should not)
                                                                .put(Node.INITIAL_STATE_TIMEOUT_SETTING.getKey(), "2s")
                                                                .put(masterNodeDataPathSettings)
                                                                .build());

        ClusterState state = cluster().client().admin().cluster().state(new ClusterStateRequest().local(true))
            .get().getState();
        assertThat(state.blocks().hasGlobalBlockWithId(NoMasterBlockService.NO_MASTER_BLOCK_ID)).isTrue();

        cluster().stopRandomNode(TestCluster.nameFilter(node));
    }

    @Test
    public void testCanRunUnsafeBootstrapAfterErroneousDetachWithoutLoosingMetadata() throws Exception {
        cluster().setBootstrapMasterNodeIndex(0);
        String masterNode = cluster().startMasterOnlyNode();
        Settings masterNodeDataPathSettings = cluster().dataPathSettings(masterNode);

        execute("SET GLOBAL PERSISTENT indices.recovery.max_bytes_per_sec = '1234kb'");

        ClusterState state = cluster().client().admin().cluster().state(new ClusterStateRequest()).get().getState();
        assertThat(
            state.metadata().persistentSettings().get(INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING.getKey())
        ).isEqualTo("1234kb");

        cluster().stopCurrentMasterNode();

        final Environment environment = TestEnvironment.newEnvironment(
            Settings.builder().put(cluster().getDefaultSettings()).put(masterNodeDataPathSettings).build());
        detachCluster(environment);
        unsafeBootstrap(environment);

        cluster().startMasterOnlyNode(masterNodeDataPathSettings);
        ensureGreen();

        state = cluster().client().admin().cluster().state(new ClusterStateRequest()).get().getState();
        assertThat(
            state.metadata().settings().get(INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING.getKey())
        ).isEqualTo("1234kb");
    }
}
