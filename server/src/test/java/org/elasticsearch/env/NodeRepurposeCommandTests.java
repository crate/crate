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
package org.elasticsearch.env;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.elasticsearch.env.NodeRepurposeCommand.NO_CLEANUP;
import static org.elasticsearch.env.NodeRepurposeCommand.NO_DATA_TO_CLEAN_UP_FOUND;
import static org.elasticsearch.env.NodeRepurposeCommand.NO_SHARD_DATA_TO_CLEAN_UP_FOUND;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.coordination.ElasticsearchNodeCommand;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.CheckedRunnable;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.gateway.PersistedClusterStateService;
import org.elasticsearch.index.Index;
import org.elasticsearch.node.Node;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;
import org.junit.Test;

import io.crate.server.cli.MockTerminal;
import joptsimple.OptionSet;

public class NodeRepurposeCommandTests extends ESTestCase {

    private static final Index INDEX = new Index("testIndex", "testUUID");
    private Settings dataMasterSettings;
    private Environment environment;
    private Path[] nodePaths;
    private Settings dataNoMasterSettings;
    private Settings noDataNoMasterSettings;
    private Settings noDataMasterSettings;

    @Before
    public void createNodePaths() throws IOException {
        dataMasterSettings = buildEnvSettings(Settings.EMPTY);
        environment = TestEnvironment.newEnvironment(dataMasterSettings);
        try (NodeEnvironment nodeEnvironment = new NodeEnvironment(dataMasterSettings, environment)) {
            nodePaths = nodeEnvironment.nodeDataPaths();
            final String nodeId = randomAlphaOfLength(10);
            try (PersistedClusterStateService.Writer writer = new PersistedClusterStateService(nodePaths, nodeId,
                xContentRegistry(), BigArrays.NON_RECYCLING_INSTANCE,
                new ClusterSettings(dataMasterSettings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS), () -> 0L, true).createWriter()) {
                writer.writeFullStateAndCommit(1L, ClusterState.EMPTY_STATE);
            }
        }
        dataNoMasterSettings = Settings.builder()
            .put(dataMasterSettings)
            .put(Node.NODE_MASTER_SETTING.getKey(), false)
            .build();
        noDataNoMasterSettings = Settings.builder()
            .put(dataMasterSettings)
            .put(Node.NODE_DATA_SETTING.getKey(), false)
            .put(Node.NODE_MASTER_SETTING.getKey(), false)
            .build();
        noDataMasterSettings = Settings.builder()
            .put(dataMasterSettings)
            .put(Node.NODE_DATA_SETTING.getKey(), false)
            .put(Node.NODE_MASTER_SETTING.getKey(), true)
            .build();
    }

    public void testEarlyExitNoCleanup() throws Exception {
        createIndexDataFiles(dataMasterSettings, randomInt(10), randomBoolean());

        verifyNoQuestions(dataMasterSettings, s -> assertThat(s).contains(NO_CLEANUP));
        verifyNoQuestions(dataNoMasterSettings, s -> assertThat(s).contains(NO_CLEANUP));
    }

    public void testNothingToCleanup() throws Exception {
        verifyNoQuestions(noDataNoMasterSettings, s -> assertThat(s).contains(NO_DATA_TO_CLEAN_UP_FOUND));
        verifyNoQuestions(noDataMasterSettings, s -> assertThat(s).contains(NO_SHARD_DATA_TO_CLEAN_UP_FOUND));

        Environment environment = TestEnvironment.newEnvironment(noDataMasterSettings);
        if (randomBoolean()) {
            try (NodeEnvironment env = new NodeEnvironment(noDataMasterSettings, environment)) {
                try (PersistedClusterStateService.Writer writer =
                         ElasticsearchNodeCommand.createPersistedClusterStateService(Settings.EMPTY, env.nodeDataPaths()).createWriter()) {
                    writer.writeFullStateAndCommit(1L, ClusterState.EMPTY_STATE);
                }
            }
        }

        verifyNoQuestions(noDataNoMasterSettings, s -> assertThat(s).contains(NO_DATA_TO_CLEAN_UP_FOUND));
        verifyNoQuestions(noDataMasterSettings, s -> assertThat(s).contains(NO_SHARD_DATA_TO_CLEAN_UP_FOUND));

        createIndexDataFiles(dataMasterSettings, 0, randomBoolean());

        verifyNoQuestions(noDataMasterSettings, s -> assertThat(s).contains(NO_SHARD_DATA_TO_CLEAN_UP_FOUND));

    }

    @Test
    public void testLocked() throws IOException {
        try (NodeEnvironment env = new NodeEnvironment(dataMasterSettings, TestEnvironment.newEnvironment(dataMasterSettings))) {
            assertThatThrownBy(() -> verifyNoQuestions(noDataNoMasterSettings, null))
                .isExactlyInstanceOf(ElasticsearchException.class)
                .hasMessageContaining(NodeRepurposeCommand.FAILED_TO_OBTAIN_NODE_LOCK_MSG);
        }
    }

    @Test
    public void testCleanupAll() throws Exception {
        int shardCount = randomIntBetween(1, 10);
        boolean verbose = randomBoolean();
        boolean hasClusterState = randomBoolean();
        createIndexDataFiles(dataMasterSettings, shardCount, hasClusterState);

        String messageText = NodeRepurposeCommand.noMasterMessage(
            1,
            environment.dataFiles().length*shardCount,
            0);

        Consumer<String> outputMatcher = s -> assertThat(s)
            .contains(messageText)
            .satisfies(
                conditionalNot("testIndex", verbose == false || hasClusterState == false),
                conditionalNot("no name for uuid: testUUID", verbose == false || hasClusterState));

        verifyUnchangedOnAbort(noDataNoMasterSettings, outputMatcher, verbose);

        // verify test setup
        assertThatThrownBy(() -> new NodeEnvironment(noDataNoMasterSettings, environment).close())
            .isExactlyInstanceOf(IllegalStateException.class);

        verifySuccess(noDataNoMasterSettings, outputMatcher, verbose);

        //verify cleaned.
        new NodeEnvironment(noDataNoMasterSettings, environment).close();
    }

    public void testCleanupShardData() throws Exception {
        int shardCount = randomIntBetween(1, 10);
        boolean verbose = randomBoolean();
        boolean hasClusterState = randomBoolean();
        createIndexDataFiles(dataMasterSettings, shardCount, hasClusterState);

        Consumer<String> matcher = s -> assertThat(s)
            .contains(NodeRepurposeCommand.shardMessage(environment.dataFiles().length * shardCount, 1))
            .satisfies(
                conditionalNot("testUUID", verbose == false),
                conditionalNot("testIndex", verbose == false || hasClusterState == false),
                conditionalNot("no name for uuid: testUUID", verbose == false || hasClusterState));

        verifyUnchangedOnAbort(noDataMasterSettings, matcher, verbose);

        // verify test setup
        assertThatThrownBy(() -> new NodeEnvironment(noDataMasterSettings, environment).close())
            .isExactlyInstanceOf(IllegalStateException.class);

        verifySuccess(noDataMasterSettings, matcher, verbose);

        //verify clean.
        new NodeEnvironment(noDataMasterSettings, environment).close();
    }

    static void verifySuccess(Settings settings, Consumer<String> outputMatcher, boolean verbose) throws Exception {
        withTerminal(verbose, outputMatcher, terminal -> {
            terminal.addTextInput(randomFrom("y", "Y"));
            executeRepurposeCommand(terminal, settings, 0);
            assertThat(terminal.getOutput()).contains("Node successfully repurposed");
        });
    }

    private void verifyUnchangedOnAbort(Settings settings, Consumer<String> outputMatcher, boolean verbose) throws Exception {
        withTerminal(verbose, outputMatcher, terminal -> {
            terminal.addTextInput(randomFrom("yy", "Yy", "n", "yes", "true", "N", "no"));
            verifyUnchangedDataFiles(() -> {
                assertThatThrownBy(() -> executeRepurposeCommand(terminal, settings, 0))
                    .isExactlyInstanceOf(ElasticsearchException.class)
                    .hasMessageContaining(NodeRepurposeCommand.ABORTED_BY_USER_MSG);
            });
        });
    }

    private void verifyNoQuestions(Settings settings, Consumer<String> outputMatcher) throws Exception {
        withTerminal(false, outputMatcher, terminal -> {
            executeRepurposeCommand(terminal, settings, 0);
        });
    }

    private static void withTerminal(boolean verbose,
                                     Consumer<String> outputMatcher,
                                     CheckedConsumer<MockTerminal, Exception> consumer) throws Exception {
        MockTerminal terminal = new MockTerminal();
        if (verbose) {
            terminal.setVerbosity(Terminal.Verbosity.VERBOSE);
        }

        consumer.accept(terminal);

        assertThat(terminal.getOutput()).satisfies(outputMatcher);

        assertThatThrownBy(() -> terminal.readText(""))
            .as("Must consume input")
            .isExactlyInstanceOf(IllegalStateException.class);
    }

    private static void executeRepurposeCommand(MockTerminal terminal, Settings settings, int ordinal) throws Exception {
        NodeRepurposeCommand nodeRepurposeCommand = new NodeRepurposeCommand();
        OptionSet options = nodeRepurposeCommand.getParser()
            .parse(ordinal != 0 ? new String[]{"--ordinal", Integer.toString(ordinal)} : new String[0]);
        Environment env = TestEnvironment.newEnvironment(settings);
        nodeRepurposeCommand.testExecute(terminal, options, env);
    }

    private void createIndexDataFiles(Settings settings, int shardCount, boolean writeClusterState) throws IOException {
        int shardDataDirNumber = randomInt(10);
        Environment environment = TestEnvironment.newEnvironment(settings);
        try (NodeEnvironment env = new NodeEnvironment(settings, environment)) {
            if (writeClusterState) {
                try (PersistedClusterStateService.Writer writer =
                         ElasticsearchNodeCommand.createPersistedClusterStateService(Settings.EMPTY, env.nodeDataPaths()).createWriter()) {
                    writer.writeFullStateAndCommit(1L, ClusterState.builder(ClusterName.DEFAULT)
                        .metadata(Metadata.builder().put(IndexMetadata.builder(INDEX.getName())
                                                             .settings(Settings.builder().put("index.version.created", Version.CURRENT)
                                                                           .put(IndexMetadata.SETTING_INDEX_UUID, INDEX.getUUID()))
                                                             .numberOfShards(1)
                                                             .numberOfReplicas(1)).build())
                        .build());
                }
            }
            for (Path path : env.indexPaths(INDEX)) {
                for (int i = 0; i < shardCount; ++i) {
                    Files.createDirectories(path.resolve(Integer.toString(shardDataDirNumber)));
                    shardDataDirNumber += randomIntBetween(1,10);
                }
            }
        }
    }

    private void verifyUnchangedDataFiles(CheckedRunnable<? extends Exception> runnable) throws Exception {
        long before = digestPaths();
        runnable.run();
        long after = digestPaths();
        assertThat(after).as("Must not touch files").isEqualTo(before);
    }

    private long digestPaths() {
        // use a commutative digest to avoid dependency on file system order.
        return Arrays.stream(environment.dataFiles()).mapToLong(this::digestPath).sum();
    }

    private long digestPath(Path path) {
        try (Stream<Path> paths = Files.walk(path)) {
            return paths.mapToLong(this::digestSinglePath).sum();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private long digestSinglePath(Path path) {
        if (Files.isDirectory(path))
            return path.toString().hashCode();
        else
            return path.toString().hashCode() + digest(readAllBytes(path));

    }

    private byte[] readAllBytes(Path path) {
        try {
            return Files.readAllBytes(path);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private long digest(byte[] bytes) {
        long result = 0;
        for (byte b : bytes) {
            result *= 31;
            result += b;
        }
        return result;
    }

    private static Consumer<String> conditionalNot(String containedStr, boolean condition) {
        return condition ? s -> assertThat(s).doesNotContain(containedStr) : s -> assertThat(s).contains(containedStr);
    }
}
