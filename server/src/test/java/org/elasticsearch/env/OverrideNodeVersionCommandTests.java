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
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.nio.file.Path;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.gateway.PersistedClusterStateService;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.crate.server.cli.MockTerminal;
import joptsimple.OptionParser;
import joptsimple.OptionSet;

public class OverrideNodeVersionCommandTests extends ESTestCase {

    private Environment environment;
    private Path[] nodePaths;
    private String nodeId;
    private final OptionSet noOptions = new OptionParser().parse();

    @Before
    public void createNodePaths() throws IOException {
        final Settings settings = buildEnvSettings(Settings.EMPTY);
        environment = TestEnvironment.newEnvironment(settings);
        try (NodeEnvironment nodeEnvironment = new NodeEnvironment(settings, environment)) {
            nodePaths = nodeEnvironment.nodeDataPaths();
            nodeId = nodeEnvironment.nodeId();

            try (PersistedClusterStateService.Writer writer = new PersistedClusterStateService(nodePaths, nodeId,
                xContentRegistry(), BigArrays.NON_RECYCLING_INSTANCE,
                new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS), () -> 0L, true).createWriter()) {
                writer.writeFullStateAndCommit(1L, ClusterState.builder(ClusterName.DEFAULT).metadata(Metadata.builder()
                    .persistentSettings(Settings.builder().put(Metadata.SETTING_READ_ONLY_SETTING.getKey(), true).build()).build())
                    .build());
            }
        }
    }


    @After
    public void checkClusterStateIntact() throws IOException {
        assertThat(Metadata.SETTING_READ_ONLY_SETTING.get(new PersistedClusterStateService(nodePaths, nodeId,
            xContentRegistry(), BigArrays.NON_RECYCLING_INSTANCE,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS), () -> 0L, true)
            .loadBestOnDiskState().metadata.persistentSettings())).isTrue();
    }

    @Test
    public void testFailsOnEmptyPath() {
        final Path emptyPath = createTempDir();
        final MockTerminal mockTerminal = new MockTerminal();
        assertThatThrownBy(() ->
            new OverrideNodeVersionCommand().processNodePaths(mockTerminal, new Path[]{emptyPath}, noOptions, environment)
        ).isExactlyInstanceOf(ElasticsearchException.class)
            .hasMessage(OverrideNodeVersionCommand.NO_METADATA_MESSAGE);
        assertThatThrownBy(() -> mockTerminal.readText(""))
            .isExactlyInstanceOf(IllegalStateException.class);
    }

    @Test
    public void testFailsIfUnnecessary() throws IOException {
        final Version nodeVersion = Version.fromId(between(Version.CURRENT.minimumIndexCompatibilityVersion().internalId, Version.CURRENT.internalId));
        PersistedClusterStateService.overrideVersion(nodeVersion, nodePaths);
        final MockTerminal mockTerminal = new MockTerminal();
        assertThatThrownBy(() ->
            new OverrideNodeVersionCommand().processNodePaths(mockTerminal, nodePaths, noOptions, environment)
        ).isExactlyInstanceOf(ElasticsearchException.class)
            .hasMessageContainingAll(
                "compatible with current version",
                Version.CURRENT.toString(),
                nodeVersion.toString()
            );
        assertThatThrownBy(() -> mockTerminal.readText(""))
            .isExactlyInstanceOf(IllegalStateException.class);
    }

    @Test
    public void testWarnsIfTooOld() throws Exception {
        final Version nodeVersion = NodeMetadataTests.tooOldVersion();
        PersistedClusterStateService.overrideVersion(nodeVersion, nodePaths);
        final MockTerminal mockTerminal = new MockTerminal();
        mockTerminal.addTextInput("n\n");
        assertThatThrownBy(() ->
            new OverrideNodeVersionCommand().processNodePaths(mockTerminal, nodePaths, noOptions, environment)
        ).isExactlyInstanceOf(ElasticsearchException.class)
            .hasMessage("aborted by user");
        assertThat(mockTerminal.getOutput(), allOf(
            containsString("too old"),
            containsString("data loss"),
            containsString("You should not use this tool"),
            containsString(Version.CURRENT.toString()),
            containsString(nodeVersion.toString())));
        assertThatThrownBy(() -> mockTerminal.readText(""))
            .isExactlyInstanceOf(IllegalStateException.class);

        final NodeMetadata nodeMetadata = PersistedClusterStateService.nodeMetadata(nodePaths);
        assertThat(nodeMetadata.nodeVersion()).isEqualTo(nodeVersion);
    }

    @Test
    public void testWarnsIfTooNew() throws Exception {
        final Version nodeVersion = NodeMetadataTests.tooNewVersion();
        PersistedClusterStateService.overrideVersion(nodeVersion, nodePaths);
        final MockTerminal mockTerminal = new MockTerminal();
        mockTerminal.addTextInput(randomFrom("yy", "Yy", "n", "yes", "true", "N", "no"));
        assertThatThrownBy(() ->
            new OverrideNodeVersionCommand().processNodePaths(mockTerminal, nodePaths, noOptions, environment)
        ).isExactlyInstanceOf(ElasticsearchException.class)
            .hasMessage("aborted by user");
        assertThat(mockTerminal.getOutput(), allOf(
            containsString("data loss"),
            containsString("You should not use this tool"),
            containsString(Version.CURRENT.toString()),
            containsString(nodeVersion.toString())));
        assertThatThrownBy(() -> mockTerminal.readText(""))
            .isExactlyInstanceOf(IllegalStateException.class);

        final NodeMetadata nodeMetadata = PersistedClusterStateService.nodeMetadata(nodePaths);
        assertThat(nodeMetadata.nodeVersion()).isEqualTo(nodeVersion);
    }

    @Test
    public void testOverwritesIfTooOld() throws Exception {
        final Version nodeVersion = NodeMetadataTests.tooOldVersion();
        PersistedClusterStateService.overrideVersion(nodeVersion, nodePaths);
        final MockTerminal mockTerminal = new MockTerminal();
        mockTerminal.addTextInput(randomFrom("y", "Y"));
        new OverrideNodeVersionCommand().processNodePaths(mockTerminal, nodePaths, noOptions, environment);
        assertThat(mockTerminal.getOutput(), allOf(
            containsString("too old"),
            containsString("data loss"),
            containsString("You should not use this tool"),
            containsString(Version.CURRENT.toString()),
            containsString(nodeVersion.toString()),
            containsString(OverrideNodeVersionCommand.SUCCESS_MESSAGE)));
        assertThatThrownBy(() -> mockTerminal.readText(""))
            .isExactlyInstanceOf(IllegalStateException.class);

        final NodeMetadata nodeMetadata = PersistedClusterStateService.nodeMetadata(nodePaths);
        assertThat(nodeMetadata.nodeVersion()).isEqualTo(Version.CURRENT);
    }

    @Test
    public void testOverwritesIfTooNew() throws Exception {
        final Version nodeVersion = NodeMetadataTests.tooNewVersion();
        PersistedClusterStateService.overrideVersion(nodeVersion, nodePaths);
        final MockTerminal mockTerminal = new MockTerminal();
        mockTerminal.addTextInput(randomFrom("y", "Y"));
        new OverrideNodeVersionCommand().processNodePaths(mockTerminal, nodePaths, noOptions, environment);
        assertThat(mockTerminal.getOutput(), allOf(
            containsString("data loss"),
            containsString("You should not use this tool"),
            containsString(Version.CURRENT.toString()),
            containsString(nodeVersion.toString()),
            containsString(OverrideNodeVersionCommand.SUCCESS_MESSAGE)));
        assertThatThrownBy(() -> mockTerminal.readText(""))
            .isExactlyInstanceOf(IllegalStateException.class);

        final NodeMetadata nodeMetadata = PersistedClusterStateService.nodeMetadata(nodePaths);
        assertThat(nodeMetadata.nodeVersion()).isEqualTo(Version.CURRENT);
    }
}
