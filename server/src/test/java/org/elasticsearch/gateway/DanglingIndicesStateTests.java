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

package org.elasticsearch.gateway;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.elasticsearch.gateway.DanglingIndicesState.AUTO_IMPORT_DANGLING_INDICES_SETTING;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Map;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexGraveyard;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.Index;
import org.elasticsearch.test.ESTestCase;

public class DanglingIndicesStateTests extends ESTestCase {

    private static Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .build();

    // The setting AUTO_IMPORT_DANGLING_INDICES_SETTING is deprecated, so we must disable
    // warning checks or all the tests will fail.
    @Override
    protected boolean enableWarningsCheck() {
        return false;
    }

    public void testCleanupWhenEmpty() throws Exception {
        try (NodeEnvironment env = newNodeEnvironment()) {
            MetaStateService metaStateService = new MetaStateService(env, writableRegistry(), xContentRegistry());
            DanglingIndicesState danglingState = createDanglingIndicesState(env, metaStateService);

            assertThat(danglingState.getDanglingIndices().isEmpty()).isTrue();
            Metadata metadata = Metadata.builder().build();
            danglingState.cleanupAllocatedDangledIndices(metadata);
            assertThat(danglingState.getDanglingIndices().isEmpty()).isTrue();
        }
    }

    public void testDanglingIndicesDiscovery() throws Exception {
        try (NodeEnvironment env = newNodeEnvironment()) {
            MetaStateService metaStateService = new MetaStateService(env, writableRegistry(), xContentRegistry());
            DanglingIndicesState danglingState = createDanglingIndicesState(env, metaStateService);
            assertThat(danglingState.getDanglingIndices().isEmpty()).isTrue();
            Metadata metadata = Metadata.builder().build();
            final Settings.Builder settings = Settings.builder().put(indexSettings).put(IndexMetadata.SETTING_INDEX_UUID, "test1UUID");
            IndexMetadata dangledIndex = IndexMetadata.builder("test1").settings(settings).build();
            metaStateService.writeIndex("test_write", dangledIndex);
            Map<Index, IndexMetadata> newDanglingIndices = danglingState.findNewDanglingIndices(metadata);
            assertThat(newDanglingIndices.containsKey(dangledIndex.getIndex())).isTrue();
            metadata = Metadata.builder().put(dangledIndex, false).build();
            newDanglingIndices = danglingState.findNewDanglingIndices(metadata);
            assertThat(newDanglingIndices.containsKey(dangledIndex.getIndex())).isFalse();
        }
    }

    public void testInvalidIndexFolder() throws Exception {
        try (NodeEnvironment env = newNodeEnvironment()) {
            MetaStateService metaStateService = new MetaStateService(env, writableRegistry(), xContentRegistry());
            DanglingIndicesState danglingState = createDanglingIndicesState(env, metaStateService);

            Metadata metadata = Metadata.builder().build();
            final String uuid = "test1UUID";
            final Settings.Builder settings = Settings.builder().put(indexSettings).put(IndexMetadata.SETTING_INDEX_UUID, uuid);
            IndexMetadata dangledIndex = IndexMetadata.builder("test1").settings(settings).build();
            metaStateService.writeIndex("test_write", dangledIndex);
            for (Path path : env.resolveIndexFolder(uuid)) {
                if (Files.exists(path)) {
                    Files.move(path, path.resolveSibling("invalidUUID"), StandardCopyOption.ATOMIC_MOVE);
                }
            }
            try {
                danglingState.findNewDanglingIndices(metadata);
                fail("no exception thrown for invalid folder name");
            } catch (IllegalStateException e) {
                assertThat(e.getMessage()).isEqualTo("[invalidUUID] invalid index folder name, rename to [test1UUID]");
            }
        }
    }

    public void testDanglingProcessing() throws Exception {
        try (NodeEnvironment env = newNodeEnvironment()) {
            MetaStateService metaStateService = new MetaStateService(env, writableRegistry(), xContentRegistry());
            DanglingIndicesState danglingState = createDanglingIndicesState(env, metaStateService);

            Metadata metadata = Metadata.builder().build();

            final Settings.Builder settings = Settings.builder().put(indexSettings).put(IndexMetadata.SETTING_INDEX_UUID, "test1UUID");
            IndexMetadata dangledIndex = IndexMetadata.builder("test1").settings(settings).build();
            metaStateService.writeIndex("test_write", dangledIndex);

            // check that several runs when not in the metadata still keep the dangled index around
            int numberOfChecks = randomIntBetween(1, 10);
            for (int i = 0; i < numberOfChecks; i++) {
                Map<Index, IndexMetadata> newDanglingIndices = danglingState.findNewDanglingIndices(metadata);
                assertThat(newDanglingIndices).containsOnlyKeys(dangledIndex.getIndex());
                assertThat(danglingState.getDanglingIndices()).isEmpty();
            }

            for (int i = 0; i < numberOfChecks; i++) {
                danglingState.findNewAndAddDanglingIndices(metadata);

                assertThat(danglingState.getDanglingIndices()).containsOnlyKeys(dangledIndex.getIndex());
            }

            // simulate allocation to the metadata
            metadata = Metadata.builder(metadata).put(dangledIndex, true).build();

            // check that several runs when in the metadata, but not cleaned yet, still keeps dangled
            for (int i = 0; i < numberOfChecks; i++) {
                Map<Index, IndexMetadata> newDanglingIndices = danglingState.findNewDanglingIndices(metadata);
                assertThat(newDanglingIndices.isEmpty()).isTrue();

                assertThat(danglingState.getDanglingIndices()).containsOnlyKeys(dangledIndex.getIndex());
            }

            danglingState.cleanupAllocatedDangledIndices(metadata);
            assertThat(danglingState.getDanglingIndices().isEmpty()).isTrue();
        }
    }

    public void testDanglingIndicesNotImportedWhenTombstonePresent() throws Exception {
        try (NodeEnvironment env = newNodeEnvironment()) {
            MetaStateService metaStateService = new MetaStateService(env, writableRegistry(), xContentRegistry());
            DanglingIndicesState danglingState = createDanglingIndicesState(env, metaStateService);

            final Settings.Builder settings = Settings.builder().put(indexSettings).put(IndexMetadata.SETTING_INDEX_UUID, "test1UUID");
            IndexMetadata dangledIndex = IndexMetadata.builder("test1").settings(settings).build();
            metaStateService.writeIndex("test_write", dangledIndex);

            final IndexGraveyard graveyard = IndexGraveyard.builder().addTombstone(dangledIndex.getIndex()).build();
            final Metadata metadata = Metadata.builder().indexGraveyard(graveyard).build();
            assertThat(danglingState.findNewDanglingIndices(metadata)).hasSize(0);
        }
    }

    public void testDanglingIndicesAreNotAllocatedWhenDisabled() throws Exception {
        try (NodeEnvironment env = newNodeEnvironment()) {
            MetaStateService metaStateService = new MetaStateService(env, writableRegistry(), xContentRegistry());
            LocalAllocateDangledIndices localAllocateDangledIndices = mock(LocalAllocateDangledIndices.class);

            final Settings allocateSettings = Settings.builder().put(AUTO_IMPORT_DANGLING_INDICES_SETTING.getKey(), false).build();

            final ClusterService clusterServiceMock = mock(ClusterService.class);
            when(clusterServiceMock.getSettings()).thenReturn(allocateSettings);

            final DanglingIndicesState danglingIndicesState = new DanglingIndicesState(
                env,
                metaStateService,
                localAllocateDangledIndices,
                clusterServiceMock
            );
            assertThat(danglingIndicesState.isAutoImportDanglingIndicesEnabled()).as("Expected dangling imports to be disabled").isFalse();
        }
    }

    public void testDanglingIndicesAreAllocatedWhenEnabled() throws Exception {
        try (NodeEnvironment env = newNodeEnvironment()) {
            MetaStateService metaStateService = new MetaStateService(env, writableRegistry(), xContentRegistry());
            LocalAllocateDangledIndices localAllocateDangledIndices = mock(LocalAllocateDangledIndices.class);
            final Settings allocateSettings = Settings.builder().put(AUTO_IMPORT_DANGLING_INDICES_SETTING.getKey(), true).build();

            final ClusterService clusterServiceMock = mock(ClusterService.class);
            when(clusterServiceMock.getSettings()).thenReturn(allocateSettings);

            DanglingIndicesState danglingIndicesState = new DanglingIndicesState(
                env,
                metaStateService,
                localAllocateDangledIndices, clusterServiceMock
            );

            assertThat(danglingIndicesState.isAutoImportDanglingIndicesEnabled()).as("Expected dangling imports to be enabled").isTrue();

            final Settings.Builder settings = Settings.builder().put(indexSettings).put(IndexMetadata.SETTING_INDEX_UUID, "test1UUID");
            IndexMetadata dangledIndex = IndexMetadata.builder("test1").settings(settings).build();
            metaStateService.writeIndex("test_write", dangledIndex);

            danglingIndicesState.findNewAndAddDanglingIndices(Metadata.builder().build());

            danglingIndicesState.allocateDanglingIndices();

            verify(localAllocateDangledIndices).allocateDangled(any(), any());
        }
    }

    private DanglingIndicesState createDanglingIndicesState(NodeEnvironment env, MetaStateService metaStateService) {
        final Settings allocateSettings = Settings.builder().put(AUTO_IMPORT_DANGLING_INDICES_SETTING.getKey(), true).build();

        final ClusterService clusterServiceMock = mock(ClusterService.class);
        when(clusterServiceMock.getSettings()).thenReturn(allocateSettings);

        return new DanglingIndicesState(env, metaStateService, null, clusterServiceMock);
    }
}
