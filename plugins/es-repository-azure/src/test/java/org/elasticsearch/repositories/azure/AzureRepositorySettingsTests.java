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

package org.elasticsearch.repositories.azure;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.repositories.blobstore.BlobStoreTestUtil;
import org.elasticsearch.test.ESTestCase;

import com.microsoft.azure.storage.LocationMode;

public class AzureRepositorySettingsTests extends ESTestCase {

    private AzureRepository azureRepository(Settings settings) {
        Settings internalSettings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toAbsolutePath())
            .putList(Environment.PATH_DATA_SETTING.getKey(), tmpPaths())
            .put(settings)
            .build();
        final AzureRepository azureRepository = new AzureRepository(
            new RepositoryMetadata("foo", "azure", internalSettings),
            NamedWriteableRegistry.EMPTY,
            NamedXContentRegistry.EMPTY,
            BlobStoreTestUtil.mockClusterService(),
            new RecoverySettings(settings, new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS))
        );
        assertThat(azureRepository.getBlobStore()).isNull();
        return azureRepository;
    }

    public void testReadonlyDefault() {
        assertThat(azureRepository(Settings.EMPTY).isReadOnly()).isFalse();
    }

    public void testReadonlyDefaultAndReadonlyOn() {
        assertThat(azureRepository(Settings.builder().put("readonly", true).build()).isReadOnly())
            .isTrue();
    }

    public void testReadonlyWithPrimaryOnly() {
        assertThat(azureRepository(Settings.builder()
            .put(AzureRepository.Repository.LOCATION_MODE_SETTING.getKey(), LocationMode.PRIMARY_ONLY.name())
            .build()).isReadOnly()).isFalse();
    }

    public void testReadonlyWithPrimaryOnlyAndReadonlyOn() {
        assertThat(azureRepository(Settings.builder()
            .put(AzureRepository.Repository.LOCATION_MODE_SETTING.getKey(), LocationMode.PRIMARY_ONLY.name())
            .put("readonly", true)
            .build()).isReadOnly()).isTrue();
    }

    public void testReadonlyWithSecondaryOnlyAndReadonlyOn() {
        assertThat(azureRepository(Settings.builder()
            .put(AzureRepository.Repository.LOCATION_MODE_SETTING.getKey(), LocationMode.SECONDARY_ONLY.name())
            .put("readonly", true)
            .build()).isReadOnly()).isTrue();
    }

    public void testReadonlyWithSecondaryOnlyAndReadonlyOff() {
        assertThat(azureRepository(Settings.builder()
            .put(AzureRepository.Repository.LOCATION_MODE_SETTING.getKey(), LocationMode.SECONDARY_ONLY.name())
            .put("readonly", false)
            .build()).isReadOnly()).isFalse();
    }

    public void testReadonlyWithPrimaryAndSecondaryOnlyAndReadonlyOn() {
        assertThat(azureRepository(Settings.builder()
            .put(AzureRepository.Repository.LOCATION_MODE_SETTING.getKey(), LocationMode.PRIMARY_THEN_SECONDARY.name())
            .put("readonly", true)
            .build()).isReadOnly()).isTrue();
    }

    public void testReadonlyWithPrimaryAndSecondaryOnlyAndReadonlyOff() {
        assertThat(azureRepository(Settings.builder()
            .put(AzureRepository.Repository.LOCATION_MODE_SETTING.getKey(), LocationMode.PRIMARY_THEN_SECONDARY.name())
            .put("readonly", false)
            .build()).isReadOnly()).isFalse();
    }

    public void testChunkSize() {
        // default chunk size
        AzureRepository azureRepository = azureRepository(Settings.EMPTY);
        assertThat(azureRepository.chunkSize()).isEqualTo(AzureStorageService.MAX_CHUNK_SIZE);

        // chunk size in settings
        int size = randomIntBetween(1, 256);
        azureRepository = azureRepository(Settings.builder().put("chunk_size", size + "mb").build());
        assertThat(azureRepository.chunkSize()).isEqualTo(new ByteSizeValue(size, ByteSizeUnit.MB));

        // zero bytes is not allowed
        assertThatThrownBy(
            () -> azureRepository(Settings.builder().put("chunk_size", "0").build()))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("failed to parse value [0] for setting [chunk_size], must be >= [1b]");

        // negative bytes not allowed
        assertThatThrownBy(
            () -> azureRepository(Settings.builder().put("chunk_size", "-1").build()))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("failed to parse value [-1] for setting [chunk_size], must be >= [1b]");

        // greater than max chunk size not allowed
        assertThatThrownBy(
            () -> azureRepository(Settings.builder().put("chunk_size", "257mb").build()))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("failed to parse value [257mb] for setting [chunk_size], must be <= [256mb]");
    }
}
