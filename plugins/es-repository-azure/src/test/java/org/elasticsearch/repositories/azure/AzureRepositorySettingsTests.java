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

import org.apache.opendal.AsyncExecutor;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.repositories.blobstore.BlobStoreTestUtil;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;
import org.mockito.Mockito;


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
            new RecoverySettings(settings, new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)),
            Mockito.mock(AsyncExecutor.class)
        );
        return azureRepository;
    }

    @Test
    public void testReadonlyDefault() {
        assertThat(azureRepository(Settings.EMPTY).isReadOnly()).isFalse();
    }

    @Test
    public void testReadonlyDefaultAndReadonlyOn() {
        assertThat(azureRepository(Settings.builder().put("readonly", true).build()).isReadOnly())
            .isTrue();
    }
}
