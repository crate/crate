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

import java.util.Locale;

import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.repositories.ESBlobStoreContainerTestCase;

import com.microsoft.azure.storage.LocationMode;

import io.crate.common.unit.TimeValue;

public class AzureBlobStoreContainerTests extends ESBlobStoreContainerTestCase {

    @Override
    protected BlobStore newBlobStore() {
        RepositoryMetadata repositoryMetadata = new RepositoryMetadata("azure", "ittest", Settings.EMPTY);
        AzureStorageServiceMock client = new AzureStorageServiceMock(randomMockAzureStorageSettings());
        try (AzureBlobStore azureBlobStore = new AzureBlobStore(repositoryMetadata, client)) {
            return azureBlobStore;
        }
    }

    static AzureStorageSettings randomMockAzureStorageSettings() {
        String account = randomAlphaOfLength(randomIntBetween(1, 10)).toLowerCase(Locale.ROOT);
        String key = randomAlphaOfLength(randomIntBetween(10, 20)).toLowerCase(Locale.ROOT);
        String endpointSuffix = randomAlphaOfLength(randomIntBetween(1, 10)).toLowerCase(Locale.ROOT);
        String endpoint = "https://storage1.privatelink.blob.core.windows.net";
        String secondaryEndpoint = "https://storage2.privatelink.blob.core.windows.net";
        return new AzureStorageSettings(
            account,
            key,
            endpoint,
            secondaryEndpoint,
            endpointSuffix,
            TimeValue.timeValueMinutes(-1),
            3,
            null,
            randomFrom(
                LocationMode.PRIMARY_ONLY,
                LocationMode.SECONDARY_ONLY,
                LocationMode.PRIMARY_THEN_SECONDARY,
                LocationMode.SECONDARY_THEN_PRIMARY));
    }
}
