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

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.jetbrains.annotations.Nullable;

import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobMetadata;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.repositories.azure.AzureRepository.Repository;

import com.microsoft.azure.storage.LocationMode;
import com.microsoft.azure.storage.StorageException;

import org.jetbrains.annotations.VisibleForTesting;

public class AzureBlobStore implements BlobStore {

    private final AzureStorageService service;

    private final String container;
    private final LocationMode locationMode;

    public AzureBlobStore(RepositoryMetadata metadata) {
        this(metadata, new AzureStorageService(AzureStorageSettings.getClientSettings(metadata.settings())));
    }

    @VisibleForTesting
    AzureBlobStore(RepositoryMetadata metadata, AzureStorageService service) {
        this.container = Repository.CONTAINER_SETTING.get(metadata.settings());
        this.locationMode = Repository.LOCATION_MODE_SETTING.get(metadata.settings());

        AzureStorageSettings repositorySettings = AzureStorageSettings
            .getClientSettings(metadata.settings());
        service.refreshSettings(repositorySettings);

        this.service = service;
    }

    @Override
    public String toString() {
        return container;
    }

    /**
     * Gets the configured {@link LocationMode} for the Azure storage requests.
     */
    public LocationMode getLocationMode() {
        return locationMode;
    }

    @Override
    public BlobContainer blobContainer(BlobPath path) {
        return new AzureBlobContainer(path, this);
    }

    @Override
    public void close() {
    }

    public boolean blobExists(String blob) throws URISyntaxException, StorageException {
        return service.blobExists(container, blob);
    }

    public void deleteBlob(String blob) throws URISyntaxException, StorageException {
        service.deleteBlob(container, blob);
    }

    public void deleteBlobDirectory(String keyPath) throws URISyntaxException, StorageException, IOException {
        service.deleteBlobDirectory(container, keyPath);
    }

    public Map<String, BlobContainer> children(BlobPath path) throws URISyntaxException, StorageException {
        return Collections.unmodifiableMap(service.children(container, path).stream().collect(
            Collectors.toMap(Function.identity(), name -> new AzureBlobContainer(path.add(name), this))));
    }

    public InputStream getInputStream(String blob, long position, @Nullable Long length)
        throws URISyntaxException, StorageException, IOException {

        return service.getInputStream(container, blob, position, length);
    }

    public Map<String, BlobMetadata> listBlobsByPrefix(String keyPath, String prefix)
        throws URISyntaxException, StorageException {
        return service.listBlobsByPrefix(container, keyPath, prefix);
    }

    public void writeBlob(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists)
        throws URISyntaxException, StorageException, IOException {
        service.writeBlob(container, blobName, inputStream, blobSize, failIfAlreadyExists);
    }
}
