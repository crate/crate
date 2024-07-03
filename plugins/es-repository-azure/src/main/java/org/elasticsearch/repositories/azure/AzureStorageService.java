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
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.NoSuchFileException;
import java.security.InvalidKeyException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.common.blobstore.BlobMetadata;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.support.PlainBlobMetadata;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.jetbrains.annotations.Nullable;

import com.microsoft.azure.storage.AccessCondition;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.OperationContext;
import com.microsoft.azure.storage.RetryExponentialRetry;
import com.microsoft.azure.storage.RetryPolicy;
import com.microsoft.azure.storage.StorageErrorCodeStrings;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.BlobListingDetails;
import com.microsoft.azure.storage.blob.BlobProperties;
import com.microsoft.azure.storage.blob.CloudBlob;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlobDirectory;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.microsoft.azure.storage.blob.DeleteSnapshotsOption;
import com.microsoft.azure.storage.blob.ListBlobItem;

import org.jetbrains.annotations.VisibleForTesting;

public class AzureStorageService {

    private static final Logger LOGGER = LogManager.getLogger(AzureStorageService.class);

    public static final ByteSizeValue MIN_CHUNK_SIZE = new ByteSizeValue(1, ByteSizeUnit.BYTES);

    /**
     * {@link com.microsoft.azure.storage.blob.BlobConstants#MAX_SINGLE_UPLOAD_BLOB_SIZE_IN_BYTES}
     */
    public static final ByteSizeValue MAX_CHUNK_SIZE = new ByteSizeValue(256, ByteSizeUnit.MB);

    @VisibleForTesting
    AzureStorageSettings storageSettings;

    AzureStorageService(AzureStorageSettings storageSettings) {
        this.storageSettings = storageSettings;
    }

    /**
     * Creates a {@code CloudBlobClient} on each invocation using the current client
     * settings. CloudBlobClient is not thread safe and the settings can change,
     * therefore the instance is not cache-able and should only be reused inside a
     * thread for logically coupled ops. The {@code OperationContext} is used to
     * specify the proxy, but a new context is *required* for each call.
     */
    public ClientOpCtx client() {
        assert this.storageSettings != null : "must be initialized before fetching a new client";
        final AzureStorageSettings azureStorageSettings = this.storageSettings;
        try {
            return new ClientOpCtx(buildClient(azureStorageSettings), () -> buildOperationContext(azureStorageSettings));
        } catch (InvalidKeyException | URISyntaxException | IllegalArgumentException e) {
            throw new SettingsException("Invalid azure client settings", e);
        }
    }

    protected CloudBlobClient buildClient(AzureStorageSettings azureStorageSettings) throws InvalidKeyException, URISyntaxException {
        final CloudBlobClient client = createClient(azureStorageSettings);
        // Set timeout option if the user sets cloud.azure.storage.timeout or
        // cloud.azure.storage.xxx.timeout (it's negative by default)
        final long timeout = azureStorageSettings.getTimeout().millis();
        if (timeout > 0) {
            if (timeout > Integer.MAX_VALUE) {
                throw new IllegalArgumentException("Timeout [" + azureStorageSettings.getTimeout() + "] exceeds 2,147,483,647ms.");
            }
            client.getDefaultRequestOptions().setTimeoutIntervalInMs((int) timeout);
        }

        client.getDefaultRequestOptions().setLocationMode(azureStorageSettings.getLocationMode());

        // We define a default exponential retry policy
        client.getDefaultRequestOptions()
                .setRetryPolicyFactory(new RetryExponentialRetry(RetryPolicy.DEFAULT_CLIENT_BACKOFF, azureStorageSettings.getMaxRetries()));
        client.getDefaultRequestOptions().setLocationMode(azureStorageSettings.getLocationMode());
        return client;
    }

    protected CloudBlobClient createClient(AzureStorageSettings azureStorageSettings) throws InvalidKeyException, URISyntaxException {
        final String connectionString = azureStorageSettings.getConnectString();
        return CloudStorageAccount.parse(connectionString).createCloudBlobClient();
    }

    protected OperationContext buildOperationContext(AzureStorageSettings azureStorageSettings) {
        final OperationContext context = new OperationContext();
        context.setProxy(azureStorageSettings.getProxy());
        return context;
    }

    void refreshSettings(AzureStorageSettings clientSettings) {
        this.storageSettings = AzureStorageSettings.copy(clientSettings);
    }

    /**
     * Extract the blob name from a URI like https://myservice.azure.net/container/path/to/myfile
     * It should remove the container part (first part of the path) and gives path/to/myfile
     * @param uri URI to parse
     * @return The blob name relative to the container
     */
    static String blobNameFromUri(URI uri) {
        final String path = uri.getPath();
        // We remove the container name from the path
        // The 3 magic number cames from the fact if path is /container/path/to/myfile
        // First occurrence is empty "/"
        // Second occurrence is "container
        // Last part contains "path/to/myfile" which is what we want to get
        final String[] splits = path.split("/", 3);
        // We return the remaining end of the string
        return splits[2];
    }

    public boolean blobExists(String container, String blob) throws URISyntaxException, StorageException {
        // Container name must be lower case.
        final ClientOpCtx clientOpCtx = client();
        final CloudBlobContainer blobContainer = clientOpCtx.cloudBlobClient.getContainerReference(container);
        final CloudBlockBlob azureBlob = blobContainer.getBlockBlobReference(blob);
        return azureBlob.exists(null, null, clientOpCtx.opCtx.get());
    }

    public void deleteBlob(String container, String blob) throws URISyntaxException, StorageException {
        final ClientOpCtx clientOpCtx = client();
        // Container name must be lower case.
        final CloudBlobContainer blobContainer = clientOpCtx.cloudBlobClient.getContainerReference(container);
        LOGGER.trace(() -> new ParameterizedMessage("delete blob for container [{}], blob [{}]", container, blob));
        final CloudBlockBlob azureBlob = blobContainer.getBlockBlobReference(blob);
        LOGGER.trace(() -> new ParameterizedMessage("container [{}]: blob [{}] found. removing.", container, blob));
        azureBlob.delete(DeleteSnapshotsOption.NONE, null, null, clientOpCtx.opCtx.get());
    }

    void deleteBlobDirectory(String container, String path) throws URISyntaxException, StorageException, IOException {
        final ClientOpCtx clientOpCtx = client();
        final CloudBlobContainer blobContainer = clientOpCtx.cloudBlobClient.getContainerReference(container);
        for (final ListBlobItem blobItem : blobContainer.listBlobs(path, true)) {
            // uri.getPath is of the form /container/keyPath.* and we want to strip off the /container/
            // this requires 1 + container.length() + 1, with each 1 corresponding to one of the /
            final String blobPath = blobItem.getUri().getPath().substring(1 + container.length() + 1);
            try {
                deleteBlob(container, blobPath);
            } catch (URISyntaxException | StorageException e) {
                throw new IOException("Deleting directory [" + path + "] failed");
            }
        }
    }

    public InputStream getInputStream(String container, String blob, long position, @Nullable Long length)
        throws URISyntaxException, StorageException, NoSuchFileException {

        final ClientOpCtx clientOpCtx = client();
        final CloudBlockBlob blockBlobReference = clientOpCtx.cloudBlobClient
            .getContainerReference(container).getBlockBlobReference(blob);
        LOGGER.trace(() -> new ParameterizedMessage("reading container [{}], blob [{}]", container, blob));
        return blockBlobReference.openInputStream(position, length, null, null, clientOpCtx.opCtx.get());
    }

    public Map<String, BlobMetadata> listBlobsByPrefix(String container, String keyPath, String prefix)
        throws URISyntaxException, StorageException {
        // NOTE: this should be here: if (prefix == null) prefix = "";
        // however, this is really inefficient since deleteBlobsByPrefix enumerates everything and
        // then does a prefix match on the result; it should just call listBlobsByPrefix with the prefix!
        final var blobsBuilder = new HashMap<String, BlobMetadata>();
        final EnumSet<BlobListingDetails> enumBlobListingDetails = EnumSet.of(BlobListingDetails.METADATA);
        final ClientOpCtx clientOpCtx = client();
        final CloudBlobContainer blobContainer = clientOpCtx.cloudBlobClient.getContainerReference(container);
        LOGGER.trace(() -> new ParameterizedMessage("listing container [{}], keyPath [{}], prefix [{}]", container, keyPath, prefix));
        for (final ListBlobItem blobItem : blobContainer.listBlobs(keyPath + (prefix == null ? "" : prefix), false,
            enumBlobListingDetails, null, clientOpCtx.opCtx.get())) {
            final URI uri = blobItem.getUri();
            LOGGER.trace(() -> new ParameterizedMessage("blob url [{}]", uri));
            // uri.getPath is of the form /container/keyPath.* and we want to strip off the /container/
            // this requires 1 + container.length() + 1, with each 1 corresponding to one of the /
            final String blobPath = uri.getPath().substring(1 + container.length() + 1);
            if (blobItem instanceof CloudBlob cloudBlobItem) {
                final BlobProperties properties = cloudBlobItem.getProperties();
                final String name = blobPath.substring(keyPath.length());
                LOGGER.trace(() -> new ParameterizedMessage("blob url [{}], name [{}], size [{}]", uri, name, properties.getLength()));
                blobsBuilder.put(name, new PlainBlobMetadata(name, properties.getLength()));
            }
        }

        return Map.copyOf(blobsBuilder);
    }

    public void writeBlob(String container, String blobName, InputStream inputStream, long blobSize,
                          boolean failIfAlreadyExists)
        throws URISyntaxException, StorageException, IOException {
        LOGGER.trace(() -> new ParameterizedMessage("writeBlob({}, stream, {})", blobName, blobSize));
        final ClientOpCtx clientOpCtx = client();
        final CloudBlobContainer blobContainer = clientOpCtx.cloudBlobClient.getContainerReference(container);
        final CloudBlockBlob blob = blobContainer.getBlockBlobReference(blobName);
        try {
            final AccessCondition accessCondition =
                failIfAlreadyExists ? AccessCondition.generateIfNotExistsCondition() : AccessCondition.generateEmptyCondition();
            blob.upload(inputStream, blobSize, accessCondition, null, clientOpCtx.opCtx.get());
        } catch (final StorageException se) {
            if (failIfAlreadyExists && se.getHttpStatusCode() == HttpURLConnection.HTTP_CONFLICT &&
                StorageErrorCodeStrings.BLOB_ALREADY_EXISTS.equals(se.getErrorCode())) {
                throw new FileAlreadyExistsException(blobName, null, se.getMessage());
            }
            throw se;
        }
        LOGGER.trace(() -> new ParameterizedMessage("writeBlob({}, stream, {}) - done", blobName, blobSize));
    }

    public Set<String> children(String container, BlobPath path) throws URISyntaxException, StorageException {
        final var blobsBuilder = new HashSet<String>();
        final ClientOpCtx clientOpCtx = client();
        final CloudBlobContainer blobContainer = clientOpCtx.cloudBlobClient.getContainerReference(container);
        final String keyPath = path.buildAsString();
        final EnumSet<BlobListingDetails> enumBlobListingDetails = EnumSet.of(BlobListingDetails.METADATA);

        for (ListBlobItem blobItem : blobContainer.listBlobs(
            keyPath, false, enumBlobListingDetails, null, clientOpCtx.opCtx().get())) {

            if (blobItem instanceof CloudBlobDirectory) {
                final URI uri = blobItem.getUri();
                LOGGER.trace(() -> new ParameterizedMessage("blob url [{}]", uri));
                // uri.getPath is of the form /container/keyPath.* and we want to strip off the /container/
                // this requires 1 + container.length() + 1, with each 1 corresponding to one of the /.
                // Lastly, we add the length of keyPath to the offset to strip this container's path.
                final String uriPath = uri.getPath();
                blobsBuilder.add(uriPath.substring(1 + container.length() + 1 + keyPath.length(), uriPath.length() - 1));
            }
        }
        return Set.copyOf(blobsBuilder);
    }

    protected record ClientOpCtx(CloudBlobClient cloudBlobClient, Supplier<OperationContext> opCtx) {}
}
