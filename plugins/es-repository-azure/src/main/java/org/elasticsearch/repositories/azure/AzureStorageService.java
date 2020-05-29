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

import com.google.common.annotations.VisibleForTesting;
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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.common.blobstore.BlobMetaData;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.support.PlainBlobMetaData;
import io.crate.common.collections.Tuple;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.FileAlreadyExistsException;
import java.security.InvalidKeyException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

public class AzureStorageService {

    private static final Logger LOGGER = LogManager.getLogger(AzureStorageService.class);

    public static final ByteSizeValue MIN_CHUNK_SIZE = new ByteSizeValue(1, ByteSizeUnit.BYTES);
    /**
     * {@link com.microsoft.azure.storage.blob.BlobConstants#MAX_SINGLE_UPLOAD_BLOB_SIZE_IN_BYTES}
     */
    public static final ByteSizeValue MAX_CHUNK_SIZE = new ByteSizeValue(256, ByteSizeUnit.MB);

    @VisibleForTesting
    volatile AzureStorageSettings storageSettings;

    /**
     * Creates a {@code CloudBlobClient} on each invocation using the current client
     * settings. CloudBlobClient is not thread safe and the settings can change,
     * therefore the instance is not cache-able and should only be reused inside a
     * thread for logically coupled ops. The {@code OperationContext} is used to
     * specify the proxy, but a new context is *required* for each call.
     */
    public Tuple<CloudBlobClient, Supplier<OperationContext>> client() {
        assert this.storageSettings != null : "must be initialized before fetching a new client";
        final AzureStorageSettings azureStorageSettings = this.storageSettings;
        if (azureStorageSettings == null) {
            throw new SettingsException("Client settings are not provided");
        }
        try {
            return new Tuple<>(buildClient(azureStorageSettings), () -> buildOperationContext(azureStorageSettings));
        } catch (InvalidKeyException | URISyntaxException | IllegalArgumentException e) {
            throw new SettingsException("Invalid azure client settings", e);
        }
    }

    protected CloudBlobClient buildClient(AzureStorageSettings azureStorageSettings) throws InvalidKeyException, URISyntaxException {
        final CloudBlobClient client = createClient(azureStorageSettings);
        // Set timeout option if the user sets cloud.azure.storage.timeout or
        // cloud.azure.storage.xxx.timeout (it's negative by default)
        final long timeout = azureStorageSettings.getTimeout().getMillis();
        if (timeout > 0) {
            if (timeout > Integer.MAX_VALUE) {
                throw new IllegalArgumentException("Timeout [" + azureStorageSettings.getTimeout() + "] exceeds 2,147,483,647ms.");
            }
            client.getDefaultRequestOptions().setTimeoutIntervalInMs((int) timeout);
        }
        // We define a default exponential retry policy
        client.getDefaultRequestOptions()
                .setRetryPolicyFactory(new RetryExponentialRetry(RetryPolicy.DEFAULT_CLIENT_BACKOFF, azureStorageSettings.getMaxRetries()));
        client.getDefaultRequestOptions().setLocationMode(azureStorageSettings.getLocationMode());
        return client;
    }

    protected CloudBlobClient createClient(AzureStorageSettings azureStorageSettings) throws InvalidKeyException, URISyntaxException {
        final String connectionString = azureStorageSettings.buildConnectionString();
        return CloudStorageAccount.parse(connectionString).createCloudBlobClient();
    }

    protected OperationContext buildOperationContext(AzureStorageSettings azureStorageSettings) {
        final OperationContext context = new OperationContext();
        context.setProxy(azureStorageSettings.getProxy());
        return context;
    }

    public void refreshSettings(AzureStorageSettings clientSettings) {
        this.storageSettings = AzureStorageSettings.copy(clientSettings);
    }

    public boolean doesContainerExist(String container) throws URISyntaxException, StorageException {
        final Tuple<CloudBlobClient, Supplier<OperationContext>> client = client();
        final CloudBlobContainer blobContainer = client.v1().getContainerReference(container);
        return blobContainer.exists(null, null, client.v2().get());
    }

    public void deleteFiles(String container, String path) throws URISyntaxException, StorageException {
        final Tuple<CloudBlobClient, Supplier<OperationContext>> client = client();
        // container name must be lower case.
        LOGGER.trace(() -> new ParameterizedMessage("delete files container [{}], path [{}]", container, path));
        // list the blobs using a flat blob listing mode
        final CloudBlobContainer blobContainer = client.v1().getContainerReference(container);
        for (final ListBlobItem blobItem : blobContainer.listBlobs(path, true, EnumSet.noneOf(BlobListingDetails.class), null,
            client.v2().get())) {
            final String blobName = blobNameFromUri(blobItem.getUri());
            LOGGER.trace(() -> new ParameterizedMessage("removing blob [{}] full URI was [{}]", blobName, blobItem.getUri()));
            // don't call {@code #deleteBlob}, use the same client
            final CloudBlockBlob azureBlob = blobContainer.getBlockBlobReference(blobName);
            azureBlob.delete(DeleteSnapshotsOption.NONE, null, null, client.v2().get());
        }
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
        final Tuple<CloudBlobClient, Supplier<OperationContext>> client = client();
        final CloudBlobContainer blobContainer = client.v1().getContainerReference(container);
        final CloudBlockBlob azureBlob = blobContainer.getBlockBlobReference(blob);
        return azureBlob.exists(null, null, client.v2().get());
    }

    public void deleteBlob(String container, String blob) throws URISyntaxException, StorageException {
        final Tuple<CloudBlobClient, Supplier<OperationContext>> client = client();
        // Container name must be lower case.
        final CloudBlobContainer blobContainer = client.v1().getContainerReference(container);
        LOGGER.trace(() -> new ParameterizedMessage("delete blob for container [{}], blob [{}]", container, blob));
        final CloudBlockBlob azureBlob = blobContainer.getBlockBlobReference(blob);
        LOGGER.trace(() -> new ParameterizedMessage("container [{}]: blob [{}] found. removing.", container, blob));
        azureBlob.delete(DeleteSnapshotsOption.NONE, null, null, client.v2().get());
    }

    public InputStream getInputStream(String container, String blob)
        throws URISyntaxException, StorageException, IOException {
        final Tuple<CloudBlobClient, Supplier<OperationContext>> client = client();
        final CloudBlockBlob blockBlobReference = client.v1().getContainerReference(container).getBlockBlobReference(blob);
        LOGGER.trace(() -> new ParameterizedMessage("reading container [{}], blob [{}]", container, blob));
        return blockBlobReference.openInputStream(null, null, client.v2().get());
    }

    public Map<String, BlobMetaData> listBlobsByPrefix(String container, String keyPath, String prefix)
        throws URISyntaxException, StorageException {
        // NOTE: this should be here: if (prefix == null) prefix = "";
        // however, this is really inefficient since deleteBlobsByPrefix enumerates everything and
        // then does a prefix match on the result; it should just call listBlobsByPrefix with the prefix!
        final var blobsBuilder = new HashMap<String, BlobMetaData>();
        final EnumSet<BlobListingDetails> enumBlobListingDetails = EnumSet.of(BlobListingDetails.METADATA);
        final Tuple<CloudBlobClient, Supplier<OperationContext>> client = client();
        final CloudBlobContainer blobContainer = client.v1().getContainerReference(container);
        LOGGER.trace(() -> new ParameterizedMessage("listing container [{}], keyPath [{}], prefix [{}]", container, keyPath, prefix));
        for (final ListBlobItem blobItem : blobContainer.listBlobs(keyPath + (prefix == null ? "" : prefix), false,
            enumBlobListingDetails, null, client.v2().get())) {
            final URI uri = blobItem.getUri();
            LOGGER.trace(() -> new ParameterizedMessage("blob url [{}]", uri));
            // uri.getPath is of the form /container/keyPath.* and we want to strip off the /container/
            // this requires 1 + container.length() + 1, with each 1 corresponding to one of the /
            final String blobPath = uri.getPath().substring(1 + container.length() + 1);
            if (blobItem instanceof CloudBlob) {
                final BlobProperties properties = ((CloudBlob) blobItem).getProperties();
                final String name = blobPath.substring(keyPath.length());
                LOGGER.trace(() -> new ParameterizedMessage("blob url [{}], name [{}], size [{}]", uri, name, properties.getLength()));
                blobsBuilder.put(name, new PlainBlobMetaData(name, properties.getLength()));
            }
        }

        return Map.copyOf(blobsBuilder);
    }

    public void writeBlob(String container, String blobName, InputStream inputStream, long blobSize,
                          boolean failIfAlreadyExists)
        throws URISyntaxException, StorageException, IOException {
        LOGGER.trace(() -> new ParameterizedMessage("writeBlob({}, stream, {})", blobName, blobSize));
        final Tuple<CloudBlobClient, Supplier<OperationContext>> client = client();
        final CloudBlobContainer blobContainer = client.v1().getContainerReference(container);
        final CloudBlockBlob blob = blobContainer.getBlockBlobReference(blobName);
        try {
            final AccessCondition accessCondition =
                failIfAlreadyExists ? AccessCondition.generateIfNotExistsCondition() : AccessCondition.generateEmptyCondition();
            blob.upload(inputStream, blobSize, accessCondition, null, client.v2().get());
        } catch (final StorageException se) {
            if (failIfAlreadyExists && se.getHttpStatusCode() == HttpURLConnection.HTTP_CONFLICT &&
                StorageErrorCodeStrings.BLOB_ALREADY_EXISTS.equals(se.getErrorCode())) {
                throw new FileAlreadyExistsException(blobName, null, se.getMessage());
            }
            throw se;
        }
        LOGGER.trace(() -> new ParameterizedMessage("writeBlob({}, stream, {}) - done", blobName, blobSize));
    }

    public Set<String> children(String account, String container, BlobPath path) throws URISyntaxException, StorageException {
        final var blobsBuilder = new HashSet<String>();
        final Tuple<CloudBlobClient, Supplier<OperationContext>> client = client();
        final CloudBlobContainer blobContainer = client.v1().getContainerReference(container);
        final String keyPath = path.buildAsString();
        final EnumSet<BlobListingDetails> enumBlobListingDetails = EnumSet.of(BlobListingDetails.METADATA);

        for (ListBlobItem blobItem : blobContainer.listBlobs(keyPath, false, enumBlobListingDetails, null, client.v2().get())) {
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
}
