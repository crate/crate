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

import static org.elasticsearch.repositories.azure.AzureStorageService.MAX_CHUNK_SIZE;
import static org.elasticsearch.repositories.azure.AzureStorageService.MIN_CHUNK_SIZE;

import java.net.Proxy;
import java.util.List;
import java.util.Locale;
import java.util.function.Function;

import com.google.common.annotations.VisibleForTesting;
import com.microsoft.azure.storage.LocationMode;
import com.microsoft.azure.storage.RetryPolicy;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.threadpool.ThreadPool;

import io.crate.common.unit.TimeValue;

/**
 * Azure file system implementation of the BlobStoreRepository
 * <p>
 * Azure file system repository supports the following settings:
 * <dl>
 * <dt>{@code container}</dt><dd>Azure container name. Defaults to crate-snapshots</dd>
 * <dt>{@code base_path}</dt><dd>Specifies the path within bucket to repository data. Defaults to root directory.</dd>
 * <dt>{@code chunk_size}</dt><dd>Large file can be divided into chunks. This parameter specifies the chunk size. Defaults to 256mb.</dd>
 * <dt>{@code compress}</dt><dd>If set to true metadata files will be stored compressed. Defaults to false.</dd>
 * </dl>
 */
public class AzureRepository extends BlobStoreRepository {
    private static final Logger LOGGER = LogManager.getLogger(AzureRepository.class);

    public static final String TYPE = "azure";

    public static final class Repository {
        static final Setting<SecureString> ACCOUNT_SETTING = Setting.maskedString("account");

        static final Setting<SecureString> KEY_SETTING = Setting.maskedString("key");

        public static final Setting<String> CLIENT_NAME = Setting.simpleString("client", Property.NodeScope);

        static final Setting<String> CONTAINER_SETTING = new Setting<>(
                "container",
                "crate-snapshots",
                Function.identity(),
                Property.NodeScope);

        static final Setting<String> BASE_PATH_SETTING =
            Setting.simpleString("base_path", Property.NodeScope);

        static final Setting<LocationMode> LOCATION_MODE_SETTING = new Setting<>(
            "location_mode",
            s -> LocationMode.PRIMARY_ONLY.toString(),
            s -> LocationMode.valueOf(s.toUpperCase(Locale.ROOT)),
            Property.NodeScope);

        static final Setting<ByteSizeValue> CHUNK_SIZE_SETTING = Setting.byteSizeSetting(
            "chunk_size",
            MAX_CHUNK_SIZE,
            MIN_CHUNK_SIZE,
            MAX_CHUNK_SIZE,
            Property.NodeScope);

        static final Setting<Boolean> READONLY_SETTING =
            Setting.boolSetting("readonly", false, Property.NodeScope);

        /**
         * max_retries: Number of retries in case of Azure errors.
         * Defaults to 3 (RetryPolicy.DEFAULT_CLIENT_RETRY_COUNT).
         */
        static final Setting<Integer> MAX_RETRIES_SETTING = Setting.intSetting(
            "max_retries",
            RetryPolicy.DEFAULT_CLIENT_RETRY_COUNT,
            Setting.Property.NodeScope);

        /**
         * Azure endpoint suffix. Default to core.windows.net (CloudStorageAccount.DEFAULT_DNS).
         */
        static final Setting<String> ENDPOINT_SUFFIX_SETTING = Setting
            .simpleString("endpoint_suffix", Property.NodeScope);

        static final Setting<TimeValue> TIMEOUT_SETTING =
            Setting.timeSetting("timeout", TimeValue.timeValueMinutes(-1), Property.NodeScope);

        /**
         * The type of the proxy to connect to azure through. Can be direct (no proxy, default), http or socks
         */
        static final Setting<Proxy.Type> PROXY_TYPE_SETTING = new Setting<>(
            "proxy_type",
            "direct",
            s -> Proxy.Type.valueOf(s.toUpperCase(Locale.ROOT)),
            Property.NodeScope);

        /**
         * The host name of a proxy to connect to azure through.
         */
        static final Setting<String> PROXY_HOST_SETTING =
            Setting.simpleString("proxy_host", Property.NodeScope);

        /**
         * The port of a proxy to connect to azure through.
         */
        static final Setting<Integer> PROXY_PORT_SETTING =
            Setting.intSetting("proxy_port", 0, 0, 65535, Setting.Property.NodeScope);
    }

    public static List<Setting<?>> optionalSettings() {
        return List.of(Repository.CONTAINER_SETTING,
                       Repository.BASE_PATH_SETTING,
                       Repository.CHUNK_SIZE_SETTING,
                       Repository.READONLY_SETTING,
                       Repository.LOCATION_MODE_SETTING,
                       COMPRESS_SETTING,
                       // client specific repository settings
                       Repository.MAX_RETRIES_SETTING,
                       Repository.ENDPOINT_SUFFIX_SETTING,
                       Repository.TIMEOUT_SETTING,
                       Repository.PROXY_TYPE_SETTING,
                       Repository.PROXY_HOST_SETTING,
                       Repository.PROXY_PORT_SETTING);
    }

    public static List<Setting<?>> mandatorySettings() {
        return List.of(Repository.ACCOUNT_SETTING, Repository.KEY_SETTING);
    }

    private final ByteSizeValue chunkSize;
    private final AzureStorageService storageService;
    private final boolean readonly;

    public AzureRepository(RepositoryMetadata metadata,
                           Environment environment,
                           NamedXContentRegistry namedXContentRegistry,
                           AzureStorageService storageService,
                           ThreadPool threadPool) {
        super(metadata, environment.settings(), namedXContentRegistry, threadPool, buildBasePath(metadata));
        this.chunkSize = Repository.CHUNK_SIZE_SETTING.get(metadata.settings());
        this.storageService = storageService;

        // If the user explicitly did not define a readonly value, we set it by ourselves depending on the location mode setting.
        // For secondary_only setting, the repository should be read only
        final LocationMode locationMode = Repository.LOCATION_MODE_SETTING.get(metadata.settings());
        if (Repository.READONLY_SETTING.exists(metadata.settings())) {
            this.readonly = Repository.READONLY_SETTING.get(metadata.settings());
        } else {
            this.readonly = locationMode == LocationMode.SECONDARY_ONLY;
        }
    }

    private static BlobPath buildBasePath(RepositoryMetadata metadata) {
        final String basePath = Strings.trimLeadingCharacter(Repository.BASE_PATH_SETTING.get(metadata.settings()), '/');
        if (Strings.hasLength(basePath)) {
            // Remove starting / if any
            BlobPath path = new BlobPath();
            for (final String elem : basePath.split("/")) {
                path = path.add(elem);
            }
            return path;
        } else {
            return BlobPath.cleanPath();
        }
    }



    @VisibleForTesting
    @Override
    protected BlobStore getBlobStore() {
        return super.getBlobStore();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected AzureBlobStore createBlobStore() {
        final AzureBlobStore blobStore = new AzureBlobStore(metadata, storageService);

        LOGGER.debug((org.apache.logging.log4j.util.Supplier<?>) () -> new ParameterizedMessage(
            "using container [{}], chunk_size [{}], compress [{}], base_path [{}]",
            blobStore, chunkSize, isCompress(), basePath()));
        return blobStore;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected ByteSizeValue chunkSize() {
        return chunkSize;
    }

    @Override
    public boolean isReadOnly() {
        return readonly;
    }
}
