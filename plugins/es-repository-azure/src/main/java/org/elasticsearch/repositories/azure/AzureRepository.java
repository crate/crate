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

import java.net.URI;
import java.util.List;
import java.util.function.Function;

import org.apache.opendal.AsyncExecutor;
import org.apache.opendal.ServiceConfig;
import org.apache.opendal.ServiceConfig.Azblob;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;

import io.crate.opendal.OpenDALBlobStore;
import io.crate.types.DataTypes;

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

    public static final String TYPE = "azure";

    public static final class Repository {

        static final Setting<SecureString> SAS_TOKEN_SETTING = Setting.maskedString("sas_token");

        static final Setting<SecureString> ACCOUNT_SETTING = Setting.maskedString("account");

        static final Setting<SecureString> KEY_SETTING = Setting.maskedString("key");

        static final Setting<String> CONTAINER_SETTING = new Setting<>(
            "container",
            "crate-snapshots",
            Function.identity(),
            DataTypes.STRING,
            Property.NodeScope);

        static final Setting<String> BASE_PATH_SETTING =
            Setting.simpleString("base_path", Property.NodeScope);

        static final Setting<ByteSizeValue> CHUNK_SIZE_SETTING = Setting.byteSizeSetting(
            "chunk_size",
            new ByteSizeValue(256, ByteSizeUnit.MB),
            new ByteSizeValue(1, ByteSizeUnit.BYTES),
            new ByteSizeValue(256, ByteSizeUnit.MB),
            Property.NodeScope);

        static final Setting<Boolean> READONLY_SETTING =
            Setting.boolSetting("readonly", false, Property.NodeScope);

        /**
         * max_retries: Number of retries in case of Azure errors.
         * Defaults to 3 (RetryPolicy.DEFAULT_CLIENT_RETRY_COUNT).
         */
        static final Setting<Integer> MAX_RETRIES_SETTING = Setting.intSetting(
            "max_retries",
            3,
            Setting.Property.NodeScope);

        static final Setting<String> ENDPOINT_SETTING =
            Setting.simpleString(
                "endpoint",
                val -> validateEndpoint(val, "endpoint"),
                Property.NodeScope);

        /**
         * Verify that the endpoint is a valid URI with a nonnull scheme and host.
         * Otherwise, the Azure SDK will throw an NPE exception if host is null.
         */
        private static void validateEndpoint(String endpoint, String settingName) {
            if (Strings.isNullOrEmpty(endpoint)) {
                return;
            }
            try {
                var uri = new URI(endpoint);
                if (uri.getScheme() == null || uri.getHost() == null) {
                    throw new ElasticsearchParseException("Invalid " + settingName + " URI: " + endpoint);
                }
            } catch (Exception e) {
                throw new ElasticsearchParseException("Invalid " + settingName + " URI: " + endpoint, e);
            }
        }
    }

    public static List<Setting<?>> optionalSettings() {
        return List.of(
            Repository.CONTAINER_SETTING,
            Repository.BASE_PATH_SETTING,
            Repository.CHUNK_SIZE_SETTING,
            Repository.READONLY_SETTING,
            COMPRESS_SETTING,
            // client specific repository settings
            Repository.MAX_RETRIES_SETTING,
            Repository.ENDPOINT_SETTING,
            Repository.KEY_SETTING,
            Repository.SAS_TOKEN_SETTING
        );
    }

    public static List<Setting<?>> mandatorySettings() {
        return List.of(
            Repository.ACCOUNT_SETTING
        );
    }

    private final ByteSizeValue chunkSize;

    private final AsyncExecutor executor;

    public AzureRepository(RepositoryMetadata metadata,
                           NamedWriteableRegistry namedWriteableRegistry,
                           NamedXContentRegistry namedXContentRegistry,
                           ClusterService clusterService,
                           RecoverySettings recoverySettings,
                           AsyncExecutor executor) {
        super(
            metadata,
            namedWriteableRegistry,
            namedXContentRegistry,
            clusterService,
            recoverySettings,
            buildBasePath(metadata));
        this.chunkSize = Repository.CHUNK_SIZE_SETTING.get(metadata.settings());
        this.executor = executor;
    }

    private static BlobPath buildBasePath(RepositoryMetadata metadata) {
        String basePathStr = Repository.BASE_PATH_SETTING.get(metadata.settings());
        final String basePath = Strings.trimLeadingCharacter(basePathStr, '/');
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


    /**
     * {@inheritDoc}
     */
    @Override
    protected OpenDALBlobStore createBlobStore() {
        Settings repoSettings = metadata.settings();
        String accountName = Repository.ACCOUNT_SETTING.get(repoSettings).toString();
        String endpoint = Repository.ENDPOINT_SETTING.get(repoSettings);
        if (endpoint == null) {
            endpoint = "https://" + accountName + ".blob.core.windows.net";
        }
        SecureString accountKey = Repository.KEY_SETTING.getOrNull(repoSettings);
        SecureString sasToken = Repository.SAS_TOKEN_SETTING.getOrNull(repoSettings);
        if (sasToken == null && accountKey == null) {
            throw new SettingsException("Neither a secret key nor a shared access token was set.");
        }
        if (sasToken != null && accountKey != null) {
            throw new SettingsException("Both a secret as well as a shared access token were set.");
        }
        Azblob config = ServiceConfig.Azblob.builder()
            .accountName(accountName)
            .accountKey(accountKey == null ? null : accountKey.toString())
            .sasToken(sasToken == null ? null : sasToken.toString())
            .container(Repository.CONTAINER_SETTING.get(repoSettings))
            .endpoint(endpoint)
            .build();
        return new OpenDALBlobStore(
            executor,
            config,
            bufferSize,
            Repository.MAX_RETRIES_SETTING.get(repoSettings),
            true
        );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected ByteSizeValue chunkSize() {
        return chunkSize;
    }
}
