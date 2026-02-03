/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.gcs;

import static org.elasticsearch.common.settings.Setting.byteSizeSetting;
import static org.elasticsearch.common.settings.Setting.simpleString;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import org.apache.opendal.AsyncExecutor;
import org.apache.opendal.ServiceConfig;
import org.apache.opendal.ServiceConfig.Gcs;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;

import io.crate.common.annotations.VisibleForTesting;
import io.crate.opendal.OpenDALBlobStore;

/**
 * Based on https://github.com/opensearch-project/OpenSearch/blob/main/plugins/repository-gcs/src/main/java/org/opensearch/repositories/gcs/GoogleCloudStorageRepository.java
 */
public class GCSRepository extends BlobStoreRepository {

    // package private for testing
    static final ByteSizeValue MIN_CHUNK_SIZE = new ByteSizeValue(1, ByteSizeUnit.BYTES);

    /**
     * Maximum allowed object size in GCS.
     *
     * @see <a href="https://cloud.google.com/storage/quotas#objects">GCS documentation</a> for details.
     */
    static final ByteSizeValue MAX_CHUNK_SIZE = new ByteSizeValue(5, ByteSizeUnit.TB);

    static final Setting<String> BUCKET_SETTING = simpleString("bucket", Property.NodeScope, Property.Dynamic);

    static final Setting<String> BASE_PATH_SETTING = simpleString("base_path", Property.NodeScope, Property.Dynamic);

    static final Setting<ByteSizeValue> CHUNK_SIZE_SETTING = byteSizeSetting(
        "chunk_size",
        MAX_CHUNK_SIZE,
        MIN_CHUNK_SIZE,
        MAX_CHUNK_SIZE,
        Property.NodeScope,
        Property.Dynamic
    );

    private static final String AUTH_URI = "https://accounts.google.com/o/oauth2/auth";
    private static final String TOKEN_URI = "https://oauth2.googleapis.com/token";
    private static final String AUTH_PROVIDER_URL = "https://www.googleapis.com/oauth2/v1/certs";

    private static final int NUM_RETRIES = 3;
    private static final boolean JITTER = true;

    private final ByteSizeValue chunkSize;
    private final AsyncExecutor executor;

    public GCSRepository(
            RepositoryMetadata metadata,
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
            buildBasePath(metadata)
        );
        this.executor = executor;
        this.chunkSize = CHUNK_SIZE_SETTING.get(metadata.settings());
    }

    @VisibleForTesting
    static ServiceConfig.Gcs createConfig(Settings repoSettings) {
        String credentials;
        try (var builder = JsonXContent.builder()
                .startObject()
                .field("type", "service_account")
                .field("project_id", GCSClientSettings.PROJECT_ID_SETTING.get(repoSettings))
                .field("private_key_id", GCSClientSettings.PRIVATE_KEY_ID_SETTING.get(repoSettings).toString())
                .field("private_key", GCSClientSettings.privateKey(repoSettings))
                .field("client_id", GCSClientSettings.CLIENT_ID_SETTING.get(repoSettings).toString())
                .field("client_email", GCSClientSettings.CLIENT_EMAIL_SETTING.get(repoSettings).toString())
                .field("auth_uri", AUTH_URI)
                .field("token_uri", TOKEN_URI)
                .field("auth_provider_x509_cert_url", AUTH_PROVIDER_URL)
                .endObject()) {

            credentials = Strings.toString(builder);
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }
        String endpoint = GCSClientSettings.ENDPOINT_SETTING.getOrNull(repoSettings);
        Gcs.GcsBuilder configBuilder = ServiceConfig.Gcs.builder()
            .allowAnonymous(true)
            .endpoint(endpoint)
            .credential(Base64.getEncoder().encodeToString(credentials.getBytes(StandardCharsets.UTF_8)))
            .bucket(BUCKET_SETTING.get(repoSettings));
        if (endpoint != null && !endpoint.contains("storage.googleapis.com")) {
            configBuilder.disableVmMetadata(true);
            configBuilder.disableConfigLoad(true);
        }
        return configBuilder.build();
    }

    @Override
    protected OpenDALBlobStore createBlobStore() {
        return new OpenDALBlobStore(
            executor,
            createConfig(metadata.settings()),
            bufferSize,
            NUM_RETRIES,
            JITTER
        );
    }

    private static BlobPath buildBasePath(RepositoryMetadata metadata) {
        String basePath = BASE_PATH_SETTING.get(metadata.settings());
        if (Strings.hasLength(basePath)) {
            BlobPath path = new BlobPath();
            for (String elem : basePath.split("/")) {
                path = path.add(elem);
            }
            return path;
        } else {
            return BlobPath.cleanPath();
        }
    }

    @Override
    protected ByteSizeValue chunkSize() {
        return chunkSize;
    }
}
