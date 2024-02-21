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

import java.net.URI;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;

import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

/**
 * Google Cloud Storage implementation of the BlobStoreRepository
 * <p>
 * This repository supports the following settings
 * <dl>
 * <dt>{@code bucket}</dt><dd>Bucket name</dd>
 * <dt>{@code base_path}</dt><dd>Base path (blob name prefix) in the bucket</dd>
 * <dt>{@code private_key_id}</dt><dd>Private key in PKCS 8 format from the google service account credentials.</dd>
 * <dt>{@code private_key}</dt><dd>Private key from the google service account credentials.</dd>
 * <dt>{@code client_id}</dt><dd>client id from the google service account credentials.</dd>
 * <dt>{@code client_email}</dt><dd>client email from the google service account credentials.</dd>
 * <dt>{@code token_uri}</dt><dd>Endpoint oauth token URI, only used for testing to connect to an alternative oauth provider.</dd>
 * <dt>{@code endpoint}</dt><dd>Endpoint root URL, only used for testing to connect to an alternative storage.</dd>
 * </dl>
 */
public class GCSRepository extends BlobStoreRepository {

    static final Setting<String> BUCKET_SETTING =
        Setting.simpleString("bucket");

    static final Setting<String> BASE_PATH_SETTING =
        Setting.simpleString("base_path", "");

    static final Setting<SecureString> PROJECT_ID_SETTING =
        Setting.secureString("project_id", Property.Masked);

    static final Setting<SecureString> PRIVATE_KEY_ID_SETTING =
        Setting.secureString("private_key_id", Property.Masked);

    static final Setting<SecureString> PRIVATE_KEY_SETTING =
        Setting.secureString("private_key", Property.Masked);

    static final Setting<SecureString> CLIENT_EMAIL_SETTING =
        Setting.secureString("client_email", Property.Masked);

    static final Setting<SecureString> CLIENT_ID_SETTING =
        Setting.secureString("client_id", Property.Masked);

    static final Setting<String> ENDPOINT_SETTING =
        Setting.simpleString("endpoint");

    static final Setting<String> TOKEN_URI_SETTING =
        Setting.simpleString("token_uri", "https://oauth2.googleapis.com/token");

    public GCSRepository(RepositoryMetadata metadata,
                         NamedXContentRegistry namedXContentRegistry,
                         ClusterService clusterService,
                         RecoverySettings recoverySettings) {
        super(metadata, namedXContentRegistry, clusterService,
            recoverySettings, buildBasePath(metadata));
    }

    private static BlobPath buildBasePath(RepositoryMetadata metadata) {
        final String basePath = BASE_PATH_SETTING.get(metadata.settings());
        return Strings.hasLength(basePath)
            ? new BlobPath().add(basePath)
            : BlobPath.cleanPath();
    }

    @Override
    protected BlobStore createBlobStore() throws Exception {
        Settings settings = metadata.settings();

        var credentials = ServiceAccountCredentials
            .newBuilder()
            .setClientId(CLIENT_ID_SETTING.get(settings).toString())
            .setClientEmail(CLIENT_EMAIL_SETTING.get(settings).toString())
            .setPrivateKeyId(PRIVATE_KEY_ID_SETTING.get(settings).toString())
            .setPrivateKeyString(privateKey())
            .setTokenServerUri(tokenUri())
            .setProjectId(PROJECT_ID_SETTING.get(settings).toString())
            .build();

        StorageOptions.Builder storageBuilder = StorageOptions
            .newBuilder()
            .setCredentials(credentials);

        if (ENDPOINT_SETTING.exists(settings)) {
            storageBuilder.setHost(ENDPOINT_SETTING.get(settings));
        }

        Storage storage = storageBuilder.setCredentials(credentials)
                .build()
                .getService();

        return new GCSBlobStore(storage, BUCKET_SETTING.get(settings));
    }

    private String privateKey() {
        SecureString secureString = PRIVATE_KEY_SETTING.get(metadata.settings());
        return secureString.toString().replaceAll("\\\\n", "\n");
    }

    private URI tokenUri() {
        return URI.create(TOKEN_URI_SETTING.get(metadata.settings()));
    }
}
