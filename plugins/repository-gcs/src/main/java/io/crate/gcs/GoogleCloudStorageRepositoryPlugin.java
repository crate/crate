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

import java.util.List;
import java.util.Map;

import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.RepositoryPlugin;
import org.elasticsearch.repositories.Repository;

import io.crate.analyze.repositories.TypeSettings;


/**
 * Based on https://github.com/opensearch-project/OpenSearch/blob/main/plugins/repository-gcs/src/main/java/org/opensearch/repositories/gcs/GoogleCloudStoragePlugin.java
 */
public class GoogleCloudStorageRepositoryPlugin extends Plugin implements RepositoryPlugin {

    private final GoogleCloudStorageService service;

    public GoogleCloudStorageRepositoryPlugin() {
        this.service = new GoogleCloudStorageService();
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(
            GoogleCloudStorageRepository.COMPRESS_SETTING,
            GoogleCloudStorageRepository.BUCKET_SETTING,
            GoogleCloudStorageRepository.BASE_PATH_SETTING,
            GoogleCloudStorageRepository.CHUNK_SIZE_SETTING,
            GoogleCloudStorageClientSettings.PROJECT_ID_SETTING,
            GoogleCloudStorageClientSettings.PRIVATE_KEY_ID_SETTING,
            GoogleCloudStorageClientSettings.PRIVATE_KEY_SETTING,
            GoogleCloudStorageClientSettings.CLIENT_EMAIL_SETTING,
            GoogleCloudStorageClientSettings.CLIENT_ID_SETTING,
            GoogleCloudStorageClientSettings.ENDPOINT_SETTING,
            GoogleCloudStorageClientSettings.TOKEN_URI_SETTING,
            GoogleCloudStorageClientSettings.CONNECT_TIMEOUT_SETTING,
            GoogleCloudStorageClientSettings.READ_TIMEOUT_SETTING
            );
    }

    @Override
    public Map<String, Repository.Factory> getRepositories(
        Environment environment, NamedXContentRegistry namedXContentRegistry,
        ClusterService clusterService, RecoverySettings recoverySettings) {
        return Map.of(
            "gcs", new Repository.Factory() {
                @Override
                public TypeSettings settings() {
                    return new TypeSettings(
                        // Required settings
                        List.of(
                            GoogleCloudStorageRepository.BUCKET_SETTING,
                            GoogleCloudStorageClientSettings.PROJECT_ID_SETTING,
                            GoogleCloudStorageClientSettings.PRIVATE_KEY_ID_SETTING,
                            GoogleCloudStorageClientSettings.PRIVATE_KEY_SETTING,
                            GoogleCloudStorageClientSettings.CLIENT_ID_SETTING,
                            GoogleCloudStorageClientSettings.CLIENT_EMAIL_SETTING
                        ),
                        // Optional settings
                        List.of(
                            GoogleCloudStorageRepository.CHUNK_SIZE_SETTING,
                            GoogleCloudStorageRepository.COMPRESS_SETTING,
                            GoogleCloudStorageRepository.BASE_PATH_SETTING,
                            GoogleCloudStorageClientSettings.ENDPOINT_SETTING,
                            GoogleCloudStorageClientSettings.TOKEN_URI_SETTING,
                            GoogleCloudStorageClientSettings.CONNECT_TIMEOUT_SETTING,
                            GoogleCloudStorageClientSettings.READ_TIMEOUT_SETTING
                        )
                    );
                }

                @Override
                public Repository create(RepositoryMetadata metadata) {
                    return new GoogleCloudStorageRepository(metadata, namedXContentRegistry, clusterService, service, recoverySettings);
                }
            }
        );
    }
}
