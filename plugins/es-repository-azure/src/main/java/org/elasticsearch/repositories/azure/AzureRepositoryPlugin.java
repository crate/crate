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

import static org.elasticsearch.repositories.azure.AzureRepository.Repository.ACCOUNT_SETTING;
import static org.elasticsearch.repositories.azure.AzureRepository.Repository.KEY_SETTING;
import static org.elasticsearch.repositories.azure.AzureRepository.Repository.SAS_TOKEN_SETTING;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.RepositoryPlugin;
import org.elasticsearch.repositories.Repository;

import io.crate.analyze.repositories.TypeSettings;

/**
 * A plugin to add a repository type that writes to and from the Azure cloud storage service.
 */
public class AzureRepositoryPlugin extends Plugin implements RepositoryPlugin {

    @Override
    public Map<String, Repository.Factory> getRepositories(Environment env,
                                                           NamedWriteableRegistry namedWriteableRegistry,
                                                           NamedXContentRegistry namedXContentRegistry,
                                                           ClusterService clusterService,
                                                           RecoverySettings recoverySettings) {
        return Collections.singletonMap(
            AzureRepository.TYPE,
            new Repository.Factory() {

                @Override
                public TypeSettings settings() {
                    return new TypeSettings(
                        AzureRepository.mandatorySettings(), AzureRepository.optionalSettings());
                }

                @Override
                public Repository create(RepositoryMetadata metadata) throws Exception {
                    return new AzureRepository(
                        metadata,
                        namedWriteableRegistry,
                        namedXContentRegistry,
                        clusterService,
                        recoverySettings
                    );
                }
            }
        );
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(ACCOUNT_SETTING, SAS_TOKEN_SETTING, KEY_SETTING);
    }
}
