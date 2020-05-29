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

import io.crate.common.unit.TimeValue;
import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.RepositoryPlugin;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.threadpool.ExecutorBuilder;
import org.elasticsearch.threadpool.ScalingExecutorBuilder;
import org.elasticsearch.threadpool.ThreadPool;

import io.crate.analyze.repositories.TypeSettings;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * A plugin to add a repository type that writes to and from the Azure cloud storage service.
 */
public class AzureRepositoryPlugin extends Plugin implements RepositoryPlugin {

    public static final String REPOSITORY_THREAD_POOL_NAME = "repository_azure";
    private final AzureStorageService azureStoreService;

    public AzureRepositoryPlugin() {
        this.azureStoreService = new AzureStorageService();
    }

    @Override
    public Map<String, Repository.Factory> getRepositories(Environment env,
                                                           NamedXContentRegistry namedXContentRegistry,
                                                           ThreadPool threadPool) {
        return Collections.singletonMap(
            AzureRepository.TYPE,
            new Repository.Factory() {

                @Override
                public TypeSettings settings() {
                    return new TypeSettings(
                        AzureRepository.mandatorySettings(), AzureRepository.optionalSettings());
                }

                @Override
                public Repository create(RepositoryMetaData metadata) throws Exception {
                    return new AzureRepository(
                        metadata,
                        env,
                        namedXContentRegistry,
                        azureStoreService,
                        threadPool
                    );
                }
            }
        );
    }

    @Override
    public List<ExecutorBuilder<?>> getExecutorBuilders(Settings settings) {
        return Collections.singletonList(executorBuilder());
    }

    public static ExecutorBuilder<?> executorBuilder() {
        return new ScalingExecutorBuilder(REPOSITORY_THREAD_POOL_NAME, 0, 32, TimeValue.timeValueSeconds(30L));
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(AzureRepository.Repository.ACCOUNT_SETTING, AzureRepository.Repository.KEY_SETTING);
    }
}
