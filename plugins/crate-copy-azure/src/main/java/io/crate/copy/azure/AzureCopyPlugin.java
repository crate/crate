/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.copy.azure;

import static io.crate.copy.azure.AzureBlobStorageSettings.SUPPORTED_SETTINGS;

import io.crate.execution.engine.collect.files.FileInputFactory;
import io.crate.execution.engine.collect.files.SchemeSettings;
import io.crate.execution.engine.export.FileOutputFactory;
import io.crate.plugin.CopyPlugin;

import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

public class AzureCopyPlugin extends Plugin implements CopyPlugin {

    public static final String OPEN_DAL_SCHEME = "azblob";
    public static final String USER_FACING_SCHEME = "az";

    private final SharedAsyncExecutor sharedAsyncExecutor;

    @Inject
    public AzureCopyPlugin(Settings settings) {
        this.sharedAsyncExecutor = new SharedAsyncExecutor(settings);
    }

    @Override
    public String scheme() {
        return USER_FACING_SCHEME;
    }

    @Override
    public FileInputFactory inputFactory() {
        return new AzureFileInputFactory(sharedAsyncExecutor);
    }

    @Override
    public FileOutputFactory outputFactory() {
        return new AzureFileOutputFactory(sharedAsyncExecutor);
    }

    @Override
    public SchemeSettings getSchemeSettings() {
        return new SchemeSettings(List.of(), SUPPORTED_SETTINGS);
    }

    @Override
    public Collection<Object> createComponents(Client client,
                                               ClusterService clusterService,
                                               ThreadPool threadPool,
                                               NamedXContentRegistry xContentRegistry,
                                               Environment environment,
                                               NodeEnvironment nodeEnvironment,
                                               NamedWriteableRegistry namedWriteableRegistry,
                                               Supplier<RepositoriesService> repositoriesServiceSupplier) {
        return Collections.singletonList(sharedAsyncExecutor);
    }
}
