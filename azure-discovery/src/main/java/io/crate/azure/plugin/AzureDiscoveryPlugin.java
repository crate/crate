/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.azure.plugin;

import io.crate.azure.AzureModule;
import io.crate.azure.discovery.AzureUnicastHostsProvider;
import io.crate.azure.management.AzureComputeServiceImpl;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.zen.UnicastHostsProvider;
import org.elasticsearch.plugins.DiscoveryPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchRequestParsers;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.watcher.ResourceWatcherService;

import java.util.*;
import java.util.function.Supplier;

import static io.crate.azure.management.AzureComputeService.Discovery.*;
import static io.crate.azure.management.AzureComputeService.Management.*;


public class AzureDiscoveryPlugin extends Plugin implements DiscoveryPlugin {

    private final Settings settings;
    private final AzureComputeServiceImpl azureComputeService;

    protected final Logger logger = Loggers.getLogger(AzureDiscoveryPlugin.class);

    public AzureDiscoveryPlugin(Settings settings) {
        this.settings = settings;
        this.azureComputeService = new AzureComputeServiceImpl(settings);
    }

    public String name() {
        return "crate-azure-discovery";
    }

    public String description() {
        return "Azure Discovery Plugin";
    }

    @Override
    public List<Setting<?>> getSettings() {
        return Arrays.asList(
            SUBSCRIPTION_ID,
            RESOURCE_GROUP_NAME,
            TENANT_ID,
            APP_ID,
            APP_SECRET,
            REFRESH,
            HOST_TYPE,
            DISCOVERY_METHOD
        );
    }

    @Override
    public Collection<Object> createComponents(Client client,
                                               ClusterService clusterService,
                                               ThreadPool threadPool,
                                               ResourceWatcherService resourceWatcherService,
                                               ScriptService scriptService,
                                               SearchRequestParsers searchRequestParsers) {
        return Collections.singletonList(azureComputeService);
    }

    @Override
    public Map<String, Supplier<UnicastHostsProvider>> getZenHostsProviders(TransportService transportService,
                                                                            NetworkService networkService) {
        return Collections.singletonMap(
            AzureModule.AZURE,
            () -> {
                if (AzureModule.isCloudReady(settings)) {
                    return new AzureUnicastHostsProvider(settings, azureComputeService, transportService, networkService);
                } else {
                    return Collections::emptyList;
                }
            });
    }
}
