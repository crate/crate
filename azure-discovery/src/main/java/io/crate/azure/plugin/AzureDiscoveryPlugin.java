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

import io.crate.azure.AzureConfiguration;
import io.crate.azure.discovery.AzureUnicastHostsProvider;
import io.crate.azure.management.AzureComputeService;
import io.crate.azure.management.AzureComputeServiceImpl;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import io.crate.es.client.Client;
import io.crate.es.cluster.service.ClusterService;
import io.crate.es.common.io.stream.NamedWriteableRegistry;
import io.crate.es.common.network.NetworkService;
import io.crate.es.common.settings.Setting;
import io.crate.es.common.settings.Settings;
import io.crate.es.common.xcontent.NamedXContentRegistry;
import io.crate.es.discovery.zen.UnicastHostsProvider;
import io.crate.es.env.Environment;
import io.crate.es.env.NodeEnvironment;
import io.crate.es.plugins.DiscoveryPlugin;
import io.crate.es.plugins.Plugin;
import io.crate.es.threadpool.ThreadPool;
import io.crate.es.transport.TransportService;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static io.crate.azure.management.AzureComputeService.Discovery.DISCOVERY_METHOD;
import static io.crate.azure.management.AzureComputeService.Discovery.HOST_TYPE;
import static io.crate.azure.management.AzureComputeService.Discovery.REFRESH;
import static io.crate.azure.management.AzureComputeService.Management.APP_ID;
import static io.crate.azure.management.AzureComputeService.Management.APP_SECRET;
import static io.crate.azure.management.AzureComputeService.Management.RESOURCE_GROUP_NAME;
import static io.crate.azure.management.AzureComputeService.Management.SUBSCRIPTION_ID;
import static io.crate.azure.management.AzureComputeService.Management.TENANT_ID;


public class AzureDiscoveryPlugin extends Plugin implements DiscoveryPlugin {

    private final Settings settings;
    private AzureComputeServiceImpl azureComputeService;

    protected final Logger logger = LogManager.getLogger(AzureDiscoveryPlugin.class);

    public AzureDiscoveryPlugin(Settings settings) {
        this.settings = settings;
    }

    private AzureComputeService azureComputeService() {
        if (azureComputeService == null) {
            azureComputeService = new AzureComputeServiceImpl(settings);
        }
        return azureComputeService;
    }

    public String name() {
        return "crate-azure-discovery";
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
                                               NamedXContentRegistry xContentRegistry,
                                               Environment environment,
                                               NodeEnvironment nodeEnvironment,
                                               NamedWriteableRegistry namedWriteableRegistry) {
        if (AzureConfiguration.isDiscoveryReady(settings, logger)) {
            return Collections.singletonList(azureComputeService());
        }
        return Collections.emptyList();
    }

    @Override
    public Map<String, Supplier<UnicastHostsProvider>> getZenHostsProviders(TransportService transportService,
                                                                            NetworkService networkService) {
        return Collections.singletonMap(
            AzureConfiguration.AZURE,
            () -> {
                if (AzureConfiguration.isDiscoveryReady(settings, logger)) {
                    return new AzureUnicastHostsProvider(settings, azureComputeService(), transportService, networkService);
                } else {
                    return hostsResolver -> Collections.emptyList();
                }
            });
    }
}
