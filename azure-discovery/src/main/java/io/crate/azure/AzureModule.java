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

package io.crate.azure;

import com.microsoft.windowsazure.core.Builder.Registry;
import io.crate.azure.discovery.AzureDiscovery;
import io.crate.azure.management.AzureComputeService;
import io.crate.azure.management.AzureComputeService.Management;
import io.crate.azure.management.AzureComputeServiceImpl;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.settings.Settings;

/**
 * Azure Module
 * <p>
 * <ul>
 * <li>If needed this module will bind azure discovery service by default to AzureComputeServiceImpl.</li>
 * </ul>
 *
 * @see io.crate.azure.management.AzureComputeServiceImpl
 */
public class AzureModule extends AbstractModule {

    // pkg private so it is settable by tests
    static Class<? extends AzureComputeService> computeServiceImpl = AzureComputeServiceImpl.class;

    public static Class<? extends AzureComputeService> getComputeServiceImpl() {
        return computeServiceImpl;
    }

    @Override
    protected void configure() {
        bind(AzureComputeService.class).to(computeServiceImpl).asEagerSingleton();
    }

    public static void registerServices(Registry registry) {
        // taken from https://github.com/Appdynamics/azure-connector-extension/blob/master/src/main/java/com/appdynamics/connectors/azure/ConnectorLocator.java
        new com.microsoft.windowsazure.core.pipeline.apache.Exports().register(registry);
        new com.microsoft.windowsazure.core.pipeline.jersey.Exports().register(registry);
        new com.microsoft.windowsazure.core.utils.Exports().register(registry);
        new com.microsoft.windowsazure.credentials.Exports().register(registry);
        new com.microsoft.windowsazure.management.configuration.Exports().register(registry);
        new com.microsoft.azure.management.compute.Exports().register(registry);
        new com.microsoft.azure.management.storage.Exports().register(registry);
        new com.microsoft.azure.management.network.Exports().register(registry);
    }

    /**
     * Check if discovery is meant to start
     *
     * @param settings settings to extract cloud enabled parameter from
     * @return true if we can start discovery features
     */
    public static boolean isCloudReady(Settings settings) {
        return settings.getAsBoolean("cloud.enabled", true);
    }

    /**
     * Check if discovery is meant to start
     *
     * @param settings settings to extract cloud enabled parameter from
     * @return true if we can start discovery features
     */
    public static boolean isDiscoveryReady(Settings settings, Logger logger) {
        // Cloud services are disabled
        if (!isCloudReady(settings)) {
            logger.trace("cloud settings are disabled");
            return false;
        }

        // User set discovery.type: azure
        if (!AzureDiscovery.AZURE.equalsIgnoreCase(settings.get("discovery.type"))) {
            logger.trace("discovery.type not set to {}", AzureDiscovery.AZURE);
            return false;
        }

        if (isPropertyMissing(settings, Management.SUBSCRIPTION_ID.getKey()) ||
            isPropertyMissing(settings, Management.RESOURCE_GROUP_NAME.getKey()) ||
            isPropertyMissing(settings, Management.TENANT_ID.getKey()) ||
            isPropertyMissing(settings, Management.APP_ID.getKey()) ||
            isPropertyMissing(settings, Management.APP_SECRET.getKey())
            ) {
            logger.warn("one or more azure discovery settings are missing. " +
                        "Check elasticsearch.yml file. Should have [{}], [{}], [{}] and [{}].",
                Management.SUBSCRIPTION_ID,
                Management.RESOURCE_GROUP_NAME,
                Management.TENANT_ID,
                Management.APP_ID,
                Management.APP_SECRET);
            return false;
        }

        String discoveryType = AzureComputeService.Discovery.DISCOVERY_METHOD.get(settings);
        if (!(AzureDiscovery.SUBNET.equalsIgnoreCase(discoveryType) ||
              AzureDiscovery.VNET.equalsIgnoreCase(discoveryType) ||
              discoveryType == null)) {
            logger.warn("discovery.azure.method must be set to {} or {}. Ignoring value {}", AzureDiscovery.VNET, AzureDiscovery.SUBNET, discoveryType);
        }

        logger.trace("all required properties for azure discovery are set!");

        return true;
    }

    private static boolean isPropertyMissing(Settings settings, String name) throws ElasticsearchException {
        return !Strings.hasText(settings.get(name));
    }
}
