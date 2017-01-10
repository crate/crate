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

package io.crate.azure.management;

import com.microsoft.aad.adal4j.AuthenticationResult;
import com.microsoft.azure.management.compute.ComputeManagementClient;
import com.microsoft.azure.management.compute.ComputeManagementService;
import com.microsoft.azure.management.network.NetworkResourceProviderClient;
import com.microsoft.azure.management.network.NetworkResourceProviderService;
import com.microsoft.azure.utility.AuthHelper;
import com.microsoft.windowsazure.Configuration;
import com.microsoft.windowsazure.core.DefaultBuilder;
import com.microsoft.windowsazure.management.configuration.ManagementConfiguration;
import io.crate.azure.AzureModule;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;

import java.io.IOException;
import java.net.URI;

import static io.crate.azure.management.AzureComputeService.Management.*;

public class AzureComputeServiceImpl extends AbstractLifecycleComponent implements AzureComputeService {

    private final String resourceGroupName;
    private final String subscriptionId;
    private final String tenantId;
    private final String appId;
    private final String appSecret;

    static final class Azure {
        private static final String ENDPOINT = "https://management.core.windows.net/";
        private static final String AUTH_ENDPOINT = "https://login.windows.net/";
    }

    private ComputeManagementClient computeManagementClient;
    private NetworkResourceProviderClient networkResourceClient;
    private Configuration configuration;

    @Inject
    public AzureComputeServiceImpl(Settings settings) {
        super(settings);
        subscriptionId = SUBSCRIPTION_ID.get(settings);
        tenantId = TENANT_ID.get(settings);
        appId = APP_ID.get(settings);
        appSecret = APP_SECRET.get(settings);
        resourceGroupName = Management.RESOURCE_GROUP_NAME.get(settings);
    }

    @Nullable
    @Override
    public ComputeManagementClient computeManagementClient() {
        if (computeManagementClient == null) {
            Configuration conf = configuration();
            if (conf == null) {
                return null;
            }
            computeManagementClient = ComputeManagementService.create(conf);
        }
        return computeManagementClient;
    }

    @Nullable
    @Override
    public NetworkResourceProviderClient networkResourceClient() {
        if (networkResourceClient == null) {
            Configuration conf = configuration();
            if (conf == null) {
                return null;
            }
            networkResourceClient = NetworkResourceProviderService.create(conf);
        }
        return networkResourceClient;
    }

    @Override
    public Configuration configuration() {
        if (configuration == null) {
            logger.trace("Creating new Azure configuration for [{}], [{}]", subscriptionId, resourceGroupName);
            configuration = createConfiguration();
        }
        return configuration;
    }

    private Configuration createConfiguration() {
        Configuration conf = null;
        try {
            AuthenticationResult authRes = AuthHelper.getAccessTokenFromServicePrincipalCredentials(
                Azure.ENDPOINT,
                Azure.AUTH_ENDPOINT,
                tenantId,
                appId,
                appSecret
            );

            DefaultBuilder registry = DefaultBuilder.create();
            AzureModule.registerServices(registry);
            conf = ManagementConfiguration.configure(null, new Configuration(registry),
                URI.create(Azure.ENDPOINT), subscriptionId, authRes.getAccessToken());
        } catch (Exception e) {
            logger.error("Could not create configuration for Azure clients", e);
        }
        return conf;
    }

    @Override
    protected void doStart() throws ElasticsearchException {
    }

    @Override
    protected void doStop() throws ElasticsearchException {
    }

    @Override
    protected void doClose() throws ElasticsearchException {
        if (computeManagementClient != null) {
            try {
                computeManagementClient.close();
            } catch (IOException e) {
                logger.error("Error while closing Azure computeManagementClient", e);
            }
        }
        if (networkResourceClient != null) {
            try {
                networkResourceClient.close();
            } catch (IOException e) {
                logger.error("Error while closing Azure networkResourceClient", e);
            }
        }
    }
}
