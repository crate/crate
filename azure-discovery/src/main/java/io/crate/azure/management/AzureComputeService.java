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

import com.microsoft.azure.management.compute.ComputeManagementClient;
import com.microsoft.azure.management.network.NetworkResourceProviderClient;
import com.microsoft.windowsazure.Configuration;
import io.crate.azure.discovery.AzureDiscovery;
import io.crate.azure.discovery.AzureUnicastHostsProvider;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.unit.TimeValue;

import java.util.function.Function;

/**
 *
 */
public interface AzureComputeService extends LifecycleComponent {

    final class Management {
        public static final Setting<String> SUBSCRIPTION_ID = Setting.simpleString(
            "cloud.azure.management.subscription.id", Setting.Property.NodeScope, Setting.Property.Filtered);
        public static final Setting<String> RESOURCE_GROUP_NAME = Setting.simpleString(
            "cloud.azure.management.resourcegroup.name", Setting.Property.NodeScope);

        public static final Setting<String> TENANT_ID = Setting.simpleString(
            "cloud.azure.management.tenant.id", Setting.Property.NodeScope, Setting.Property.Filtered);
        public static final Setting<String> APP_ID = Setting.simpleString(
            "cloud.azure.management.app.id", Setting.Property.NodeScope, Setting.Property.Filtered);
        public static final Setting<String> APP_SECRET = Setting.simpleString(
            "cloud.azure.management.app.secret", Setting.Property.NodeScope, Setting.Property.Filtered);
    }

    final class Discovery {
        public static final Setting<TimeValue> REFRESH = Setting.timeSetting(
            "discovery.azure.refresh_interval", TimeValue.timeValueSeconds(5L), Setting.Property.NodeScope);
        public static final Setting<String> HOST_TYPE = new Setting<>(
            "discovery.azure.host.type", s -> AzureUnicastHostsProvider.HostType.PRIVATE_IP.name(),
            Function.identity(), Setting.Property.NodeScope);
        public static final Setting<String> DISCOVERY_METHOD = new Setting<>(
            "discovery.azure.method", s -> AzureDiscovery.VNET, Function.identity(), Setting.Property.NodeScope);
    }

    Configuration configuration();

    ComputeManagementClient computeManagementClient();

    NetworkResourceProviderClient networkResourceClient();
}
