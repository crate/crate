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

import com.microsoft.azure.management.compute.ComputeManagementClient;
import com.microsoft.azure.management.network.NetworkResourceProviderClient;
import com.microsoft.windowsazure.Configuration;
import io.crate.azure.management.AzureComputeServiceAbstractMock;
import io.crate.azure.plugin.AzureDiscoveryPlugin;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;

import java.util.List;

/**
 * Mock Azure API with a single started node
 */
public class AzureComputeServiceSimpleMock extends AzureComputeServiceAbstractMock {

    @Inject
    public AzureComputeServiceSimpleMock(Settings settings) {
        super(settings);
    }

    @Override
    public Configuration configuration() {
        return null;
    }

    @Override
    public ComputeManagementClient computeManagementClient() {
        return null;
    }

    @Override
    public NetworkResourceProviderClient networkResourceClient() {
        return null;
    }

    public static class TestPlugin extends AzureDiscoveryPlugin {

        public TestPlugin(Settings settings) {
            super(settings);
        }

        @Override
        public String name() {
            return "mock-compute-service";
        }

        @Override
        public String description() {
            return "plugs in a mock compute service for testing";
        }

        public void onModule(AzureModule azureModule) {
            azureModule.computeServiceImpl = AzureComputeServiceSimpleMock.class;
        }

        @Override
        public List<Setting<?>> getSettings() {
            return super.getSettings();
        }
    }

}
