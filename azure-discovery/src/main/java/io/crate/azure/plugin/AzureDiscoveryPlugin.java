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
import io.crate.azure.discovery.AzureDiscovery;
import io.crate.azure.discovery.AzureUnicastHostsProvider;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.DiscoveryModule;
import org.elasticsearch.plugins.Plugin;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;


public class AzureDiscoveryPlugin extends Plugin {

    protected final ESLogger logger = Loggers.getLogger(AzureDiscoveryPlugin.class);
    private final Settings settings;

    public AzureDiscoveryPlugin(Settings settings) {
        this.settings = settings;
    }

    @Override
    public String name() {
        return "crate-azure-discovery";
    }

    @Override
    public String description() {
        return "Azure Discovery Plugin";
    }

    @Override
    public Collection<Module> nodeModules() {
        List<Module> modules = new ArrayList<>();
        if (AzureModule.isDiscoveryReady(settings, logger)) {
            modules.add(new AzureModule());
        }
        return modules;
    }

    @Override
    public Collection<Class<? extends LifecycleComponent>> nodeServices() {
        Collection<Class<? extends LifecycleComponent>> services = new ArrayList<>();
        if (AzureModule.isDiscoveryReady(settings, logger)) {
            services.add(AzureModule.getComputeServiceImpl());
        }
        return services;
    }

    public void onModule(DiscoveryModule discoveryModule) {
        if (AzureModule.isDiscoveryReady(settings, logger)) {
            discoveryModule.addDiscoveryType("azure", AzureDiscovery.class);
            discoveryModule.addUnicastHostProvider(AzureUnicastHostsProvider.class);
        }
    }

}
