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

package io.crate.plugin;

import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.ClusterPlugin;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.plugins.Plugin;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;


public class SQLProxyPlugin extends Plugin implements ActionPlugin, MapperPlugin, ClusterPlugin {

    private final SQLPlugin plugin;

    public SQLProxyPlugin(Settings settings, boolean isEnterprise) {
        this.plugin = new SQLPlugin(settings, isEnterprise);
    }

    @Override
    public Settings additionalSettings() {
        return plugin.additionalSettings();
    }

    @Override
    public List<Setting<?>> getSettings() {
        return plugin.getSettings();
    }

    @Override
    public Collection<Class<? extends LifecycleComponent>> getGuiceServiceClasses() {
        return plugin.getGuiceServiceClasses();
    }

    @Override
    public Collection<Module> createGuiceModules() {
        return plugin.createGuiceModules();
    }

    @Override
    public Map<String, Mapper.TypeParser> getMappers() {
        return plugin.getMappers();
    }

    @Override
    public Collection<AllocationDecider> createAllocationDeciders(Settings settings, ClusterSettings clusterSettings) {
        return plugin.createAllocationDeciders(settings, clusterSettings);
    }

    @Override
    public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return plugin.getNamedWriteables();
    }

    @Override
    public List<NamedXContentRegistry.Entry> getNamedXContent() {
        return plugin.getNamedXContent();
    }

    @Override
    public UnaryOperator<IndexMetaData> getIndexMetaDataUpgrader() {
        return plugin.getIndexMetaDataUpgrader();
    }

    @Override
    public UnaryOperator<Map<String, IndexTemplateMetaData>> getIndexTemplateMetaDataUpgrader() {
        return plugin.getIndexTemplateMetaDataUpgrader();
    }
}
