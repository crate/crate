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

import com.google.common.annotations.VisibleForTesting;
import io.crate.common.collections.Lists2;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.ClusterPlugin;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.plugins.Plugin;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;

public class PluginLoaderPlugin extends Plugin implements ActionPlugin, MapperPlugin, ClusterPlugin {

    private static final Logger LOGGER = LogManager.getLogger(PluginLoaderPlugin.class);

    @VisibleForTesting
    final PluginLoader pluginLoader;

    @VisibleForTesting
    final Settings settings;

    private final SQLPlugin sqlPlugin;
    private final Settings additionalSettings;
    private final List<Setting<?>> settingList = new ArrayList<>();

    public PluginLoaderPlugin(Settings settings) {
        pluginLoader = new PluginLoader(settings);
        this.settings = Settings.builder()
            .put(pluginLoader.additionalSettings())
            .put(settings)
            .build();
        // SQLPlugin contains modules which use settings which may be overwritten by CratePlugins,
        // so the SQLPlugin needs to be created here with settings that incl. pluginLoader.additionalSettings
        sqlPlugin = new SQLPlugin(this.settings);
        additionalSettings = Settings.builder()
            .put(pluginLoader.additionalSettings())
            .put(sqlPlugin.additionalSettings()).build();

        settingList.add(PluginLoader.SETTING_CRATE_PLUGINS_PATH);
        settingList.addAll(pluginLoader.getSettings());
        settingList.addAll(sqlPlugin.getSettings());

    }

    @Override
    public Settings additionalSettings() {
        return additionalSettings;
    }

    @Override
    public List<Setting<?>> getSettings() {
        return settingList;
    }

    @Override
    public Collection<Class<? extends LifecycleComponent>> getGuiceServiceClasses() {
        return Lists2.concat(sqlPlugin.getGuiceServiceClasses(), pluginLoader.getGuiceServiceClasses());
    }

    @Override
    public Collection<Module> createGuiceModules() {
        return Lists2.concat(pluginLoader.createGuiceModules(), sqlPlugin.createGuiceModules());
    }

    @Override
    public Map<String, Mapper.TypeParser> getMappers() {
        return sqlPlugin.getMappers();
    }

    @Override
    public Collection<AllocationDecider> createAllocationDeciders(Settings settings, ClusterSettings clusterSettings) {
        return sqlPlugin.createAllocationDeciders(settings, clusterSettings);
    }

    @Override
    public void onIndexModule(IndexModule indexModule) {
        sqlPlugin.onIndexModule(indexModule);
    }

    @Override
    public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return sqlPlugin.getNamedWriteables();
    }

    @Override
    public List<NamedXContentRegistry.Entry> getNamedXContent() {
        return sqlPlugin.getNamedXContent();
    }

    @Override
    public UnaryOperator<IndexMetadata> getIndexMetadataUpgrader() {
        return sqlPlugin.getIndexMetadataUpgrader();
    }

    @Override
    public UnaryOperator<Map<String, IndexTemplateMetadata>> getIndexTemplateMetadataUpgrader() {
        return sqlPlugin.getIndexTemplateMetadataUpgrader();
    }

}
