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

package org.elasticsearch.common.settings;

import org.elasticsearch.common.inject.Binder;
import org.elasticsearch.common.inject.Module;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A module that binds the provided settings to the {@link Settings} interface.
 */
public class SettingsModule implements Module {

    private final Settings settings;
    private final Map<String, Setting<?>> nodeSettings = new HashMap<>();
    private final Map<String, Setting<?>> indexSettings = new HashMap<>();
    private final IndexScopedSettings indexScopedSettings;
    private final ClusterSettings clusterSettings;

    public SettingsModule(Settings settings, Setting<?>... additionalSettings) {
        this(settings, Arrays.asList(additionalSettings), Collections.emptySet());
    }

    public SettingsModule(
            Settings settings,
            List<Setting<?>> additionalSettings,
            Set<SettingUpgrader<?>> settingUpgraders) {
        this.settings = settings;
        ArrayList<Setting<?>> maskedSettings = new ArrayList<>();
        for (Setting<?> setting : ClusterSettings.BUILT_IN_CLUSTER_SETTINGS) {
            registerSetting(setting);
            if (setting.isMasked()) {
                maskedSettings.add(setting);
            }
        }
        for (Setting<?> setting : IndexScopedSettings.BUILT_IN_INDEX_SETTINGS) {
            registerSetting(setting);
        }
        for (Setting<?> setting : additionalSettings) {
            registerSetting(setting);
            if (setting.isMasked()) {
                maskedSettings.add(setting);
            }
        }

        final Set<SettingUpgrader<?>> clusterSettingUpgraders = new HashSet<>();
        for (final SettingUpgrader<?> settingUpgrader : settingUpgraders) {
            assert settingUpgrader.getSetting().hasNodeScope() : settingUpgrader.getSetting().getKey();
            final boolean added = clusterSettingUpgraders.add(settingUpgrader);
            assert added : settingUpgrader.getSetting().getKey();
        }
        this.indexScopedSettings = new IndexScopedSettings(settings, new HashSet<>(this.indexSettings.values()));
        this.clusterSettings = new ClusterSettings(settings,
                                                   maskedSettings,
                                                   new HashSet<>(this.nodeSettings.values()),
                                                   clusterSettingUpgraders);
        Settings indexSettings = settings.filter((s) -> s.startsWith("index.") && clusterSettings.get(s) == null);
        if (indexSettings.isEmpty() == false) {
            throw new IllegalArgumentException(
                "Index settings found. These have been unsupported since CrateDB 2.0 and should've been migrated.");
        }
        // by now we are fully configured, lets check node level settings for unregistered index settings
        clusterSettings.validate(settings, true);
    }

    @Override
    public void configure(Binder binder) {
        binder.bind(Settings.class).toInstance(settings);
        binder.bind(ClusterSettings.class).toInstance(clusterSettings);
        binder.bind(IndexScopedSettings.class).toInstance(indexScopedSettings);
    }


    /**
     * Registers a new setting. This method should be used by plugins in order to expose any custom settings the plugin defines.
     * Unless a setting is registered the setting is unusable. If a setting is never the less specified the node will reject
     * the setting during startup.
     */
    private void registerSetting(Setting<?> setting) {
        if (setting.hasNodeScope() || setting.hasIndexScope()) {
            if (setting.hasNodeScope()) {
                Setting<?> existingSetting = nodeSettings.get(setting.getKey());
                if (existingSetting != null) {
                    throw new IllegalArgumentException("Cannot register setting [" + setting.getKey() + "] twice");
                }
                nodeSettings.put(setting.getKey(), setting);
            }
            if (setting.hasIndexScope()) {
                Setting<?> existingSetting = indexSettings.get(setting.getKey());
                if (existingSetting != null) {
                    throw new IllegalArgumentException("Cannot register setting [" + setting.getKey() + "] twice");
                }
                indexSettings.put(setting.getKey(), setting);
            }
        } else {
            throw new IllegalArgumentException("No scope found for setting [" + setting.getKey() + "]");
        }
    }

    public Settings getSettings() {
        return settings;
    }

    public IndexScopedSettings getIndexScopedSettings() {
        return indexScopedSettings;
    }

    public ClusterSettings getClusterSettings() {
        return clusterSettings;
    }
}
