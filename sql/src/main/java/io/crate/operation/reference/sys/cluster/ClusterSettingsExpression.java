/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.operation.reference.sys.cluster;

import io.crate.metadata.SimpleObjectExpression;
import io.crate.metadata.settings.CrateSettings;
import io.crate.metadata.settings.Setting;
import io.crate.operation.reference.NestedObjectExpression;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.settings.NodeSettingsService;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;


public class ClusterSettingsExpression extends NestedObjectExpression {

    public static final String NAME = "settings";

    static class SettingExpression extends SimpleObjectExpression<Object> {
        private final Map<String, Object> values;
        private final String name;

        protected SettingExpression(Setting<?, ?> setting, Map<String, Object> values) {
            this.name = setting.settingName();
            this.values = values;
        }

        @Override
        public Object value() {
            return this.values.get(name);
        }

    }

    static class NestedSettingExpression extends NestedObjectExpression {

        private final Map<String, Object> values;

        protected NestedSettingExpression(Setting<?, ?> setting, Map<String, Object> values) {
            this.values = values;
            addChildImplementations(setting.children());
        }

        public void addChildImplementations(List<Setting> childSettings) {
            for (Setting childSetting : childSettings) {
                if (childSetting.children().isEmpty()) {
                    childImplementations.put(childSetting.name(),
                            new SettingExpression(childSetting, values)
                    );
                } else {
                    childImplementations.put(childSetting.name(),
                            new NestedSettingExpression(childSetting, values)
                    );
                }
            }
        }
    }

    static class ApplySettings implements NodeSettingsService.Listener {

        private final ConcurrentMap<String, Object> values;
        private final Settings initialSettings;
        protected final ESLogger logger;

        ApplySettings(Settings initialSettings, ConcurrentMap<String, Object> values) {
            this.logger = Loggers.getLogger(getClass());
            this.values = values;
            this.initialSettings = initialSettings;
        }

        @Override
        public void onRefreshSettings(Settings settings) {
            applySettings(CrateSettings.CRATE_SETTINGS,
                    ImmutableSettings.builder()
                            .put(initialSettings)
                            .put(settings).build()
            );
        }

        /**
         * if setting is not available in new settings
         * and not in initialSettings, reset to default
         */
        private void applySettings(List<Setting> clusterSettings, Settings newSettings) {
            for (Setting<?, ?> setting : clusterSettings) {

                String name = setting.settingName();
                Object newValue = setting.extract(newSettings);
                if (newSettings.get(name) == null) {
                    applySettings(setting.children(), newSettings);
                }
                if (!newValue.equals(values.get(name))) {
                    if (newSettings.get(name) != null) {
                        logger.info("updating [{}] from [{}] to [{}]", name, values.get(name), newValue);
                    }
                    values.put(name, newValue);
                }
            }
        }
    }


    private final ConcurrentHashMap<String, Object> values = new ConcurrentHashMap<>();

    @Inject
    public ClusterSettingsExpression(Settings settings, NodeSettingsService nodeSettingsService) {
        applyDefaults(CrateSettings.CRATE_SETTINGS);
        ApplySettings applySettings = new ApplySettings(settings, values);

        nodeSettingsService.addListener(applySettings);
        addChildImplementations();
    }

    private void applyDefaults(List<Setting> settings) {
        for (Setting<?, ?> setting : settings) {
            String settingName = setting.settingName();
            values.put(settingName, setting.defaultValue());
            applyDefaults(setting.children());
        }
    }

    private void addChildImplementations() {
        childImplementations.put(
                CrateSettings.STATS.name(),
                new NestedSettingExpression(CrateSettings.STATS, values));
        childImplementations.put(
                CrateSettings.CLUSTER.name(),
                new NestedSettingExpression(CrateSettings.CLUSTER, values));
        childImplementations.put(
                CrateSettings.DISCOVERY.name(),
                new NestedSettingExpression(CrateSettings.DISCOVERY, values));
        childImplementations.put(
                CrateSettings.INDICES.name(),
                new NestedSettingExpression(CrateSettings.INDICES, values));
        childImplementations.put(
                CrateSettings.BULK.name(),
                new NestedSettingExpression(CrateSettings.BULK, values));
    }
}
