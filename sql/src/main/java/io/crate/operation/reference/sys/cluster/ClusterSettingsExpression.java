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

import io.crate.metadata.ReferenceImplementation;
import io.crate.metadata.settings.CrateSettings;
import io.crate.metadata.settings.Setting;
import io.crate.operation.reference.NestedObjectExpression;
import io.crate.types.DataType;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.settings.NodeSettingsService;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;


public class ClusterSettingsExpression extends NestedObjectExpression {

    public static final String NAME = "settings";

    static class SettingExpression implements ReferenceImplementation<Object> {
        private final Map<String, Object> values;
        private final String name;
        private final DataType dataType;

        protected SettingExpression(Setting<?, ?> setting, Map<String, Object> values) {
            this.name = setting.settingName();
            this.values = values;
            this.dataType = setting.dataType();
        }

        @Override
        public Object value() {
            return dataType.value(this.values.get(name));
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
            applySettings(CrateSettings.SETTINGS, initialSettings);
        }

        @Override
        public void onRefreshSettings(Settings settings) {
            applySettings(CrateSettings.SETTINGS,
                Settings.builder()
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
    private final ClusterService clusterService;

    @Inject
    public ClusterSettingsExpression(Settings settings, NodeSettingsService nodeSettingsService, ClusterService clusterService) {
        this.clusterService = clusterService;
        setDefaultValues(CrateSettings.SETTINGS);
        ApplySettings applySettings = new ApplySettings(settings, values);

        nodeSettingsService.addListener(applySettings);
        addChildImplementations();
    }

    private void setDefaultValues(List<Setting> settings) {
        for (Setting<?, ?> setting : settings) {
            String settingName = setting.settingName();
            values.put(settingName, setting.defaultValue());
            setDefaultValues(setting.children());
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
        childImplementations.put(
            CrateSettings.GATEWAY.name(),
            new NestedSettingExpression(CrateSettings.GATEWAY, values));
        childImplementations.put(
            CrateSettings.UDC.name(),
            new NestedSettingExpression(CrateSettings.UDC, values));
        childImplementations.put(
            CrateSettings.LICENSE.name(),
            new NestedSettingExpression(CrateSettings.LICENSE, values));
        childImplementations.put(
            CrateSettings.UDF.name(),
            new NestedSettingExpression(CrateSettings.UDF, values));
        childImplementations.put(
            ClusterLoggingOverridesExpression.NAME,
            new ClusterLoggingOverridesExpression(clusterService));
    }
}
