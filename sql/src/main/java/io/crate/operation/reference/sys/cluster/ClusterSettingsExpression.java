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
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


public class ClusterSettingsExpression extends NestedObjectExpression {

    public static final String NAME = "settings";
    private final static Logger LOGGER = Loggers.getLogger(ClusterSettingsExpression.class);

    static class SettingExpression implements ReferenceImplementation<Object> {
        private final Map<String, Object> values;
        private final String name;
        private final DataType dataType;

        SettingExpression(Setting<?, ?> setting, Map<String, Object> values) {
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

        NestedSettingExpression(Setting<?, ?> setting, Map<String, Object> values) {
            this.values = values;
            addChildImplementations(setting.children());
        }

        private void addChildImplementations(List<Setting> childSettings) {
            for (Setting childSetting : childSettings) {
                if (childSetting.children().isEmpty()) {
                    childImplementations.put(childSetting.name(), new SettingExpression(childSetting, values));
                } else {
                    childImplementations.put(childSetting.name(), new NestedSettingExpression(childSetting, values));
                }
            }
        }
    }

    private final ConcurrentHashMap<String, Object> values = new ConcurrentHashMap<>();
    private final ClusterSettings clusterSettings;
    private final ClusterService clusterService;

    @Inject
    public ClusterSettingsExpression(Settings settings, ClusterService clusterService) {
        this.clusterSettings = clusterService.getClusterSettings();
        this.clusterService = clusterService;
        setDefaultValues(CrateSettings.SETTINGS);
        applyInitialSettingsAndRegisterUpdateConsumer(CrateSettings.SETTINGS, settings);
        addChildImplementations();
    }

    private void setDefaultValues(List<Setting> settings) {
        for (Setting<?, ?> setting : settings) {
            String settingName = setting.settingName();
            values.put(settingName, setting.defaultValue());
            setDefaultValues(setting.children());
        }
    }

    private void applyInitialSettingsAndRegisterUpdateConsumer(List<Setting> crateSettings, Settings settings) {
        for (Setting<?, ?> setting : crateSettings) {
            String name = setting.settingName();
            Object initialSetting = setting.extract(settings);
            if (initialSetting != null) {
                values.put(name, initialSetting);
            }
            org.elasticsearch.common.settings.Setting<?> esSetting = setting.esSetting();
            if (esSetting == null) { // = NestedSetting (= container for other settings)
                applyInitialSettingsAndRegisterUpdateConsumer(setting.children(), settings);
            } else if (setting.isRuntime()) {
                try {
                    clusterSettings.addSettingsUpdateConsumer(esSetting, v -> values.put(name, v));
                } catch (IllegalArgumentException e) {
                    if (clusterSettings.get(esSetting.getKey()) != null) {
                        // Setting exists, but identity comparison differs, probably an already registered ES setting
                        LOGGER.debug(String.format(Locale.ENGLISH,
                            "An update consumer for setting key [%s] is already registered, ignoring..",
                            esSetting.getKey()));
                    } else {
                        LOGGER.error(e);
                    }
                }
            }
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
            ClusterLoggingOverridesExpression.NAME,
            new ClusterLoggingOverridesExpression(clusterService));
    }
}
