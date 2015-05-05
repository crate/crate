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

import io.crate.metadata.ColumnIdent;
import io.crate.metadata.settings.CrateSettings;
import io.crate.metadata.settings.Setting;
import io.crate.operation.reference.sys.SysClusterObjectReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.settings.NodeSettingsService;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


public class ClusterSettingsExpression extends SysClusterObjectReference {

    public static final String NAME = "settings";

    static class SettingExpression extends SysClusterExpression<Object> {
        private final Map<String, Object> values;
        private final String name;

        protected SettingExpression(Setting setting, Map<String, Object> values) {
            super(new ColumnIdent(NAME, setting.chain()));
            this.name = setting.settingName();
            this.values = values;
        }

        @Override
        public Object value() {
            return this.values.get(name);
        }

    }

    static class NestedSettingExpression extends SysClusterObjectReference {

        private final Map<String, Object> values;

        protected NestedSettingExpression(Setting setting, Map<String, Object> values) {
            super(new ColumnIdent(NAME, setting.chain()));
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

        private final Map values;
        protected final ESLogger logger;

        ApplySettings(Map values) {
            this.logger = Loggers.getLogger(getClass());
            this.values = values;
        }

        @Override
        public void onRefreshSettings(Settings settings) {
            applySettings(CrateSettings.CRATE_SETTINGS, settings);
        }

        private void applySettings(List<Setting> clusterSettings, Settings settings) {
            for (Setting setting : clusterSettings) {
                String name = setting.settingName();
                Object newValue = setting.extract(settings);
                if (settings.get(name) == null) {
                    applySettings((List<Setting>) setting.children(), settings);
                }
                if (!newValue.equals(values.get(name))) {
                    if (settings.get(name) != null) {
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
        super(NAME);
        nodeSettingsService.addListener(new ApplySettings(values));
        addChildImplementations();
        applySettings(CrateSettings.CRATE_SETTINGS);
    }

    private final void applySettings(List<Setting> settings) {
        for (Setting setting : settings) {
            String settingName = setting.settingName();
            values.put(settingName, setting.defaultValue());
            applySettings((List<Setting>) setting.children());
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
