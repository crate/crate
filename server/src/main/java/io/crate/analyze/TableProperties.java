/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.analyze;

import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.jetbrains.annotations.Nullable;

import io.crate.metadata.settings.NumberOfReplicas;
import io.crate.sql.tree.ColumnPolicy;
import io.crate.sql.tree.GenericProperties;

public final class TableProperties {

    private static final String INVALID_MESSAGE = "Invalid property \"%s\" passed to [ALTER | CREATE] TABLE statement";

    private TableProperties() {}

    public static void analyze(Settings.Builder settingsBuilder,
                               TableParameters tableParameters,
                               GenericProperties<Object> properties) {
        Map<String, Setting<?>> settingMap = tableParameters.supportedSettings();
        settingsFromProperties(
            settingsBuilder,
            properties,
            settingMap);
    }

    private static void settingsFromProperties(Settings.Builder builder,
                                               GenericProperties<Object> properties,
                                               Map<String, Setting<?>> supportedSettings) {
        for (Map.Entry<String, Object> entry : properties) {
            String settingName = entry.getKey();
            Setting<?> setting = getSupportedSetting(supportedSettings, settingName);
            if (setting == null) {
                throw new IllegalArgumentException(String.format(Locale.ENGLISH, INVALID_MESSAGE, entry.getKey()));
            }
            Object value = entry.getValue();
            if (value == null) {
                throw new IllegalArgumentException(
                    String.format(
                        Locale.ENGLISH,
                        "Cannot set NULL to property %s.",
                        entry.getKey()
                    )
                );
            }
            apply(builder, setting, entry.getValue());
        }
    }

    /**
     * Processes the property names which should be reset and updates the settings or mappings with the related
     * default value.
     */
    public static void analyzeResetProperties(Settings.Builder settingsBuilder,
                                              TableParameters tableParameters,
                                              List<String> properties) {
        Map<String, Setting<?>> supportedSettings = tableParameters.supportedSettings();
        for (String name : properties) {
            Setting<?> setting = getSupportedSetting(supportedSettings, name);
            if (setting == null) {
                throw new IllegalArgumentException(String.format(Locale.ENGLISH, INVALID_MESSAGE, name));
            }
            reset(settingsBuilder, setting);
        }
    }

    static void apply(Settings.Builder builder, Setting<?> setting, Object value) {
        if (setting instanceof Setting.AffixSetting<?>) {
            // only concrete affix settings are supported otherwise it's not possible to parse values
            throw new IllegalArgumentException(
                "Cannot change a dynamic group setting, only concrete settings allowed.");
        }
        // Need to go through `setting.get` to apply the setting's parse/validation logic
        // E.g. A `number_of_replicas` value of `0-all` becomes a `Settings` value with
        //  index.auto_expand_replicas: 0-1
        //  index.number_of_replicas: 0-all
        Object resolvedValue = setting.get(Settings.builder()
            .put(builder.build())
            .put(setting.getKey(), value)
            .build()
        );
        if (resolvedValue instanceof Settings settings) {
            builder.put(settings);
        } else {
            builder.putStringOrList(setting.getKey(), resolvedValue);
        }
    }

    static void reset(Settings.Builder builder, Setting<?> setting) {
        if (setting instanceof Setting.AffixSetting<?>) {
            // only concrete affix settings are supported, ES does not support to reset a whole Affix setting group
            throw new IllegalArgumentException(
                "Cannot change a dynamic group setting, only concrete settings allowed.");
        }
        builder.putNull(setting.getKey());
        if (setting instanceof NumberOfReplicas numberOfReplicas) {
            builder.put(numberOfReplicas.getDefault(Settings.EMPTY));
        } else if (setting.equals(TableParameters.COLUMN_POLICY)) {
            builder.put(setting.getKey(), ColumnPolicy.STRICT);
        }
    }

    @Nullable
    private static Setting<?> getSupportedSetting(Map<String, Setting<?>> supportedSettings, String settingName) {
        Setting<?> setting = supportedSettings.get(settingName);
        if (setting == null) {
            String groupKey = getPossibleGroup(settingName);
            if (groupKey != null) {
                setting = supportedSettings.get(groupKey);
                if (setting instanceof Setting.AffixSetting<?> affixSetting) {
                    setting = affixSetting.getConcreteSetting(IndexMetadata.INDEX_SETTING_PREFIX + settingName);
                    return setting;
                }
            }
        }
        return setting;
    }

    @Nullable
    private static String getPossibleGroup(String key) {
        int idx = key.lastIndexOf('.');
        if (idx > 0) {
            return key.substring(0, idx);
        }
        return null;
    }
}
