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

import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_SETTING_PREFIX;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Predicate;

import javax.annotation.Nullable;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;

import io.crate.sql.tree.GenericProperties;

public class GenericPropertiesConverter {

    private static final String INVALID_SETTING_MESSAGE = "setting '%s' not supported";

    public static void genericPropertyToSetting(Settings.Builder builder,
                                                String name,
                                                Object value) {
        if (value instanceof String[]) {
            builder.putList(name, (String[]) value);
        } else if (value instanceof List) {
            builder.putList(name, (List) value);
        } else {
            builder.put(name, value.toString());
        }
    }

    public static Settings genericPropertiesToSettings(GenericProperties<Object> genericProperties,
                                                       Map<String, Setting<?>> supportedSettings) {
        Settings.Builder builder = Settings.builder();
        genericPropertiesToSettings(
            builder,
            genericProperties,
            (settingKey) -> {
                if (!supportedSettings.containsKey(settingKey)) {
                    throw new IllegalArgumentException(
                        String.format(Locale.ENGLISH, INVALID_SETTING_MESSAGE, settingKey));
                }
            });
        return builder.build();
    }

    public static Settings genericPropertiesToSettings(GenericProperties<Object> genericProperties) {
        Settings.Builder builder = Settings.builder();
        genericPropertiesToSettings(builder, genericProperties, (settingKey) -> {
        });
        return builder.build();
    }

    private static void genericPropertiesToSettings(Settings.Builder builder,
                                                    GenericProperties<Object> genericProperties,
                                                    Consumer<String> settingKeyValidator) {
        for (Map.Entry<String, Object> entry : genericProperties.properties().entrySet()) {
            settingKeyValidator.accept(entry.getKey());
            builder.put(entry.getKey(), entry.getValue().toString());
        }
    }

    static void settingsFromProperties(Settings.Builder builder,
                                       GenericProperties<Object> properties,
                                       Map<String, Setting<?>> supportedSettings,
                                       boolean setDefaults,
                                       Predicate<String> ignoreProperty,
                                       String invalidMessage) {
        if (setDefaults) {
            setDefaults(builder, supportedSettings);
        }
        for (Map.Entry<String, Object> entry : properties.properties().entrySet()) {
            String settingName = entry.getKey();
            if (ignoreProperty.test(settingName)) {
                continue;
            }
            String groupName = getPossibleGroup(settingName);
            if (groupName != null && ignoreProperty.test(groupName)) {
                continue;
            }
            SettingHolder settingHolder = getSupportedSetting(supportedSettings, settingName);
            if (settingHolder == null) {
                throw new IllegalArgumentException(String.format(Locale.ENGLISH, invalidMessage, entry.getKey()));
            }
            settingHolder.apply(builder, entry.getValue());
        }
    }

    @SuppressWarnings("SameParameterValue")
    static void resetSettingsFromProperties(Settings.Builder builder,
                                            List<String> properties,
                                            Map<String, Setting<?>> supportedSettings,
                                            Predicate<String> ignoreProperty,
                                            String invalidMessage) {
        for (String name : properties) {
            if (ignoreProperty.test(name)) {
                continue;
            }
            String groupName = getPossibleGroup(name);
            if (groupName != null && ignoreProperty.test(groupName)) {
                continue;
            }
            SettingHolder settingHolder = getSupportedSetting(supportedSettings, name);
            if (settingHolder == null) {
                throw new IllegalArgumentException(String.format(Locale.ENGLISH, invalidMessage, name));
            }
            settingHolder.reset(builder);
        }
    }

    private static void setDefaults(Settings.Builder builder, Map<String, Setting<?>> supportedSettings) {
        for (Map.Entry<String, Setting<?>> entry : supportedSettings.entrySet()) {
            SettingHolder settingHolder = new SettingHolder(entry.getValue());
            // We'd set the "wrong" default for settings that base their default on other settings
            if (TableParameters.SETTINGS_NOT_INCLUDED_IN_DEFAULT.contains(settingHolder.setting)) {
                continue;
            }
            settingHolder.applyDefault(builder);
        }
    }

    @Nullable
    private static SettingHolder getSupportedSetting(Map<String, Setting<?>> supportedSettings,
                                                     String settingName) {
        Setting<?> setting = supportedSettings.get(settingName);
        if (setting == null) {
            String groupKey = getPossibleGroup(settingName);
            if (groupKey != null) {
                setting = supportedSettings.get(groupKey);
                if (setting instanceof Setting.AffixSetting) {
                    setting = ((Setting.AffixSetting) setting).getConcreteSetting(INDEX_SETTING_PREFIX + settingName);
                    return new SettingHolder(setting, true);
                }
            }
        }

        if (setting != null) {
            return new SettingHolder(setting);
        }
        return null;
    }

    @Nullable
    private static String getPossibleGroup(String key) {
        int idx = key.lastIndexOf('.');
        if (idx > 0) {
            return key.substring(0, idx);
        }
        return null;
    }

    private static class SettingHolder {

        private final Setting<?> setting;
        private final boolean isAffixSetting;
        private final boolean isChildOfAffixSetting;

        SettingHolder(Setting<?> setting) {
            this(setting, false);
        }

        SettingHolder(Setting<?> setting, boolean isChildOfAffixSetting) {
            this.setting = setting;
            this.isAffixSetting = setting instanceof Setting.AffixSetting;
            this.isChildOfAffixSetting = isChildOfAffixSetting;
        }

        void applyDefault(Settings.Builder builder) {
            if (isAffixSetting) {
                // affix settings are user defined, they have no default value
                return;
            }
            Object value = setting.getDefault(Settings.EMPTY);
            if (value instanceof Settings) {
                builder.put((Settings) value);
            } else {
                builder.put(setting.getKey(), value.toString());
            }
        }

        void apply(Settings.Builder builder, Object valueSymbol) {
            if (isAffixSetting) {
                // only concrete affix settings are supported otherwise it's not possible to parse values
                throw new IllegalArgumentException(
                    "Cannot change a dynamic group setting, only concrete settings allowed.");
            }
            Settings.Builder singleSettingBuilder = Settings.builder().put(builder.build());
            genericPropertyToSetting(singleSettingBuilder, setting.getKey(), valueSymbol);
            Object value = setting.get(singleSettingBuilder.build());
            if (value instanceof Settings) {
                builder.put((Settings) value);
            } else {
                builder.put(setting.getKey(), value.toString());
            }

        }

        void reset(Settings.Builder builder) {
            if (isAffixSetting) {
                // only concrete affix settings are supported, ES does not support to reset a whole Affix setting group
                throw new IllegalArgumentException(
                    "Cannot change a dynamic group setting, only concrete settings allowed.");
            }
            Object value = setting.getDefault(Settings.EMPTY);
            if (isChildOfAffixSetting) {
                // affix settings should be removed on reset, they don't have a default value
                builder.putNull(setting.getKey());
            } else if (value instanceof Settings) {
                builder.put((Settings) value);
            } else {
                builder.put(setting.getKey(), value.toString());
            }
        }
    }
}
