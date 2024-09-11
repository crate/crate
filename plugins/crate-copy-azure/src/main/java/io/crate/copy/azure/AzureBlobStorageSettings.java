/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.copy.azure;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;

/**
 * Reflects settings from https://docs.rs/opendal/latest/opendal/services/struct.Azblob.html
 */
public class AzureBlobStorageSettings {

    // All operations will happen under this directory (user provided path will be prepended by root).
    static final Setting<String> ROOT_SETTING = Setting.simpleString("root", Property.NodeScope);

    static final Setting<String> ACCOUNT_NAME_SETTING = Setting.simpleString("account_name", Property.NodeScope);
    static final Setting<String> ACCOUNT_KEY_SETTING = Setting.simpleString("account_key", Property.NodeScope);

    static final Setting<String> CONTAINER_SETTING = Setting.simpleString("container", Property.NodeScope);
    static final Setting<String> ENDPOINT_SETTING = Setting.simpleString("endpoint", Property.NodeScope);

    /**
     * Registry of all settings also containing information whether setting is required or not.
     */
    private static Map<Setting<String>, Boolean> supportedSettings() {
        return Map.of(
            CONTAINER_SETTING, true,
            ENDPOINT_SETTING, true,
            ACCOUNT_NAME_SETTING, true,
            ACCOUNT_KEY_SETTING, true,
            ROOT_SETTING, false
        );
    }

    /**
     * Creates OpenDAL config from user provided settings.
     * Rejects unknown settings.
     * @param commonCopySettings is either common COPY TO or common COPY FROM settings.
     */
    public static Map<String, String> openDALConfig(Settings settings, List<String> commonCopySettings) {
        rejectUnknownSettings(settings, commonCopySettings);

        Map<String, String> config = new HashMap<>();
        for (Map.Entry<Setting<String>, Boolean> entry: AzureBlobStorageSettings.supportedSettings().entrySet()) {
            var setting = entry.getKey();
            var required = entry.getValue();
            var value = setting.get(settings);
            if (value != null) {
                config.put(setting.getKey(), value);
            } else if (required) {
                throw new IllegalArgumentException(
                    String.format(Locale.ENGLISH, "Setting %s must be provided", setting.getKey())
                );
            }
        }
        return config;
    }

    private static void rejectUnknownSettings(Settings settings, List<String> commonCopySettings) {
        Set<String> azureCopySettings = supportedSettings().keySet().stream().map(Setting::getKey).collect(Collectors.toSet());
        for (String key : settings.keySet()) {
            if (commonCopySettings.contains(key) == false && azureCopySettings.contains(key) == false) {
                throw new IllegalArgumentException("Setting '" + key + "' is not supported");
            }
        }
    }
}
