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

import static io.crate.analyze.CopyStatementSettings.COMMON_COPY_FROM_SETTINGS;
import static io.crate.analyze.CopyStatementSettings.COMMON_COPY_TO_SETTINGS;

import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;

import io.crate.common.collections.Lists;

/**
 * Reflects settings from https://docs.rs/opendal/latest/opendal/services/struct.Azblob.html
 */
public class AzureBlobStorageSettings {

    // All operations will happen under this directory (user provided path will be prepended by root).
    static final Setting<String> BASE_PATH_SETTING = Setting.simpleString("base_path", Property.NodeScope);

    static final Setting<String> ACCOUNT_SETTING = Setting.simpleString("account", Property.NodeScope);
    static final Setting<String> KEY_SETTING = Setting.simpleString("key", Property.NodeScope);
    static final Setting<String> SAS_TOKEN_SETTING = Setting.simpleString("sas_token");

    static final Setting<String> CONTAINER_SETTING = Setting.simpleString("container", Property.NodeScope);
    static final Setting<String> ENDPOINT_SETTING = Setting.simpleString("endpoint", Property.NodeScope);

    static final List<Setting<String>> SUPPORTED_SETTINGS = List.of(
            CONTAINER_SETTING,
            ENDPOINT_SETTING,
            ACCOUNT_SETTING,
            KEY_SETTING,
            SAS_TOKEN_SETTING,
            BASE_PATH_SETTING
    );

    static final List<String> REQUIRED_SETTINGS = List.of(
            CONTAINER_SETTING.getKey(),
            ENDPOINT_SETTING.getKey()
    );

    /**
     * @param settings represents WITH clause parameters in COPY... operation.
     * @param read is 'true' for COPY FROM and 'false' for COPY TO.
     */
    public static void validate(Settings settings, boolean read) {
        List<String> validSettings = Lists.concat(
                SUPPORTED_SETTINGS.stream().map(Setting::getKey).toList(),
                read ? COMMON_COPY_FROM_SETTINGS : COMMON_COPY_TO_SETTINGS
        );
        for (String key : settings.keySet()) {
            if (validSettings.contains(key) == false) {
                throw new IllegalArgumentException("Setting '" + key + "' is not supported");
            }
        }

        for (String required: REQUIRED_SETTINGS) {
            if (settings.get(required) == null) {
                throw new IllegalArgumentException(
                    String.format(Locale.ENGLISH, "Setting '%s' must be provided", required)
                );
            }
        }

        if (settings.get(SAS_TOKEN_SETTING.getKey()) == null) {
            if (settings.get(ACCOUNT_SETTING.getKey()) == null && settings.get(KEY_SETTING.getKey()) == null) {
                throw new IllegalArgumentException("Authentication setting must be provided: either sas_token or account and key");
            }
        }
    }

    /**
     * Mapping of the user provided setting names,
     * which are aligned with 'CREATE REPOSITORY ... TYPE AZURE',
     * to OpenDAL supported names, listed in https://docs.rs/opendal/latest/opendal/services/struct.Azblob.html.
     */
    public static Map<String, String> AZURE_TO_OPEN_DAL = Map.of(
        ACCOUNT_SETTING.getKey(), "account_name",
        KEY_SETTING.getKey(), "account_key",
        BASE_PATH_SETTING.getKey(), "root",
        // Settings below have same names, listed for documentation purpose.
        CONTAINER_SETTING.getKey(), "container",
        ENDPOINT_SETTING.getKey(), "endpoint",
        SAS_TOKEN_SETTING.getKey(), "sas_token"
    );
}
