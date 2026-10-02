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

package io.crate.copy.gcs;

import static io.crate.analyze.CopyStatementSettings.COMMON_COPY_FROM_SETTINGS;
import static io.crate.analyze.CopyStatementSettings.COMMON_COPY_TO_SETTINGS;

import java.util.List;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;

import io.crate.common.collections.Lists;

public class GCSStorageSettings {

    public static final Setting<String> PROJECT_ID_SETTING = Setting.simpleString("project_id");
    public static final Setting<String> PRIVATE_KEY_ID_SETTING = Setting.simpleString("private_key_id");
    public static final Setting<String> PRIVATE_KEY_SETTING = Setting.simpleString("private_key");
    public static final Setting<String> CLIENT_EMAIL_SETTING = Setting.simpleString("client_email");
    public static final Setting<String> CLIENT_ID_SETTING = Setting.simpleString("client_id");
    public static final Setting<String> ENDPOINT_SETTING = Setting.simpleString("endpoint");

    private static final List<String> REQUIRED_CREDENTIAL_KEYS = List.of(
        "project_id", "private_key_id", "private_key", "client_email", "client_id"
    );

    public static final List<Setting<String>> SUPPORTED_SETTINGS = List.of(
        PROJECT_ID_SETTING,
        PRIVATE_KEY_ID_SETTING,
        PRIVATE_KEY_SETTING,
        CLIENT_EMAIL_SETTING,
        CLIENT_ID_SETTING,
        ENDPOINT_SETTING
    );

    static String privateKey(Settings settings) {
        return PRIVATE_KEY_SETTING.get(settings).replaceAll("\\\\n", "\n");
    }

    static void validate(Settings settings, boolean read) {
        List<String> validSettings = Lists.concat(
            SUPPORTED_SETTINGS.stream().map(Setting::getKey).toList(),
            read ? COMMON_COPY_FROM_SETTINGS : COMMON_COPY_TO_SETTINGS
        );
        for (String key : settings.keySet()) {
            if (validSettings.contains(key) == false) {
                throw new IllegalArgumentException("Setting '" + key + "' is not supported");
            }
        }
        List<String> missing = REQUIRED_CREDENTIAL_KEYS.stream()
            .filter(k -> settings.get(k) == null)
            .toList();
        if (missing.isEmpty() == false) {
            throw new IllegalArgumentException(
                "Required credential settings are missing: " + String.join(", ", missing));
        }
    }
}
