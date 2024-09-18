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

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;

import io.crate.common.collections.Lists;
import io.crate.types.DataTypes;

public class AzureBlobStorageSettings {

    private static List<String> SUPPORTED_PROTOCOLS = List.of("https", "http");

    public static final Setting<String> KEY_SETTING = Setting.simpleString("key", Property.NodeScope);
    public static final Setting<String> SAS_TOKEN_SETTING = Setting.simpleString("sas_token");
    public static final Setting<String> PROTOCOL_SETTING = new Setting<>(
        "protocol",
        "https",
        s -> {
            if (SUPPORTED_PROTOCOLS.contains(s) == false) {
                throw new IllegalArgumentException(
                    String.format(Locale.ENGLISH,"Invalid protocol `%s`. Expected HTTP or HTTPS", s)
                );
            }
            return s;

        },
        DataTypes.STRING);

    public static final List<Setting<String>> SUPPORTED_SETTINGS = List.of(
        KEY_SETTING,
        SAS_TOKEN_SETTING,
        PROTOCOL_SETTING
    );

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
        if (settings.get(SAS_TOKEN_SETTING.getKey()) == null && settings.get(KEY_SETTING.getKey()) == null) {
            throw new IllegalArgumentException("Authentication setting must be provided: either sas_token or key");
        }
    }


}
