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

import java.util.List;
import java.util.Locale;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;

import io.crate.types.DataTypes;

public class AzureBlobStorageSettings {

    private static final List<String> SUPPORTED_PROTOCOLS = List.of("https", "http");

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
}
