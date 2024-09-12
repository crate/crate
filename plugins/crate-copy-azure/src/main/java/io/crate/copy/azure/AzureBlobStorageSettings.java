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

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;

import io.crate.common.collections.Lists;

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

    static final List<Setting<String>> SUPPORTED_SETTINGS = List.of(
            CONTAINER_SETTING,
            ENDPOINT_SETTING,
            ACCOUNT_NAME_SETTING,
            ACCOUNT_KEY_SETTING,
            ROOT_SETTING
    );

    static final List<String> REQUIRED_SETTINGS = List.of(
            CONTAINER_SETTING.getKey(),
            ENDPOINT_SETTING.getKey(),
            ACCOUNT_NAME_SETTING.getKey(),
            ACCOUNT_KEY_SETTING.getKey()
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
    }
}
