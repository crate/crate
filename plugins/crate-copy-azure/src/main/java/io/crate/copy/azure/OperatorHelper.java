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

import static io.crate.copy.azure.AzureBlobStorageSettings.KEY_SETTING;
import static io.crate.copy.azure.AzureBlobStorageSettings.PROTOCOL_SETTING;
import static io.crate.copy.azure.AzureBlobStorageSettings.SAS_TOKEN_SETTING;
import static io.crate.copy.azure.AzureBlobStorageSettings.validate;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;

/**
 * Helper class to build OpenDAL config.
 */
public class OperatorHelper {

    /**
     * OpenDAL specific settings.
     * Not same with SUPPORTED_SETTINGS since protocol is not related and required only to construct endpoint.
     */
    public static final List<Setting<String>> SETTINGS_TO_REWRITE = List.of(
        KEY_SETTING,
        SAS_TOKEN_SETTING
    );

    /**
     * Mapping of the user provided setting names,
     * which are aligned with 'CREATE REPOSITORY ... TYPE AZURE',
     * to OpenDAL supported names, listed in https://docs.rs/opendal/latest/opendal/services/struct.Azblob.html.
     */
    private static Map<String, String> AZURE_TO_OPEN_DAL = Map.of(
        KEY_SETTING.getKey(), "account_key",
        SAS_TOKEN_SETTING.getKey(), "sas_token"
    );

    /**
     * @param settings represents WITH clause parameters in COPY... operation.
     */
    public static Map<String, String> config(AzureURI azureURI, Settings settings) {
        validate(settings);

        Map<String, String> config = new HashMap<>();
        for (Setting<String> setting : SETTINGS_TO_REWRITE) {
            var value = setting.get(settings);
            var key = setting.getKey();
            var mappedKey = AZURE_TO_OPEN_DAL.get(key);
            assert mappedKey != null : "All known settings must have their OpenDAL counterpart specified.";
            if (value != null) {
                config.put(mappedKey, value);
            }
        }
        String endpoint = String.format(
            Locale.ENGLISH,
            "%s://%s",
            PROTOCOL_SETTING.get(settings),
            azureURI.endpoint()
        );
        config.put("account_name", azureURI.account());
        config.put("container", azureURI.container());
        config.put("endpoint", endpoint);
        return config;
    }
}
