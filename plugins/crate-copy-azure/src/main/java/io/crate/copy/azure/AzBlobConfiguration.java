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
import static io.crate.copy.azure.AzureBlobStorageSettings.SUPPORTED_SETTINGS;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;

import io.crate.copy.Configuration;

public class AzBlobConfiguration implements Configuration<AzureURI> {

    @Override
    public List<Setting<String>> supportedSettings() {
        return SUPPORTED_SETTINGS;
    }

    @Override
    public Map<String, String> fromURIAndSettings(AzureURI azureURI, Settings settings) {
        // Scheme specific validation. Common validation is already done at this point.
        var key = settings.get(KEY_SETTING.getKey());
        var token = settings.get(SAS_TOKEN_SETTING.getKey());
        if (key == null && token == null) {
            throw new IllegalArgumentException("Authentication setting must be provided: either sas_token or key");
        }
        String endpoint = String.format(
            Locale.ENGLISH,
            "%s://%s",
            PROTOCOL_SETTING.get(settings),
            azureURI.endpoint()
        );

        Map<String, String> config = new HashMap<>();
        config.put("account_name", azureURI.account());
        config.put("container", azureURI.container());
        config.put("endpoint", endpoint);

        if (key != null) {
            config.put("account_key", key);
        } else {
            // Validation passed, SAS token is not null.
            config.put("sas_token", token);
        }
        return config;
    }
}
