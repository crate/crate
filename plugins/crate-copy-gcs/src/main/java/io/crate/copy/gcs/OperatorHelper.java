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

import static io.crate.copy.gcs.GCSStorageSettings.ENDPOINT_SETTING;
import static io.crate.copy.gcs.GCSStorageSettings.validate;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.json.JsonXContent;

/**
 * Helper class to build OpenDAL config for the GCS backend.
 */
public class OperatorHelper {

    /**
     * @param settings represents WITH clause parameters in COPY... operation.
     * @param read is 'true' for COPY FROM and 'false' for COPY TO.
     */
    public static Map<String, String> config(GCSURI gcsURI, Settings settings, boolean read) {
        validate(settings, read);

        Map<String, String> config = new HashMap<>();

        String credentials;
        try (var builder = JsonXContent.builder()
                .startObject()
                .field("type", "service_account")
                .field("project_id", GCSStorageSettings.PROJECT_ID_SETTING.get(settings))
                .field("private_key_id", GCSStorageSettings.PRIVATE_KEY_ID_SETTING.get(settings))
                .field("private_key", GCSStorageSettings.privateKey(settings))
                .field("client_id", GCSStorageSettings.CLIENT_ID_SETTING.get(settings))
                .field("client_email", GCSStorageSettings.CLIENT_EMAIL_SETTING.get(settings))
                .field("auth_uri", "https://accounts.google.com/o/oauth2/auth")
                .field("token_uri", "https://oauth2.googleapis.com/token")
                .field("auth_provider_x509_cert_url", "https://www.googleapis.com/oauth2/v1/certs")
                .endObject()) {
            credentials = Strings.toString(builder);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        config.put("credential",
            Base64.getEncoder().encodeToString(credentials.getBytes(StandardCharsets.UTF_8)));

        config.put("bucket", gcsURI.bucket());

        String endpoint = settings.get(ENDPOINT_SETTING.getKey());
        if (endpoint != null) {
            config.put("endpoint", endpoint);
        }

        config.put("allow_anonymous", "true");
        if (endpoint != null && !endpoint.contains("storage.googleapis.com")) {
            config.put("disable_vm_metadata", "true");
            config.put("disable_config_load", "true");
        }

        return config;
    }
}
