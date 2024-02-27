/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package io.crate.gcs;

import java.net.URI;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;

import com.google.auth.oauth2.ServiceAccountCredentials;

import io.crate.common.unit.TimeValue;
import io.crate.exceptions.InvalidArgumentException;

/**
 * Based on https://github.com/opensearch-project/OpenSearch/blob/main/plugins/repository-gcs/src/main/java/org/opensearch/repositories/gcs/GoogleCloudStorageClientSettings.java
 */
public record GoogleCloudStorageClientSettings(
    ServiceAccountCredentials credentials,
    String endpoint,
    String projectId,
    TimeValue connectTimeout,
    TimeValue readTimeout,
    URI tokenUri
) {

    static final Setting<SecureString> PRIVATE_KEY_ID_SETTING = Setting.maskedString("private_key_id");

    static final Setting<SecureString> PRIVATE_KEY_SETTING = Setting.maskedString("private_key");

    static final Setting<SecureString> CLIENT_EMAIL_SETTING = Setting.maskedString("client_email");

    static final Setting<SecureString> CLIENT_ID_SETTING = Setting.maskedString("client_id");

    static final Setting<String> ENDPOINT_SETTING = Setting.simpleString("endpoint", Setting.Property.NodeScope);

    static final Setting<String> PROJECT_ID_SETTING = Setting.simpleString("project_id", Setting.Property.NodeScope);

    static final Setting<String> TOKEN_URI_SETTING = Setting.simpleString(
        "token_uri", "https://oauth2.googleapis.com/token", Setting.Property.NodeScope);

    /** The timeout to establish a connection. The default value is 0 which uses the Google Cloud Storage standard value
     * of 20 seconds */
    static final Setting<TimeValue> CONNECT_TIMEOUT_SETTING = Setting.timeSetting(
        "connect_timeout", TimeValue.ZERO, TimeValue.MINUS_ONE, Setting.Property.NodeScope);

    /** The timeout to read data from an established connection. The default value is 0 which uses the Google Cloud
     * Storage standard value of 20 seconds */
    static final Setting<TimeValue> READ_TIMEOUT_SETTING = Setting.timeSetting(
        "read_timeout", TimeValue.ZERO, TimeValue.MINUS_ONE, Setting.Property.NodeScope);


    static GoogleCloudStorageClientSettings fromSettings(final Settings settings) {
        return new GoogleCloudStorageClientSettings(
            loadCredentials(settings),
            ENDPOINT_SETTING.get(settings),
            PROJECT_ID_SETTING.get(settings),
            CONNECT_TIMEOUT_SETTING.get(settings),
            READ_TIMEOUT_SETTING.get(settings),
            tokenUri(settings)
        );
    }

    static ServiceAccountCredentials loadCredentials(Settings settings) {
        try {
            return ServiceAccountCredentials
                .newBuilder()
                .setClientId(CLIENT_ID_SETTING.get(settings).toString())
                .setClientEmail(CLIENT_EMAIL_SETTING.get(settings).toString())
                .setPrivateKeyId(PRIVATE_KEY_ID_SETTING.get(settings).toString())
                .setPrivateKeyString(privateKey(settings))
                .setTokenServerUri(tokenUri(settings))
                .setProjectId(PROJECT_ID_SETTING.get(settings))
                .build();
        } catch (Exception e) {
            throw new InvalidArgumentException(e.getMessage());
        }
    }

    private static URI tokenUri(Settings settings) {
        return URI.create(TOKEN_URI_SETTING.get(settings));
    }

    private static String privateKey(Settings settings) {
        SecureString secureString = PRIVATE_KEY_SETTING.get(settings);
        return secureString.toString().replaceAll("\\\\n", "\n");
    }
}
