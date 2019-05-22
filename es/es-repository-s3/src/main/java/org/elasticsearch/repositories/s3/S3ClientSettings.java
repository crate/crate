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

package org.elasticsearch.repositories.s3;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.BasicSessionCredentials;
import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.common.settings.SecureSetting;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;

import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * A container for settings used to create an S3 client.
 */
public final class S3ClientSettings {

    private static final String DEFAULT = "default";

    // prefix for s3 default client settings
    private static final String DEFAULT_PREFIX = "s3.client.default.";

    /** The access key (ie login id) for connecting to s3. */
    static final Setting<SecureString> ACCESS_KEY_SETTING = SecureSetting.insecureString(DEFAULT_PREFIX + "access_key");

    /** The secret key (ie password) for connecting to s3. */
    static final Setting<SecureString> SECRET_KEY_SETTING = SecureSetting.insecureString(DEFAULT_PREFIX + "secret_key");

    /** The secret key (ie password) for connecting to s3. */
    static final Setting<SecureString> SESSION_TOKEN_SETTING = SecureSetting.insecureString(
        DEFAULT_PREFIX + "session_token");

    /** An override for the s3 endpoint to connect to. */
    static final Setting<String> ENDPOINT_SETTING = Setting.simpleString(DEFAULT_PREFIX + "endpoint",
                                                                         s -> s.toLowerCase(Locale.ROOT),
                                                                         Property.NodeScope);

    /** The protocol to use to connect to s3. */
    static final Setting<Protocol> PROTOCOL_SETTING = new Setting<>(DEFAULT_PREFIX + "protocol",
                                                                    "https",
                                                                    s -> Protocol.valueOf(s.toUpperCase(Locale.ROOT)),
                                                                    Property.NodeScope);

    /** The host name of a proxy to connect to s3 through. */
    static final Setting<String> PROXY_HOST_SETTING = Setting.simpleString(DEFAULT_PREFIX + "proxy.host",
                                                                           Property.NodeScope);

    /** The port of a proxy to connect to s3 through. */
    static final Setting<Integer> PROXY_PORT_SETTING = Setting.intSetting(DEFAULT_PREFIX + "proxy.port",
                                                                          80,
                                                                          0,
                                                                          1<<16,
                                                                          Property.NodeScope);

    /** The username of a proxy to connect to s3 through. */
    static final Setting<SecureString> PROXY_USERNAME_SETTING = SecureSetting.insecureString(
        DEFAULT_PREFIX + "proxy.username");

    /** The password of a proxy to connect to s3 through. */
    static final Setting<SecureString> PROXY_PASSWORD_SETTING = SecureSetting.insecureString(
        DEFAULT_PREFIX + "proxy.password");

    /** The socket timeout for connecting to s3. */
    static final Setting<TimeValue> READ_TIMEOUT_SETTING = Setting.timeSetting(DEFAULT_PREFIX + "read_timeout",
                                                                               TimeValue.timeValueMillis(ClientConfiguration.DEFAULT_SOCKET_TIMEOUT),
                                                                               Property.NodeScope);

    /** The number of retries to use when an s3 request fails. */
    static final Setting<Integer> MAX_RETRIES_SETTING = Setting.intSetting(DEFAULT_PREFIX + "max_retries",
                                                                           ClientConfiguration.DEFAULT_RETRY_POLICY.getMaxErrorRetry(),
                                                                           0,
                                                                           Property.NodeScope);

    /** Whether retries should be throttled (ie use backoff). */
    static final Setting<Boolean> USE_THROTTLE_RETRIES_SETTING = Setting.boolSetting(DEFAULT_PREFIX + "use_throttle_retries",
                                                                                     ClientConfiguration.DEFAULT_THROTTLE_RETRIES,
                                                                                     Property.NodeScope);
    public static List<Setting<?>> optionalSettings() {
        return List.of(ENDPOINT_SETTING,
                       PROTOCOL_SETTING,
                       MAX_RETRIES_SETTING,
                       USE_THROTTLE_RETRIES_SETTING);
    }

    /** Credentials to authenticate with s3. */
    final AWSCredentials credentials;

    /** The s3 endpoint the client should talk to, or empty string to use the default. */
    final String endpoint;

    /** The protocol to use to talk to s3. Defaults to https. */
    final Protocol protocol;

    /** An optional proxy host that requests to s3 should be made through. */
    final String proxyHost;

    /** The port number the proxy host should be connected on. */
    final int proxyPort;

    // these should be "secure" yet the api for the s3 client only takes String, so storing them
    // as SecureString here won't really help with anything
    /** An optional username for the proxy host, for basic authentication. */
    final String proxyUsername;

    /** An optional password for the proxy host, for basic authentication. */
    final String proxyPassword;

    /** The read timeout for the s3 client. */
    final int readTimeoutMillis;

    /** The number of retries to use for the s3 client. */
    final int maxRetries;

    /** Whether the s3 client should use an exponential backoff retry policy. */
    final boolean throttleRetries;

    private S3ClientSettings(AWSCredentials credentials, String endpoint, Protocol protocol,
                             String proxyHost, int proxyPort, String proxyUsername, String proxyPassword,
                             int readTimeoutMillis, int maxRetries, boolean throttleRetries) {
        this.credentials = credentials;
        this.endpoint = endpoint;
        this.protocol = protocol;
        this.proxyHost = proxyHost;
        this.proxyPort = proxyPort;
        this.proxyUsername = proxyUsername;
        this.proxyPassword = proxyPassword;
        this.readTimeoutMillis = readTimeoutMillis;
        this.maxRetries = maxRetries;
        this.throttleRetries = throttleRetries;
    }

    /**
     * Load the default client settings from the given settings.
     *
     */
    static Map<String, S3ClientSettings> load(Settings settings) {
        return Map.of(DEFAULT, getClientSettings(settings));
    }

    static boolean checkRepositoryCredentials(Settings repositorySettings) {
        if (S3Repository.ACCESS_KEY_SETTING.exists(repositorySettings)) {
            if (S3Repository.SECRET_KEY_SETTING.exists(repositorySettings) == false) {
                throw new IllegalArgumentException("Repository setting [" + S3Repository.ACCESS_KEY_SETTING.getKey()
                        + " must be accompanied by setting [" + S3Repository.SECRET_KEY_SETTING.getKey() + "]");
            }
            return true;
        } else if (S3Repository.SECRET_KEY_SETTING.exists(repositorySettings)) {
            throw new IllegalArgumentException("Repository setting [" + S3Repository.SECRET_KEY_SETTING.getKey()
                    + " must be accompanied by setting [" + S3Repository.ACCESS_KEY_SETTING.getKey() + "]");
        }
        return false;
    }

    // backcompat for reading keys out of repository settings (clusterState)
    static BasicAWSCredentials loadRepositoryCredentials(Settings repositorySettings) {
        assert checkRepositoryCredentials(repositorySettings);
        try (SecureString key = S3Repository.ACCESS_KEY_SETTING.get(repositorySettings);
                SecureString secret = S3Repository.SECRET_KEY_SETTING.get(repositorySettings)) {
            return new BasicAWSCredentials(key.toString(), secret.toString());
        }
    }

    private static AWSCredentials loadCredentials(Settings settings) {
        try (SecureString accessKey = getConfigValue(settings, ACCESS_KEY_SETTING);
             SecureString secretKey = getConfigValue(settings, SECRET_KEY_SETTING);
             SecureString sessionToken = getConfigValue(settings, SESSION_TOKEN_SETTING)) {
            if (accessKey.length() != 0) {
                if (secretKey.length() != 0) {
                    if (sessionToken.length() != 0) {
                        return new BasicSessionCredentials(accessKey.toString(), secretKey.toString(), sessionToken.toString());
                    } else {
                        return new BasicAWSCredentials(accessKey.toString(), secretKey.toString());
                    }
                } else {
                    throw new IllegalArgumentException("Missing secret key for s3 client [" + DEFAULT + "]");
                }
            } else {
                if (secretKey.length() != 0) {
                    throw new IllegalArgumentException("Missing access key for s3 client [" + DEFAULT + "]");
                }
                if (sessionToken.length() != 0) {
                    throw new IllegalArgumentException("Missing access key and secret key for s3 client [" + DEFAULT + "]");
                }
                return null;
            }
        }
    }

    // pkg private for tests
    /** Parse settings for a single client. */
    private static S3ClientSettings getClientSettings(final Settings settings) {
        final AWSCredentials credentials = S3ClientSettings.loadCredentials(settings);
        return getClientSettings(settings, credentials);
    }

    private static S3ClientSettings getClientSettings(final Settings settings, final AWSCredentials credentials) {
        try (SecureString proxyUsername = getConfigValue(settings, PROXY_USERNAME_SETTING);
             SecureString proxyPassword = getConfigValue(settings, PROXY_PASSWORD_SETTING)) {
            return new S3ClientSettings(
                    credentials,
                    getConfigValue(settings, ENDPOINT_SETTING),
                    getConfigValue(settings, PROTOCOL_SETTING),
                    getConfigValue(settings, PROXY_HOST_SETTING),
                    getConfigValue(settings, PROXY_PORT_SETTING),
                    proxyUsername.toString(),
                    proxyPassword.toString(),
                    Math.toIntExact(getConfigValue(settings, READ_TIMEOUT_SETTING).millis()),
                    getConfigValue(settings, MAX_RETRIES_SETTING),
                    getConfigValue(settings, USE_THROTTLE_RETRIES_SETTING)
            );
        }
    }

    static S3ClientSettings getClientSettings(final RepositoryMetaData metadata, final AWSCredentials credentials) {
        final Settings.Builder builder = Settings.builder();
        for (final String key : metadata.settings().keySet()) {
            builder.put(DEFAULT_PREFIX + key, metadata.settings().get(key));
        }
        return getClientSettings(builder.build(), credentials);
    }

    private static <T> T getConfigValue(Settings settings, Setting<T> clientSetting) {
        return clientSetting.get(settings);
    }

}
