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

import static org.elasticsearch.repositories.s3.S3RepositorySettings.ACCESS_KEY_SETTING;
import static org.elasticsearch.repositories.s3.S3RepositorySettings.ENDPOINT_SETTING;
import static org.elasticsearch.repositories.s3.S3RepositorySettings.MAX_RETRIES_SETTING;
import static org.elasticsearch.repositories.s3.S3RepositorySettings.PROTOCOL_SETTING;
import static org.elasticsearch.repositories.s3.S3RepositorySettings.PROXY_HOST_SETTING;
import static org.elasticsearch.repositories.s3.S3RepositorySettings.PROXY_PASSWORD_SETTING;
import static org.elasticsearch.repositories.s3.S3RepositorySettings.PROXY_PORT_SETTING;
import static org.elasticsearch.repositories.s3.S3RepositorySettings.PROXY_USERNAME_SETTING;
import static org.elasticsearch.repositories.s3.S3RepositorySettings.READ_TIMEOUT_SETTING;
import static org.elasticsearch.repositories.s3.S3RepositorySettings.SECRET_KEY_SETTING;
import static org.elasticsearch.repositories.s3.S3RepositorySettings.SESSION_TOKEN_SETTING;
import static org.elasticsearch.repositories.s3.S3RepositorySettings.USE_THROTTLE_RETRIES_SETTING;

import java.util.Objects;

import org.jetbrains.annotations.Nullable;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;

import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.BasicSessionCredentials;

/**
 * A container for settings used to create an S3 client.
 */
final class S3ClientSettings {

    /** Credentials to authenticate with s3. */
    @Nullable
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

    private S3ClientSettings(@Nullable AWSCredentials credentials,
                             String endpoint,
                             Protocol protocol,
                             String proxyHost,
                             int proxyPort,
                             String proxyUsername,
                             String proxyPassword,
                             int readTimeoutMillis,
                             int maxRetries,
                             boolean throttleRetries) {
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
                    throw new IllegalArgumentException("Missing secret key for s3 client");
                }
            } else {
                if (secretKey.length() != 0) {
                    throw new IllegalArgumentException("Missing access key for s3 client");
                }
                if (sessionToken.length() != 0) {
                    throw new IllegalArgumentException("Missing access key and secret key for s3 client");
                }
                return null;
            }
        }
    }

    // pkg private for tests
    /** Parse settings for a single client. */
    static S3ClientSettings getClientSettings(final Settings settings) {
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

    private static <T> T getConfigValue(Settings settings, Setting<T> clientSetting) {
        return clientSetting.get(settings);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        S3ClientSettings that = (S3ClientSettings) o;
        return proxyPort == that.proxyPort &&
               readTimeoutMillis == that.readTimeoutMillis &&
               maxRetries == that.maxRetries &&
               throttleRetries == that.throttleRetries &&
               compareCredentials(credentials, that.credentials) &&
               Objects.equals(endpoint, that.endpoint) &&
               protocol == that.protocol &&
               Objects.equals(proxyHost, that.proxyHost) &&
               Objects.equals(proxyUsername, that.proxyUsername) &&
               Objects.equals(proxyPassword, that.proxyPassword);
    }

    @Override
    public int hashCode() {
        String accessKey = null;
        String secretKey = null;
        if (credentials != null) {
            accessKey = credentials.getAWSAccessKeyId();
            secretKey = credentials.getAWSSecretKey();
        }
        return Objects.hash(accessKey,
                            secretKey,
                            endpoint,
                            protocol,
                            proxyHost,
                            proxyPort,
                            proxyUsername,
                            proxyPassword,
                            readTimeoutMillis,
                            maxRetries,
                            throttleRetries);
    }

    private boolean compareCredentials(@Nullable AWSCredentials first, @Nullable AWSCredentials second) {
        if (first != null && second != null) {
            return first.getAWSAccessKeyId().equals(second.getAWSAccessKeyId()) &&
                   first.getAWSSecretKey().equals(second.getAWSSecretKey());
        } else {
            return first == second;
        }
    }
}
