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

package org.elasticsearch.discovery.ec2;

import java.time.Duration;
import java.util.Locale;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;

import io.crate.common.unit.TimeValue;
import io.crate.types.DataTypes;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;

/**
 * A container for settings used to create an EC2 client.
 */
final class Ec2ClientSettings {

    /** The access key (ie login id) for connecting to ec2. */
    static final Setting<SecureString> ACCESS_KEY_SETTING = Setting.maskedString("discovery.ec2.access_key");

    /** The secret key (ie password) for connecting to ec2. */
    static final Setting<SecureString> SECRET_KEY_SETTING = Setting.maskedString("discovery.ec2.secret_key");

    /** The session token for connecting to ec2. */
    static final Setting<SecureString> SESSION_TOKEN_SETTING = Setting.maskedString("discovery.ec2.session_token");

    /** The host name of a proxy to connect to ec2 through. */
    static final Setting<String> PROXY_HOST_SETTING = Setting.simpleString("discovery.ec2.proxy.host", Property.NodeScope);

    /** The port of a proxy to connect to ec2 through. */
    static final Setting<Integer> PROXY_PORT_SETTING = Setting.intSetting("discovery.ec2.proxy.port", 80, 0, 1 << 16, Property.NodeScope);

    /** An override for the ec2 endpoint to connect to. */
    static final Setting<String> ENDPOINT_SETTING = new Setting<>(
        "discovery.ec2.endpoint",
        "",
        s -> s.toLowerCase(Locale.ROOT),
        DataTypes.STRING,
        Property.NodeScope
    );

    /** The protocol to use to connect to to ec2. */
    static final Setting<String> PROTOCOL_SETTING = new Setting<>(
        "discovery.ec2.protocol",
        "https",
        s -> s.toLowerCase(Locale.ROOT),
        DataTypes.STRING,
        Property.NodeScope
    );

    /** The username of a proxy to connect to s3 through. */
    static final Setting<SecureString> PROXY_USERNAME_SETTING = Setting.maskedString("discovery.ec2.proxy.username");

    /** The password of a proxy to connect to s3 through. */
    static final Setting<SecureString> PROXY_PASSWORD_SETTING = Setting.maskedString("discovery.ec2.proxy.password");

    /** The socket timeout for connecting to s3. */
    static final Setting<TimeValue> READ_TIMEOUT_SETTING = Setting.timeSetting("discovery.ec2.read_timeout",
        // SdkHttpConfigurationOption.DEFAULT_SOCKET_READ_TIMEOUT = Duration.ofSeconds(30L); is inaccessible
        TimeValue.timeValueMillis(Duration.ofSeconds(30L).toMillis()), Property.NodeScope);

    private static final Logger LOGGER = LogManager.getLogger(Ec2ClientSettings.class);

    private static final DeprecationLogger DEPRECATION_LOGGER = new DeprecationLogger(LOGGER);

    /** Credentials to authenticate with ec2. */
    final AwsCredentials credentials;

    /**
     * The ec2 endpoint the client should talk to, or empty string to use the
     * default.
     */
    final String endpoint;

    /** The protocol to use to talk to ec2. Defaults to https. */
    final String protocol;

    /** An optional proxy host that requests to ec2 should be made through. */
    final String proxyHost;

    /** The port number the proxy host should be connected on. */
    final int proxyPort;

    // these should be "secure" yet the api for the ec2 client only takes String, so
    // storing them
    // as SecureString here won't really help with anything
    /** An optional username for the proxy host, for basic authentication. */
    final String proxyUsername;

    /** An optional password for the proxy host, for basic authentication. */
    final String proxyPassword;

    /** The read timeout for the ec2 client. */
    final int readTimeoutMillis;

    protected Ec2ClientSettings(AwsCredentials credentials, String endpoint, String protocol, String proxyHost, int proxyPort,
            String proxyUsername, String proxyPassword, int readTimeoutMillis) {
        this.credentials = credentials;
        this.endpoint = endpoint;
        this.protocol = protocol;
        this.proxyHost = proxyHost;
        this.proxyPort = proxyPort;
        this.proxyUsername = proxyUsername;
        this.proxyPassword = proxyPassword;
        this.readTimeoutMillis = readTimeoutMillis;
    }

    static AwsCredentials loadCredentials(Settings settings) {
        try (SecureString key = ACCESS_KEY_SETTING.get(settings);
             SecureString secret = SECRET_KEY_SETTING.get(settings);
             SecureString sessionToken = SESSION_TOKEN_SETTING.get(settings)) {
            if (key.length() == 0 && secret.length() == 0) {
                if (sessionToken.length() > 0) {
                    throw new SettingsException("Setting [{}] is set but [{}] and [{}] are not",
                        SESSION_TOKEN_SETTING.getKey(), ACCESS_KEY_SETTING.getKey(), SECRET_KEY_SETTING.getKey());
                }

                LOGGER.debug("Using either environment variables, system properties or instance profile credentials");
                return null;
            } else {
                if (key.length() == 0) {
                    DEPRECATION_LOGGER.deprecatedAndMaybeLog("ec2_invalid_settings",
                        "Setting [{}] is set but [{}] is not, which will be unsupported in future",
                        SECRET_KEY_SETTING.getKey(), ACCESS_KEY_SETTING.getKey());
                }
                if (secret.length() == 0) {
                    DEPRECATION_LOGGER.deprecatedAndMaybeLog("ec2_invalid_settings",
                        "Setting [{}] is set but [{}] is not, which will be unsupported in future",
                         ACCESS_KEY_SETTING.getKey(), SECRET_KEY_SETTING.getKey());
                }

                final AwsCredentials credentials;
                if (sessionToken.length() == 0) {
                    LOGGER.debug("Using basic key/secret credentials");
                    credentials = AwsBasicCredentials.create(key.toString(), secret.toString());
                } else {
                    LOGGER.debug("Using basic session credentials");
                    credentials = AwsSessionCredentials.create(key.toString(), secret.toString(), sessionToken.toString());
                }
                return credentials;
            }
        }
    }

    // pkg private for tests
    /** Parse settings for a single client. */
    static Ec2ClientSettings getClientSettings(Settings settings) {
        final AwsCredentials credentials = loadCredentials(settings);
        try (SecureString proxyUsername = PROXY_USERNAME_SETTING.get(settings);
             SecureString proxyPassword = PROXY_PASSWORD_SETTING.get(settings)) {
            return new Ec2ClientSettings(
                credentials,
                ENDPOINT_SETTING.get(settings),
                PROTOCOL_SETTING.get(settings),
                PROXY_HOST_SETTING.get(settings),
                PROXY_PORT_SETTING.get(settings),
                proxyUsername.toString(),
                proxyPassword.toString(),
                (int)READ_TIMEOUT_SETTING.get(settings).millis());
        }
    }

}
