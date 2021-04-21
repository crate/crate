/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.protocols.ssl;

import java.util.Locale;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;

import io.crate.common.unit.TimeValue;
import io.crate.types.DataTypes;

/**
 * Settings for configuring Postgres SSL. Only applicable to the ssl-impl module.
 */
public final class SslSettings {

    private SslSettings() {
    }

    private static final String SSL_TRANSPORT_MODE_NAME = "ssl.transport.mode";
    private static final String SSL_HTTP_ENABLED_SETTING_NAME = "ssl.http.enabled";
    private static final String SSL_PSQL_ENABLED_SETTING_NAME = "ssl.psql.enabled";

    static final String SSL_TRUSTSTORE_FILEPATH_SETTING_NAME = "ssl.truststore_filepath";
    static final String SSL_TRUSTSTORE_PASSWORD_SETTING_NAME = "ssl.truststore_password";
    static final String SSL_KEYSTORE_FILEPATH_SETTING_NAME = "ssl.keystore_filepath";
    static final String SSL_KEYSTORE_PASSWORD_SETTING_NAME = "ssl.keystore_password";
    static final String SSL_KEYSTORE_KEY_PASSWORD_SETTING_NAME = "ssl.keystore_key_password";

    static final String SSL_RESOURCE_POLL_INTERVAL_NAME = "ssl.resource_poll_interval";

    public static final Setting<Boolean> SSL_HTTP_ENABLED = Setting.boolSetting(
        SSL_HTTP_ENABLED_SETTING_NAME,
        false,
        Setting.Property.NodeScope
    );

    public static final Setting<Boolean> SSL_PSQL_ENABLED = Setting.boolSetting(
        SSL_PSQL_ENABLED_SETTING_NAME,
        false,
        Setting.Property.NodeScope
    );

    public enum SSLMode {
        ON,
        OFF,
        DUAL;

        static SSLMode parse(String value) {
            return switch (value.toLowerCase(Locale.ENGLISH)) {
                // allow true/false as well because YAML `on` is interpreted as true
                case "on" -> ON;
                case "true" -> ON;
                case "off" -> OFF;
                case "false" -> OFF;
                case "dual" -> DUAL;
                default -> throw new IllegalArgumentException(value + " is not a valid SSL mode setting");
            };
        }
    }

    public static final Setting<SSLMode> SSL_TRANSPORT_MODE = new Setting<SSLMode>(
        SSL_TRANSPORT_MODE_NAME,
        settings -> SSLMode.OFF.name(),
        SSLMode::parse,
        DataTypes.STRING,
        Setting.Property.NodeScope
    );

    public static final Setting<String> SSL_TRUSTSTORE_FILEPATH = Setting.simpleString(
        SSL_TRUSTSTORE_FILEPATH_SETTING_NAME, Setting.Property.NodeScope);

    public static final Setting<String> SSL_TRUSTSTORE_PASSWORD = Setting.simpleString(
        SSL_TRUSTSTORE_PASSWORD_SETTING_NAME, Setting.Property.NodeScope);

    public static final Setting<String> SSL_KEYSTORE_FILEPATH = Setting.simpleString(
        SSL_KEYSTORE_FILEPATH_SETTING_NAME, Setting.Property.NodeScope);

    public static final Setting<String> SSL_KEYSTORE_PASSWORD = Setting.simpleString(
        SSL_KEYSTORE_PASSWORD_SETTING_NAME, "", Setting.Property.NodeScope);

    public static final Setting<String> SSL_KEYSTORE_KEY_PASSWORD = Setting.simpleString(
        SSL_KEYSTORE_KEY_PASSWORD_SETTING_NAME, Setting.Property.NodeScope);

    public static final Setting<TimeValue> SSL_RESOURCE_POLL_INTERVAL = Setting.positiveTimeSetting(
        SSL_RESOURCE_POLL_INTERVAL_NAME,
        TimeValue.timeValueMinutes(5),
        Setting.Property.NodeScope
    );

    public static boolean isHttpsEnabled(Settings settings) {
        return SSL_HTTP_ENABLED.get(settings);
    }

    public static boolean isPSQLSslEnabled(Settings settings) {
        return SSL_PSQL_ENABLED.get(settings);
    }
}
