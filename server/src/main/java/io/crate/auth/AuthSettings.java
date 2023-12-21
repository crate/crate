/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.auth;

import java.util.function.Function;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;

import io.crate.types.DataTypes;
import io.netty.handler.ssl.ClientAuth;

public final class AuthSettings {

    private AuthSettings() {
    }

    public static final Setting<Boolean> AUTH_HOST_BASED_ENABLED_SETTING = Setting.boolSetting(
        "auth.host_based.enabled",
        false,
        Setting.Property.NodeScope
    );

    public static final Setting<Settings> AUTH_HOST_BASED_CONFIG_SETTING = Setting.groupSetting(
        "auth.host_based.config.", Setting.Property.NodeScope
    );

    // Explicit generic is required for eclipse JDT, otherwise it won't compile
    public static final Setting<String> AUTH_TRUST_HTTP_DEFAULT_HEADER = new Setting<String>(
        "auth.trust.http_default_user",
        "crate",
        Function.identity(),
        DataTypes.STRING,
        Setting.Property.NodeScope
    );

    public static final String HTTP_HEADER_REAL_IP = "X-Real-Ip";
    public static final Setting<Boolean> AUTH_TRUST_HTTP_SUPPORT_X_REAL_IP = Setting.boolSetting(
        "auth.trust.http_support_x_real_ip",
        false,
        Setting.Property.NodeScope
    );

    public static ClientAuth resolveClientAuth(Settings settings, Protocol protocol) {
        Settings hbaSettings = AUTH_HOST_BASED_CONFIG_SETTING.get(settings);
        int numMethods = 0;
        int numCertMethods = 0;
        for (var entry : hbaSettings.getAsGroups().entrySet()) {
            Settings entrySettings = entry.getValue();
            String protocolEntry = entrySettings.get("protocol");
            if (protocolEntry != null && !protocol.name().equalsIgnoreCase(protocolEntry)) {
                // We need null check for protocolEntry since we want entry without protocol be matched with any protocol
                // Without it !equalsIgnoreCase returns true and HBA with only 'cert' entries but all without protocol
                // might end up with NONE while correct value is 'REQUIRED'.
                continue;
            }
            String method = entrySettings.get("method", "trust");
            numMethods++;
            if (method.equals("cert")) {
                numCertMethods++;
            }
        }
        if (numCertMethods == 0) {
            return ClientAuth.NONE;
        }
        return numCertMethods == numMethods
            ? ClientAuth.REQUIRE
            : ClientAuth.OPTIONAL;
    }
}
