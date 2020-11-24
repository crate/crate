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

package io.crate.auth;

import io.crate.settings.CrateSetting;
import io.crate.types.DataTypes;
import io.netty.handler.ssl.ClientAuth;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;

import java.util.function.Function;


public final class AuthSettings {

    private AuthSettings() {
    }

    public static final CrateSetting<Boolean> AUTH_HOST_BASED_ENABLED_SETTING = CrateSetting.of(Setting.boolSetting(
        "auth.host_based.enabled",
        false, Setting.Property.NodeScope),
        DataTypes.BOOLEAN);

    public static final CrateSetting<Settings> AUTH_HOST_BASED_CONFIG_SETTING = CrateSetting.of(Setting.groupSetting(
        "auth.host_based.config.", Setting.Property.NodeScope), DataTypes.UNTYPED_OBJECT);

    public static final CrateSetting<String> AUTH_TRUST_HTTP_DEFAULT_HEADER = CrateSetting.of(
        // Explicit generic is required for eclipse JDT, otherwise it won't compile
        new Setting<String>(
            "auth.trust.http_default_user",
            "crate",
            Function.identity(),
            Setting.Property.NodeScope
        ),
        DataTypes.STRING
    );

    public static final String HTTP_HEADER_REAL_IP = "X-Real-Ip";

    public static ClientAuth resolveClientAuth(Settings settings) {
        Settings hbaSettings = AUTH_HOST_BASED_CONFIG_SETTING.setting().get(settings);
        int numMethods = 0;
        int numCertMethods = 0;
        for (var entry : hbaSettings.getAsGroups().entrySet()) {
            Settings entrySettings = entry.getValue();
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
