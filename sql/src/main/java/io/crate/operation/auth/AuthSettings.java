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

package io.crate.operation.auth;

import io.crate.settings.CrateSetting;
import io.crate.types.DataTypes;
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
        "auth.host_based.config.", Setting.Property.NodeScope),
        DataTypes.OBJECT);

    public static final CrateSetting<String> AUTH_TRUST_HTTP_DEFAULT_HEADER = CrateSetting.of(new Setting<>(
        "auth.trust.http_default_user", "crate", Function.identity(), Setting.Property.NodeScope),
        DataTypes.STRING);

    @Deprecated //Scheduled to be removed as HTTP Basic Auth Header is introduced"
    public static final String HTTP_HEADER_USER = "X-User";
    public static final String HTTP_HEADER_REAL_IP = "X-Real-Ip";
}
