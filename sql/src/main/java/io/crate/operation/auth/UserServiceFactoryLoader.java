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

import io.crate.plugin.EnterpriseLoader;
import io.crate.settings.SharedSettings;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.settings.Settings;

import java.io.IOException;
import java.nio.file.Path;

import static org.elasticsearch.common.Strings.cleanPath;
import static org.elasticsearch.env.Environment.PATH_HOME_SETTING;

public final class UserServiceFactoryLoader {

    @Nullable
    public static UserServiceFactory load(Settings settings) {
        if (!SharedSettings.ENTERPRISE_LICENSE_SETTING.setting().get(settings)) {
            return null;
        }
        Path crateHomeDir = PathUtils.get(cleanPath(PATH_HOME_SETTING.get(settings)));
        Path esPlugins = crateHomeDir.resolve("lib").resolve("enterprise").resolve("es-plugins");

        try {
            return EnterpriseLoader.loadSingle(esPlugins, UserServiceFactory.class);
        } catch (IOException e) {
            return null;
        }
    }
}
