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

package io.crate.execution.engine.collect.files;

import static io.crate.analyze.CopyStatementSettings.COMMON_COPY_FROM_SETTINGS;
import static io.crate.analyze.CopyStatementSettings.COMMON_COPY_TO_SETTINGS;

import java.util.List;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;

import io.crate.common.collections.Lists;

/**
 * Represents scheme specific settings for COPY FROM/TO statements.
 * Created per schema by the {@link io.crate.plugin.CopyPlugin}.
 */
public class SchemeSettings {

    private final List<Setting<?>> mandatorySettings;

    private final List<String> allKnownSettings;

    public SchemeSettings(List<Setting<?>> mandatorySettings, List<Setting<?>> optionalSettings) {
        this.mandatorySettings = mandatorySettings;
        this.allKnownSettings = Lists.concat(
            mandatorySettings.stream().map(Setting::getKey).toList(),
            optionalSettings.stream().map(Setting::getKey).toList()
        );
    }

    public void validate(Settings settings, boolean read) {
        List<String> validSettings = Lists.concat(
            allKnownSettings,
            read ? COMMON_COPY_FROM_SETTINGS : COMMON_COPY_TO_SETTINGS
        );
        for (String key : settings.keySet()) {
            if (validSettings.contains(key) == false) {
                throw new IllegalArgumentException("Setting '" + key + "' is not supported");
            }
        }

        for (Setting<?> setting: mandatorySettings) {
            if (settings.get(setting.getKey()) == null) {
                throw new IllegalArgumentException("Setting '" + setting.getKey() + "' must be provided");
            }
        }
    }
}
