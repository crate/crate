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

package io.crate.copy;

import static io.crate.analyze.CopyStatementSettings.COMMON_COPY_FROM_SETTINGS;
import static io.crate.analyze.CopyStatementSettings.COMMON_COPY_TO_SETTINGS;

import java.util.List;
import java.util.Map;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;

import io.crate.common.collections.Lists;

public interface Configuration<T extends OpenDalURI> {

    List<Setting<String>> supportedSettings();

    /**
     * @param read is true for COPY FROM validation and false for COPY TO.
     */
    default void validate(Settings settings, boolean read) {
        List<String> validSettings = Lists.concat(
            supportedSettings().stream().map(Setting::getKey).toList(),
            read ? COMMON_COPY_FROM_SETTINGS : COMMON_COPY_TO_SETTINGS
        );
        for (String key : settings.keySet()) {
            if (validSettings.contains(key) == false) {
                throw new IllegalArgumentException("Setting '" + key + "' is not supported");
            }
        }
    }

    Map<String, String> fromURIAndSettings(T uri, Settings settings);
}
