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

package io.crate.analyze;


import org.elasticsearch.common.settings.Setting;

import java.util.Map;

public final class SnapshotSettings {

    public static final Setting<Boolean> IGNORE_UNAVAILABLE = Setting.boolSetting("ignore_unavailable", false);

    public static final Setting<Boolean> WAIT_FOR_COMPLETION = Setting.boolSetting("wait_for_completion", false);

    public static final Setting<String> SCHEMA_RENAME_PATTERN = Setting.simpleString("schema_rename_pattern", "(.+)");

    public static final Setting<String> SCHEMA_RENAME_REPLACEMENT = Setting.simpleString("schema_rename_replacement", "$1");

    public static final Setting<String> TABLE_RENAME_PATTERN = Setting.simpleString("table_rename_pattern", "(.+)");

    public static final Setting<String> TABLE_RENAME_REPLACEMENT = Setting.simpleString("table_rename_replacement", "$1");

    public static final Map<String, Setting<?>> SETTINGS = Map.of(
        IGNORE_UNAVAILABLE.getKey(), IGNORE_UNAVAILABLE,
        WAIT_FOR_COMPLETION.getKey(), WAIT_FOR_COMPLETION,
        SCHEMA_RENAME_PATTERN.getKey(), SCHEMA_RENAME_PATTERN,
        SCHEMA_RENAME_REPLACEMENT.getKey(), SCHEMA_RENAME_REPLACEMENT,
        TABLE_RENAME_PATTERN.getKey(), TABLE_RENAME_PATTERN,
        TABLE_RENAME_REPLACEMENT.getKey(), TABLE_RENAME_REPLACEMENT
    );

    private SnapshotSettings() {
    }
}
