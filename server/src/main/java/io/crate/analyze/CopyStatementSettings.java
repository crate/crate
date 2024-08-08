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

import java.util.Locale;

import org.elasticsearch.common.settings.Setting;

import com.fasterxml.jackson.dataformat.csv.CsvSchema;

import io.crate.metadata.settings.Validators;
import io.crate.types.DataTypes;

public final class CopyStatementSettings {

    private CopyStatementSettings() {
    }

    public static final Setting<Boolean> WAIT_FOR_COMPLETION_SETTING = Setting.boolSetting(
        "wait_for_completion",
        true
    );

    public static final Setting<String> COMPRESSION_SETTING = Setting.simpleString(
        "compression",
        Validators.stringValidator("compression", "gzip"),
        Setting.Property.Dynamic);

    public static final Setting<String> OUTPUT_FORMAT_SETTING = Setting.simpleString(
        "format",
        Validators.stringValidator("format", "json_object", "json_array"),
        Setting.Property.Dynamic);

    public static final Setting<String> INPUT_FORMAT_SETTING = new Setting<>(
        "format",
        "json",
        (s) -> s,
        Validators.stringValidator("format", "json", "csv"),
        DataTypes.STRING,
        Setting.Property.Dynamic);

    public static final Setting<Boolean> EMPTY_STRING_AS_NULL = Setting.boolSetting(
        "empty_string_as_null",
        false,
        Setting.Property.Dynamic);

    public static final Setting<Boolean> INPUT_HEADER_SETTINGS = Setting.boolSetting(
        "header",
        true,
        Setting.Property.Dynamic);

    public static final Setting<Long> SKIP_NUM_LINES = Setting.longSetting("skip", 0, 0, Setting.Property.Dynamic);

    public static final Setting<Character> CSV_COLUMN_SEPARATOR = new Setting<>(
        "delimiter",
        String.valueOf(CsvSchema.DEFAULT_COLUMN_SEPARATOR),
        value -> {
            if (value.length() != 1) {
                throw new IllegalArgumentException(
                    "Invalid CSV fields delimiter: " + value + ". The delimiter must be a single character.");
            }
            return value.charAt(0);
        },
        DataTypes.STRING,
        Setting.Property.Dynamic
    );

    public static <E extends Enum<E>> E settingAsEnum(Class<E> settingsEnum, String settingValue) {
        if (settingValue == null || settingValue.isEmpty()) {
            return null;
        }
        return Enum.valueOf(settingsEnum, settingValue.toUpperCase(Locale.ENGLISH));
    }
}
