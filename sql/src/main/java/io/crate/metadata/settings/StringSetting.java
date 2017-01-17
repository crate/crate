/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.metadata.settings;

import com.google.common.base.Joiner;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.common.settings.Settings;

import javax.annotation.Nullable;
import java.util.Locale;
import java.util.Set;
import java.util.function.Function;

public class StringSetting extends Setting<String, String> {

    private final String name;
    final Set<String> allowedValues;
    private final boolean isRuntime;

    @Nullable
    private final String defaultValue;
    @Nullable
    private final Setting<?, ?> parent;

    public StringSetting(String name,
                         @Nullable Set<String> allowedValues,
                         boolean isRuntime,
                         @Nullable String defaultValue,
                         @Nullable Setting<?, ?> parent) {

        this.name = name;
        this.allowedValues = allowedValues;
        this.isRuntime = isRuntime;
        this.defaultValue = defaultValue;
        this.parent = parent;
    }

    public StringSetting(String name, @javax.annotation.Nullable Set<String> allowedValues, boolean isRuntime) {
        this(name, allowedValues, isRuntime, null, null);
    }

    public StringSetting(String name, boolean isRuntime) {
        this(name, null, isRuntime, null, null);
    }

    @Override
    public Setting parent() {
        return parent;
    }

    @Override
    public boolean isRuntime() {
        return isRuntime;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public String defaultValue() {
        return defaultValue;
    }

    @Override
    public DataType dataType() {
        return DataTypes.STRING;
    }

    @Override
    public String extract(Settings settings) {
        return settings.get(settingName(), defaultValue());
    }

    /**
     * @return Error message if not valid, else null.
     */
    @Nullable
    public String validate(String value) {
        if (allowedValues != null && !allowedValues.contains(value)) {
            return String.format(Locale.ENGLISH, "'%s' is not an allowed value. Allowed values are: %s",
                value, Joiner.on(", ").join(allowedValues)

            );
        }
        return null;
    }

    @Override
    org.elasticsearch.common.settings.Setting<String> createESSetting() {
        return new org.elasticsearch.common.settings.Setting<>(
            settingName(),
            defaultValue(),
            Function.identity(),
            propertiesForUpdateConsumer()
        );
    }
}
