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

import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.common.settings.Settings;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class BoolSetting extends Setting<Boolean, Boolean> {

    private final String name;
    private final boolean value;
    private final boolean isRuntimeSetting;
    private final Setting parent;

    public BoolSetting(String name, boolean defaultValue, boolean isRuntimeSetting) {
        this(name, defaultValue, isRuntimeSetting, null, null);
    }

    public BoolSetting(String name,
                       boolean defaultValue,
                       boolean isRuntimeSetting,
                       @Nullable Setting parent,
                       @Nullable org.elasticsearch.common.settings.Setting<Boolean> esSetting) {
        this.name = name;
        this.value = defaultValue;
        this.isRuntimeSetting = isRuntimeSetting;
        this.parent = parent;
        this.esSetting = esSetting;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public Boolean defaultValue() {
        return value;
    }

    @Override
    public Setting parent() {
        return parent;
    }

    @Override
    public Boolean extract(Settings settings) {
        return settings.getAsBoolean(settingName(), defaultValue());
    }

    public Boolean extract(Settings settings, @Nonnull Boolean defaultValue) {
        return settings.getAsBoolean(settingName(), defaultValue);
    }

    @Override
    public boolean isRuntime() {
        return isRuntimeSetting;
    }

    @Override
    public DataType dataType() {
        return DataTypes.BOOLEAN;
    }

    @Override
    org.elasticsearch.common.settings.Setting<Boolean> createESSetting() {
        return org.elasticsearch.common.settings.Setting.boolSetting(
            settingName(),
            defaultValue(),
            propertiesForUpdateConsumer()
        );
    }
}
