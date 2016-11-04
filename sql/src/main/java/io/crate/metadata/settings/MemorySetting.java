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

package io.crate.metadata.settings;

import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.MemorySizeValue;

import javax.annotation.Nullable;

public class MemorySetting extends Setting<ByteSizeValue, ByteSizeValue> {

    private final String name;
    private final String defaultValue;
    private final boolean isRuntime;
    private final Setting<?, ?> parent;

    public MemorySetting(String name,
                         String defaultValue,
                         boolean isRuntime,
                         @Nullable Setting<?, ?> parent,
                         @Nullable org.elasticsearch.common.settings.Setting<ByteSizeValue> esSetting) {
        this.name = name;
        this.defaultValue = defaultValue;
        this.isRuntime = isRuntime;
        this.parent = parent;
        this.esSetting = esSetting;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public ByteSizeValue defaultValue() {
        return MemorySizeValue.parseBytesSizeValueOrHeapRatio(defaultValue, settingName());
    }

    @Override
    public ByteSizeValue extract(Settings settings) {
        return settings.getAsMemory(settingName(), defaultValue);
    }

    @Override
    public boolean isRuntime() {
        return isRuntime;
    }

    @Override
    public DataType dataType() {
        return DataTypes.STRING;
    }

    @Override
    public Setting parent() {
        return parent;
    }

    @Override
    org.elasticsearch.common.settings.Setting<ByteSizeValue> createESSetting() {
        return org.elasticsearch.common.settings.Setting.memorySizeSetting(
            settingName(),
            defaultValue(),
            propertiesForUpdateConsumer()
        );
    }
}
