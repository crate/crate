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

public class IntSetting extends Setting<Integer, Integer> {

    private final String name;
    private final Integer defaultValue;
    private final boolean isRuntimeSetting;

    public IntSetting(String name, Integer defaultValue, boolean isRuntimeSetting) {
        this.name = name;
        this.defaultValue = defaultValue;
        this.isRuntimeSetting = isRuntimeSetting;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public Integer defaultValue() {
        return defaultValue;
    }

    @Override
    public boolean isRuntime() {
        return isRuntimeSetting;
    }

    public Integer maxValue() {
        return Integer.MAX_VALUE;
    }

    public Integer minValue() {
        return Integer.MIN_VALUE;
    }

    @Override
    public DataType dataType() {
        return DataTypes.INTEGER;
    }

    @Override
    public Integer extract(Settings settings) {
        return settings.getAsInt(settingName(), defaultValue());
    }
}
