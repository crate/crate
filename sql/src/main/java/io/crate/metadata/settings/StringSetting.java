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
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.settings.Settings;

import java.util.Set;

public abstract class StringSetting extends Setting<String, String> {

    protected Set<String> allowedValues;

    protected StringSetting(Set<String> allowedValues) {
        this.allowedValues = allowedValues;
    }

    protected StringSetting() {
        this.allowedValues = null;
    }

    @Override
    public String defaultValue() {
        return "";
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
            return String.format("'%s' is not an allowed value. Allowed values are: %s",
                    value, Joiner.on(", ").join(allowedValues)

            );
        }
        return null;
    }
}
