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
import com.google.common.collect.ImmutableList;
import io.crate.types.DataType;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.settings.Settings;

import java.util.List;

public abstract class Setting<T, E> {

    private final static Joiner dotJoiner = Joiner.on(".");

    public String settingName() {
        return dotJoiner.join(chain());
    }

    public abstract String name();

    public abstract T defaultValue();

    public abstract E extract(Settings settings);

    public abstract boolean isRuntime();

    public List<Setting> children() {
        return ImmutableList.of();
    }

    @Nullable
    public Setting parent() {
        return null;
    }

    public abstract DataType dataType();

    /**
     * Return a list of setting names up to the uppers parent which will be used
     * e.g. to compute the full-qualified setting name
     */
    public List<String> chain() {
        Setting parentSetting = parent();
        if (parentSetting == null) { return ImmutableList.of(name()); }
        ImmutableList.Builder<String> builder = ImmutableList.builder();
        builder.add(name());
        while (parentSetting != null) {
            builder.add(parentSetting.name());
            parentSetting = parentSetting.parent();
        }
        return builder.build().reverse();

    }
}
