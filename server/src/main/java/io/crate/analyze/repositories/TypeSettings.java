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

package io.crate.analyze.repositories;

import com.google.common.collect.ImmutableMap;
import io.crate.sql.tree.GenericProperties;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class TypeSettings {

    private static final Map<String, Setting<?>> GENERIC = ImmutableMap.<String, Setting<?>>builder()
        .put("max_restore_bytes_per_sec", Setting.byteSizeSetting("max_restore_bytes_per_sec", new ByteSizeValue(40, ByteSizeUnit.MB), Setting.Property.NodeScope))
        .put("max_snapshot_bytes_per_sec", Setting.byteSizeSetting("max_snapshot_bytes_per_sec", new ByteSizeValue(40, ByteSizeUnit.MB), Setting.Property.NodeScope))
        .build();

    private final Map<String, Setting<?>> required;
    private final Map<String, Setting<?>> all;

    public TypeSettings(List<Setting<?>> required, List<Setting<?>> optional) {
        this(
            required.stream().collect(Collectors.toMap(Setting::getKey, Function.identity())),
            optional.stream().collect(Collectors.toMap(Setting::getKey, Function.identity()))
        );
    }

    public TypeSettings(Map<String, Setting<?>> required, Map<String, Setting<?>> optional) {
        this.required = required;
        this.all = ImmutableMap.<String, Setting<?>>builder()
            .putAll(required)
            .putAll(optional)
            .putAll(GENERIC)
            .build();
    }

    public Map<String, Setting<?>> required() {
        return required;
    }

    public Map<String, Setting<?>> all() {
        return all;
    }


    /**
     * Return possible dynamic GenericProperties which will not be validated.
     */
    public GenericProperties<?> dynamicProperties(GenericProperties<?> genericProperties) {
        return GenericProperties.empty();
    }
}
