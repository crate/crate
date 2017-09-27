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
import io.crate.metadata.settings.ByteSizeSetting;
import io.crate.metadata.settings.SettingsApplier;
import io.crate.metadata.settings.SettingsAppliers;
import io.crate.sql.tree.GenericProperties;

import java.util.Map;
import java.util.Optional;

public class TypeSettings {

    private static final Map<String, SettingsApplier> GENERIC = ImmutableMap.<String, SettingsApplier>builder()
        .put("max_restore_bytes_per_sec", new SettingsAppliers.ByteSizeSettingsApplier(new ByteSizeSetting("max_restore_bytes_per_sec", null)))
        .put("max_snapshot_bytes_per_sec", new SettingsAppliers.ByteSizeSettingsApplier(new ByteSizeSetting("max_snapshot_bytes_per_sec", null)))
        .build();

    private final Map<String, SettingsApplier> required;
    private final Map<String, SettingsApplier> all;

    public TypeSettings(Map<String, SettingsApplier> required,
                        Map<String, SettingsApplier> optional) {
        this.required = required;
        this.all = ImmutableMap.<String, SettingsApplier>builder().putAll(required).putAll(optional).putAll(GENERIC).build();
    }

    public Map<String, SettingsApplier> required() {
        return required;
    }

    public Map<String, SettingsApplier> all() {
        return all;
    }


    /**
     * Return possible dynamic GenericProperties which will not be validated.
     */
    public Optional<GenericProperties> dynamicProperties(Optional<GenericProperties> genericProperties) {
        return Optional.empty();
    }
}
