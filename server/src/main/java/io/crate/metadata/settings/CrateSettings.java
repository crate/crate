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

package io.crate.metadata.settings;

import static org.elasticsearch.common.settings.AbstractScopedSettings.ARCHIVED_SETTINGS_PREFIX;
import static org.elasticsearch.common.settings.AbstractScopedSettings.LOGGER_SETTINGS_PREFIX;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import org.elasticsearch.cluster.routing.allocation.decider.FilterAllocationDecider;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;

import io.crate.common.collections.Lists;
import io.crate.types.DataTypes;

public final class CrateSettings {

    /**
     * List of cluster/node settings exposed via `sys.cluster.settings`,
     * see also {@link io.crate.metadata.sys.SysClusterTableInfo}
     */
    public static final List<Setting<?>> EXPOSED_SETTINGS = Lists.concat(
        ClusterSettings.BUILT_IN_CLUSTER_SETTINGS.stream()
            .filter(Setting::isExposed)
            .toList(),
        List.of(
            Setting.simpleString(
                FilterAllocationDecider.CLUSTER_ROUTING_INCLUDE_GROUP_SETTING.getKey() + "_ip",
                Setting.Property.NodeScope, Setting.Property.Dynamic),
            Setting.simpleString(
                FilterAllocationDecider.CLUSTER_ROUTING_INCLUDE_GROUP_SETTING.getKey() + "_id",
                Setting.Property.NodeScope, Setting.Property.Dynamic),
            Setting.simpleString(
                FilterAllocationDecider.CLUSTER_ROUTING_INCLUDE_GROUP_SETTING.getKey() + "_host",
                Setting.Property.NodeScope, Setting.Property.Dynamic),
            Setting.simpleString(
                FilterAllocationDecider.CLUSTER_ROUTING_INCLUDE_GROUP_SETTING.getKey() + "_name",
                Setting.Property.NodeScope, Setting.Property.Dynamic),
            Setting.simpleString(
                FilterAllocationDecider.CLUSTER_ROUTING_EXCLUDE_GROUP_SETTING.getKey() + "_ip",
                Setting.Property.NodeScope, Setting.Property.Dynamic),
            Setting.simpleString(
                FilterAllocationDecider.CLUSTER_ROUTING_EXCLUDE_GROUP_SETTING.getKey() + "_id",
                Setting.Property.NodeScope, Setting.Property.Dynamic),
            Setting.simpleString(
                FilterAllocationDecider.CLUSTER_ROUTING_EXCLUDE_GROUP_SETTING.getKey() + "_host",
                Setting.Property.NodeScope, Setting.Property.Dynamic),
            Setting.simpleString(
                FilterAllocationDecider.CLUSTER_ROUTING_EXCLUDE_GROUP_SETTING.getKey() + "_name",
                Setting.Property.NodeScope, Setting.Property.Dynamic),
            Setting.simpleString(
                FilterAllocationDecider.CLUSTER_ROUTING_REQUIRE_GROUP_SETTING.getKey() + "_ip",
                Setting.Property.NodeScope, Setting.Property.Dynamic),
            Setting.simpleString(
                FilterAllocationDecider.CLUSTER_ROUTING_REQUIRE_GROUP_SETTING.getKey() + "_id",
                Setting.Property.NodeScope, Setting.Property.Dynamic),
            Setting.simpleString(
                FilterAllocationDecider.CLUSTER_ROUTING_REQUIRE_GROUP_SETTING.getKey() + "_host",
                Setting.Property.NodeScope, Setting.Property.Dynamic),
            Setting.simpleString(
                FilterAllocationDecider.CLUSTER_ROUTING_REQUIRE_GROUP_SETTING.getKey() + "_name",
                Setting.Property.NodeScope, Setting.Property.Dynamic)
        )
    );

    private static final List<String> EXPOSED_SETTING_NAMES = EXPOSED_SETTINGS.stream()
        .map(Setting::getKey)
        .collect(Collectors.toList());

    public static boolean isValidSetting(String name) {
        return isLoggingSetting(name) ||
               isArchivedSetting(name) ||
               EXPOSED_SETTING_NAMES.contains(name) ||
               EXPOSED_SETTING_NAMES.stream().noneMatch(s -> s.startsWith(name + ".")) == false;
    }

    public static List<String> settingNamesByPrefix(String prefix) {
        if (isLoggingSetting(prefix)) {
            return Collections.singletonList(prefix);
        }
        if (isArchivedSetting(prefix)) {
            return Collections.singletonList(prefix);
        }
        List<String> filteredList = new ArrayList<>();
        for (String key : EXPOSED_SETTING_NAMES) {
            if (key.startsWith(prefix)) {
                filteredList.add(key);
            }
        }
        return filteredList;
    }

    public static void checkIfRuntimeSetting(String name) {
        for (Setting<?> setting : EXPOSED_SETTINGS) {
            if (setting.getKey().equals(name) && setting.isDynamic() == false) {
                throw new UnsupportedOperationException(String.format(Locale.ENGLISH,
                    "Setting '%s' cannot be set/reset at runtime", name));
            }
        }
    }

    @SuppressWarnings("unchecked")
    public static void flattenSettings(Settings.Builder settingsBuilder,
                                       String key,
                                       Object value) {
        if (value instanceof Map<?, ?>) {
            for (Map.Entry<String, Object> setting : ((Map<String, Object>) value).entrySet()) {
                flattenSettings(
                    settingsBuilder,
                    String.join(".", key, setting.getKey()),
                    setting.getValue()
                );
            }
        } else if (value == null) {
            throw new IllegalArgumentException(
                "Cannot set \"" + key + "\" to `null`. Use `RESET [GLOBAL] \"" + key +
                "\"` to reset a setting to its default value");
        } else if (value instanceof List) {
            List<String> values = DataTypes.STRING_ARRAY.sanitizeValue(value);
            settingsBuilder.put(key, String.join(",", values));
        } else {
            settingsBuilder.put(key, value.toString());
        }
    }

    private static boolean isLoggingSetting(String name) {
        return name.startsWith(LOGGER_SETTINGS_PREFIX);
    }

    private static boolean isArchivedSetting(String name) {
        return name.startsWith(ARCHIVED_SETTINGS_PREFIX);
    }
}
