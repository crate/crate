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
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public abstract class Setting<T, E> {

    private final static Joiner dotJoiner = Joiner.on(".");
    protected org.elasticsearch.common.settings.Setting<T> esSetting;

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
    private List<String> chain() {
        Setting parentSetting = parent();
        if (parentSetting == null) {
            return ImmutableList.of(name());
        }
        ImmutableList.Builder<String> builder = ImmutableList.builder();
        builder.add(name());
        while (parentSetting != null) {
            builder.add(parentSetting.name());
            parentSetting = parentSetting.parent();
        }
        return builder.build().reverse();

    }

    /**
     * Return the corresponding {@link org.elasticsearch.common.settings.Setting}
     */
    public final org.elasticsearch.common.settings.Setting<T> esSetting() {
        /**
         * Need to re-use esSettings because
         * {@link org.elasticsearch.common.settings.AbstractScopedSettings#addSettingsUpdateConsumer(org.elasticsearch.common.settings.Setting, Consumer)}
         * Does Identity comparison to verify that settings have been registered
         */
        if (esSetting == null) {
            esSetting = createESSetting();
        }
        return esSetting;
    }

    abstract org.elasticsearch.common.settings.Setting<T> createESSetting();

    /**
     * Register a settings consumer for this setting to {@link ClusterSettings}.
     * <p>
     * Note: Setting must be registered to {@link org.elasticsearch.cluster.ClusterModule}
     * at the {@link io.crate.plugin.SQLPlugin} to make it dynamically changeable.
     * </p>
     */
    public void registerUpdateConsumer(ClusterSettings clusterSettings, Consumer<T> consumer) {
        clusterSettings.addSettingsUpdateConsumer(esSetting(), consumer);
    }

    org.elasticsearch.common.settings.Setting.Property[] propertiesForUpdateConsumer() {
        List<org.elasticsearch.common.settings.Setting.Property> properties = new ArrayList<>();
        properties.add(org.elasticsearch.common.settings.Setting.Property.NodeScope);
        if (isRuntime()) {
            properties.add(org.elasticsearch.common.settings.Setting.Property.Dynamic);
        }
        return properties.toArray(new org.elasticsearch.common.settings.Setting.Property[0]);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{" + settingName() + "}";
    }
}
