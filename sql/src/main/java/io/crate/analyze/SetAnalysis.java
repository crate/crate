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

package io.crate.analyze;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import io.crate.metadata.TableIdent;
import io.crate.metadata.settings.CrateSettings;
import io.crate.metadata.table.SchemaInfo;
import io.crate.metadata.table.TableInfo;
import org.elasticsearch.common.settings.Settings;

import javax.annotation.Nullable;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

public class SetAnalysis extends Analysis {

    public static final Map<String, SettingsApplier> SUPPORTED_SETTINGS = ImmutableMap.<String, SettingsApplier>builder()
        .put(CrateSettings.JOBS_LOG_SIZE.settingName(),
                new SettingsAppliers.IntSettingsApplier(CrateSettings.JOBS_LOG_SIZE))
        .put(CrateSettings.OPERATIONS_LOG_SIZE.settingName(),
                new SettingsAppliers.IntSettingsApplier(CrateSettings.OPERATIONS_LOG_SIZE))
        .put(CrateSettings.COLLECT_STATS.settingName(),
                new SettingsAppliers.BooleanSettingsApplier(CrateSettings.COLLECT_STATS))
        .put(CrateSettings.GRACEFUL_STOP.settingName(),
                new SettingsAppliers.ObjectSettingsApplier(CrateSettings.GRACEFUL_STOP))
        .put(CrateSettings.GRACEFUL_STOP_MIN_AVAILABILITY.settingName(),
                new SettingsAppliers.StringSettingsApplier(CrateSettings.GRACEFUL_STOP_MIN_AVAILABILITY))
        .put(CrateSettings.GRACEFUL_STOP_REALLOCATE.settingName(),
                new SettingsAppliers.BooleanSettingsApplier(CrateSettings.GRACEFUL_STOP_REALLOCATE))
        .put(CrateSettings.GRACEFUL_STOP_FORCE.settingName(),
                new SettingsAppliers.BooleanSettingsApplier(CrateSettings.GRACEFUL_STOP_FORCE))
        .put(CrateSettings.GRACEFUL_STOP_TIMEOUT.settingName(),
                new SettingsAppliers.TimeSettingsApplier(CrateSettings.GRACEFUL_STOP_TIMEOUT))
        .put(CrateSettings.GRACEFUL_STOP_IS_DEFAULT.settingName(),
                new SettingsAppliers.BooleanSettingsApplier(CrateSettings.GRACEFUL_STOP_IS_DEFAULT))
        .put(CrateSettings.ROUTING.settingName(),
                new SettingsAppliers.ObjectSettingsApplier(CrateSettings.ROUTING))
        .put(CrateSettings.ROUTING_ALLOCATION.settingName(),
                new SettingsAppliers.ObjectSettingsApplier(CrateSettings.ROUTING_ALLOCATION))
        .put(CrateSettings.ROUTING_ALLOCATION_ENABLE.settingName(),
                new SettingsAppliers.StringSettingsApplier(CrateSettings.ROUTING_ALLOCATION_ENABLE))
            .build();

    private Settings settings;
    private Set<String> settingsToRemove;
    private boolean persistent = false;
    private boolean isReset = false;

    protected SetAnalysis(Analyzer.ParameterContext parameterContext) {
        super(parameterContext);
    }

    public Settings settings() {
        return settings;
    }

    public void settings(Settings settings) {
        this.settings = settings;
    }

    @Nullable
    public Set<String> settingsToRemove() {
        return settingsToRemove;
    }

    public void settingsToRemove(Set<String> settingsToRemove) {
        this.settingsToRemove = settingsToRemove;
    }

    public boolean isPersistent() {
        return persistent;
    }

    public boolean isTransient() {
        return !persistent;
    }

    public boolean isReset() {
        return isReset;
    }

    public void isReset(boolean isReset) {
        this.isReset = isReset;
    }

    public void persistent(boolean persistent) {
        this.persistent = persistent;
    }

    public @Nullable SettingsApplier getSetting(String name) {
        return SUPPORTED_SETTINGS.get(name);
    }

    public Set<String> settingNamesByPrefix(String prefix) {
        Set<String> settingNames = Sets.newHashSet();
        if (SUPPORTED_SETTINGS.containsKey(prefix)) {
            settingNames.add(prefix);
        }
        prefix += ".";
        for (String name : SUPPORTED_SETTINGS.keySet()) {
            if (name.startsWith(prefix)) {
                settingNames.add(name);
            }
        }
        return settingNames;
    }

    @Override
    public void table(TableIdent tableIdent) {
        throw new UnsupportedOperationException(
                String.format(Locale.ENGLISH, "table() not supported on %s", getClass().getSimpleName())
        );
    }

    @Override
    public TableInfo table() {
        throw new UnsupportedOperationException(
                String.format(Locale.ENGLISH, "table() not supported on %s", getClass().getSimpleName()));
    }

    @Override
    public SchemaInfo schema() {
        throw new UnsupportedOperationException(
                String.format(Locale.ENGLISH, "schema() not supported on %s", getClass().getSimpleName())
        );
    }

    @Override
    public boolean hasNoResult() {
        if (settings != null) {
            return settings.getAsMap().isEmpty();
        }
        if (settingsToRemove != null) {
            return settingsToRemove.size() == 0;
        }
        return true;
    }

    @Override
    public void normalize() {

    }

    @Override
    public <C, R> R accept(AnalysisVisitor<C, R> analysisVisitor, C context) {
        return analysisVisitor.visitSetAnalysis(this, context);
    }
}
