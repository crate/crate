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
import io.crate.metadata.TableIdent;
import io.crate.metadata.table.SchemaInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.operation.reference.sys.cluster.ClusterSettingsExpression;
import org.elasticsearch.common.settings.Settings;

import javax.annotation.Nullable;
import java.util.Map;

public class SetAnalysis extends Analysis {

    public static Map<String, SettingsApplier> SUPPORTED_SETTINGS = ImmutableMap.<String, SettingsApplier>builder()
        .put(
                ClusterSettingsExpression.SETTING_JOBS_LOG_SIZE,
                new SettingsAppliers.PositiveLongSettingsApplier(ClusterSettingsExpression.SETTING_JOBS_LOG_SIZE, new Integer(Integer.MAX_VALUE).longValue(), 0L)
        )
        .put(
            ClusterSettingsExpression.SETTING_OPERATIONS_LOG_SIZE,
            new SettingsAppliers.PositiveLongSettingsApplier(ClusterSettingsExpression.SETTING_OPERATIONS_LOG_SIZE, new Integer(Integer.MAX_VALUE).longValue(), 0L)
        )
        .put(ClusterSettingsExpression.SETTING_COLLECT_STATS,
                new SettingsAppliers.BooleanSettingsApplier(ClusterSettingsExpression.SETTING_COLLECT_STATS, true))
        .build();

    private Settings settings;
    private boolean persistent = false;

    protected SetAnalysis(Object[] parameters) {
        super(parameters);
    }

    public Settings settings() {
        return settings;
    }

    public void settings(Settings settings) {
        this.settings = settings;
    }

    public boolean isPersistent() {
        return persistent;
    }

    public boolean isTransient() {
        return !persistent;
    }

    public void persistent(boolean persistent) {
        this.persistent = persistent;
    }

    public @Nullable SettingsApplier getSetting(String name) {
        return SUPPORTED_SETTINGS.get(name);
    }

    @Override
    public void table(TableIdent tableIdent) {
        throw new UnsupportedOperationException();
    }

    @Override
    public TableInfo table() {
        throw new UnsupportedOperationException();
    }

    @Override
    public SchemaInfo schema() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean hasNoResult() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void normalize() {

    }

    @Override
    public <C, R> R accept(AnalysisVisitor<C, R> analysisVisitor, C context) {
        return analysisVisitor.visitSetAnalysis(this, context);
    }
}
