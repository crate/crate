/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.analyze;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableMap;
import io.crate.core.NumberOfReplicas;
import io.crate.sql.tree.ArrayLiteral;
import io.crate.sql.tree.Expression;
import io.crate.sql.tree.GenericProperties;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;

import java.util.List;
import java.util.Locale;
import java.util.Map;

public class TablePropertiesAnalyzer {

    private static final ImmutableBiMap<String, String> CRATE_TO_ES_SETTINGS_MAP =
            ImmutableBiMap.<String, String>builder()
                    .put(stripIndexPrefix(TableSettingsInfo.NUMBER_OF_REPLICAS), TableSettingsInfo.NUMBER_OF_REPLICAS)
                    .put(stripIndexPrefix(TableSettingsInfo.REFRESH_INTERVAL), TableSettingsInfo.REFRESH_INTERVAL)
                    .put(stripIndexPrefix(TableSettingsInfo.NUMBER_OF_SHARDS), TableSettingsInfo.NUMBER_OF_SHARDS)
                    .put("blobs_path", TableSettingsInfo.BLOBS_PATH)
                    .build();

    private static final ImmutableBiMap<String, String> ES_TO_CRATE_SETTINGS_MAP =
            CRATE_TO_ES_SETTINGS_MAP.inverse();

    private static final ImmutableMap<String, SettingsApplier> SETTINGS_APPLIER =
            ImmutableMap.<String, SettingsApplier>builder()
                    .put(TableSettingsInfo.NUMBER_OF_REPLICAS, new NumberOfReplicasSettingApplier())
                    .put(TableSettingsInfo.REFRESH_INTERVAL, new RefreshIntervalSettingApplier())
                    .put(TableSettingsInfo.NUMBER_OF_SHARDS, new NumberOfShardsSettingsApplier())
                    .put(TableSettingsInfo.BLOBS_PATH, new BlobPathSettingApplier())
                    .build();

    private static String stripIndexPrefix(String setting) {
        if (setting.startsWith("index.")) {
            return setting.substring("index.".length());
        }
        return setting;
    }

    public void analyze(TableSettings tableSettings,
                        TableSettingsInfo tableSettingsInfo,
                        Optional<GenericProperties> properties,
                        Object[] parameters) {
        analyze(tableSettings, tableSettingsInfo, properties, parameters, false);
    }

    public void analyze(TableSettings tableSettings,
                        TableSettingsInfo tableSettingsInfo,
                        Optional<GenericProperties> properties,
                        Object[] parameters,
                        boolean withDefaults) {
        if (withDefaults) {
            for (String setting : tableSettingsInfo.supportedSettings()) {
                SettingsApplier defaultSetting = SETTINGS_APPLIER.get(setting);
                tableSettings.settingsBuilder().put(defaultSetting.getDefault());
            }
        }
        if (properties.isPresent()) {
            for (Map.Entry<String, Expression> entry : properties.get().properties().entrySet()) {
                SettingsApplier settingsApplier = resolveSettingsApplier(tableSettingsInfo, entry.getKey());
                settingsApplier.apply(tableSettings.settingsBuilder(), parameters, entry.getValue());
            }
        }

    }

    public void analyze(TableSettings tableSettings,
                        TableSettingsInfo tableSettingsInfo,
                        List<String> properties) {
        for (String property : properties) {
            SettingsApplier settingsApplier = resolveSettingsApplier(tableSettingsInfo, property);
            tableSettings.settingsBuilder().put(settingsApplier.getDefault());
        }
    }

    private SettingsApplier resolveSettingsApplier(TableSettingsInfo tableSettingsInfo, String setting) {
        String esSettingName = CRATE_TO_ES_SETTINGS_MAP.get(setting);
        Preconditions.checkArgument(tableSettingsInfo.supportedSettings().contains(esSettingName),
                String.format(Locale.ENGLISH, "Invalid property \"%s\" passed to [ALTER | CREATE] TABLE statement",
                        setting));
        assert SETTINGS_APPLIER.containsKey(esSettingName) : "Missing settings applier for setting " + esSettingName;
        return SETTINGS_APPLIER.get(esSettingName);
    }


    protected static class NumberOfReplicasSettingApplier extends SettingsAppliers.AbstractSettingsApplier {

        private static final Settings DEFAULT = ImmutableSettings.builder()
                .put(TableSettingsInfo.NUMBER_OF_REPLICAS, 1)
                .put(TableSettingsInfo.AUTO_EXPAND_REPLICAS, false)
                .build();

        public NumberOfReplicasSettingApplier() {
            super(ES_TO_CRATE_SETTINGS_MAP.get(TableSettingsInfo.NUMBER_OF_REPLICAS), DEFAULT);
        }

        @Override
        public void apply(ImmutableSettings.Builder settingsBuilder,
                          Object[] parameters,
                          Expression expression) {
            Preconditions.checkArgument(!(expression instanceof ArrayLiteral),
                    String.format("array literal not allowed for \"%s\"", ES_TO_CRATE_SETTINGS_MAP.get(TableSettingsInfo.NUMBER_OF_REPLICAS)));

            NumberOfReplicas numberOfReplicas;
            try {
                Integer numReplicas = ExpressionToNumberVisitor.convert(expression, parameters).intValue();
                numberOfReplicas = new NumberOfReplicas(numReplicas);
            } catch (IllegalArgumentException e) {
                String numReplicas = ExpressionToObjectVisitor.convert(expression, parameters).toString();
                numberOfReplicas = new NumberOfReplicas(numReplicas);
            }

            // in case the number_of_replicas is changing from auto_expand to a fixed number -> disable auto expand
            settingsBuilder.put(TableSettingsInfo.AUTO_EXPAND_REPLICAS, false);
            settingsBuilder.put(numberOfReplicas.esSettingKey(), numberOfReplicas.esSettingValue());
        }

        @Override
        public Settings getDefault() {
            return DEFAULT;
        }

        @Override
        public void applyValue(ImmutableSettings.Builder settingsBuilder, Object value) {
            throw new UnsupportedOperationException("Not supported");
        }
    }

    private static class RefreshIntervalSettingApplier extends SettingsAppliers.AbstractSettingsApplier {

        public static final Settings DEFAULT = ImmutableSettings.builder()
                .put(TableSettingsInfo.REFRESH_INTERVAL, 1000).build(); // ms

        private RefreshIntervalSettingApplier() {
            super(ES_TO_CRATE_SETTINGS_MAP.get(TableSettingsInfo.REFRESH_INTERVAL), DEFAULT);
        }

        @Override
        public void apply(ImmutableSettings.Builder settingsBuilder,
                          Object[] parameters,
                          Expression expression) {
            Number refreshIntervalValue;
            try {
                refreshIntervalValue = ExpressionToNumberVisitor.convert(expression, parameters);
            } catch (IllegalArgumentException e) {
                throw invalidException(e);
            }
            settingsBuilder.put(TableSettingsInfo.REFRESH_INTERVAL, refreshIntervalValue.toString());
        }

        @Override
        public Settings getDefault() {
            return DEFAULT;
        }

        @Override
        public void applyValue(ImmutableSettings.Builder settingsBuilder, Object value) {
            throw new UnsupportedOperationException("Not supported");
        }
    }

    private static class NumberOfShardsSettingsApplier extends SettingsAppliers.AbstractSettingsApplier {

        public static final Settings DEFAULT = ImmutableSettings.builder()
                .put(TableSettingsInfo.NUMBER_OF_SHARDS, 5).build();

        private NumberOfShardsSettingsApplier() {
            super(ES_TO_CRATE_SETTINGS_MAP.get(TableSettingsInfo.NUMBER_OF_SHARDS), DEFAULT);
        }

        @Override
        public void apply(ImmutableSettings.Builder settingsBuilder,
                          Object[] parameters,
                          Expression expression) {
            int numberOfShardsValue = 0;
            try {
                numberOfShardsValue = ExpressionToNumberVisitor.convert(expression, parameters).intValue();
            } catch (IllegalArgumentException e) {
                throw invalidException(e);
            }
            if (numberOfShardsValue < 1) {
                throw new IllegalArgumentException(String.format("%s must be greater than 0", name));
            }

            settingsBuilder.put(TableSettingsInfo.NUMBER_OF_SHARDS, numberOfShardsValue);
        }

        @Override
        public void applyValue(ImmutableSettings.Builder settingsBuilder, Object value) {
            throw new UnsupportedOperationException("Not supported");
        }

        @Override
        public Settings getDefault() {
            return DEFAULT;
        }
    }

    private static class BlobPathSettingApplier extends SettingsAppliers.AbstractSettingsApplier {

        private BlobPathSettingApplier() {
            super(ES_TO_CRATE_SETTINGS_MAP.get(TableSettingsInfo.BLOBS_PATH), ImmutableSettings.EMPTY);
        }

        @Override
        public void apply(ImmutableSettings.Builder settingsBuilder,
                          Object[] parameters,
                          Expression expression) {
            String blobPath;
            try {
                blobPath = SafeExpressionToStringVisitor.convert(expression, parameters);
            } catch (IllegalArgumentException e) {
                throw invalidException(e);
            }
            settingsBuilder.put(TableSettingsInfo.BLOBS_PATH, blobPath);
        }

        @Override
        public void applyValue(ImmutableSettings.Builder settingsBuilder, Object value) {
            throw new UnsupportedOperationException("Not supported");
        }
    }

}
