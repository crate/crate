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
import io.crate.analyze.expressions.ExpressionToNumberVisitor;
import io.crate.analyze.expressions.ExpressionToObjectVisitor;
import io.crate.analyze.expressions.ExpressionToStringVisitor;
import io.crate.core.NumberOfReplicas;
import io.crate.metadata.table.ColumnPolicy;
import io.crate.sql.tree.ArrayLiteral;
import io.crate.sql.tree.Expression;
import io.crate.sql.tree.GenericProperties;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;

import java.util.*;

public class TablePropertiesAnalyzer {

    private static final ImmutableBiMap<String, String> CRATE_TO_ES_SETTINGS_MAP =
            ImmutableBiMap.<String, String>builder()
                    .put(stripIndexPrefix(TableParameterInfo.NUMBER_OF_REPLICAS), TableParameterInfo.NUMBER_OF_REPLICAS)
                    .put(stripIndexPrefix(TableParameterInfo.REFRESH_INTERVAL), TableParameterInfo.REFRESH_INTERVAL)
                    .put(stripIndexPrefix(TableParameterInfo.NUMBER_OF_SHARDS), TableParameterInfo.NUMBER_OF_SHARDS)
                    .put("blobs_path", TableParameterInfo.BLOBS_PATH)
                    .build();

    private static final ImmutableBiMap<String, String> ES_TO_CRATE_SETTINGS_MAP =
            CRATE_TO_ES_SETTINGS_MAP.inverse();

    private static final ImmutableBiMap<String, String> CRATE_TO_ES_MAPPINGS_MAP =
            ImmutableBiMap.<String, String>builder()
                    .put("column_policy", TableParameterInfo.COLUMN_POLICY)
                    .build();

    private static final ImmutableBiMap<String, String> ES_TO_CRATE_MAPPINGS_MAP =
            CRATE_TO_ES_MAPPINGS_MAP.inverse();


    private static final ImmutableMap<String, SettingsApplier> SETTINGS_APPLIER =
            ImmutableMap.<String, SettingsApplier>builder()
                    .put(TableParameterInfo.NUMBER_OF_REPLICAS, new NumberOfReplicasSettingApplier())
                    .put(TableParameterInfo.REFRESH_INTERVAL, new RefreshIntervalSettingApplier())
                    .put(TableParameterInfo.NUMBER_OF_SHARDS, new NumberOfShardsSettingsApplier())
                    .put(TableParameterInfo.BLOBS_PATH, new BlobPathSettingApplier())
                    .build();

    private static final ImmutableMap<String, MappingsApplier> MAPPINGS_APPLIER =
            ImmutableMap.<String, MappingsApplier>builder()
                    .put(TableParameterInfo.COLUMN_POLICY, new ColumnPolicyMappingApplier())
                    .build();

    private static String stripIndexPrefix(String setting) {
        if (setting.startsWith("index.")) {
            return setting.substring("index.".length());
        }
        return setting;
    }

    public void analyze(TableParameter tableParameter,
                        TableParameterInfo tableParameterInfo,
                        Optional<GenericProperties> properties,
                        Object[] parameters) {
        analyze(tableParameter, tableParameterInfo, properties, parameters, false);
    }

    public void analyze(TableParameter tableParameter,
                        TableParameterInfo tableParameterInfo,
                        Optional<GenericProperties> properties,
                        Object[] parameters,
                        boolean withDefaults) {
        if (withDefaults) {
            for (String setting : tableParameterInfo.supportedSettings()) {
                SettingsApplier settingsApplier = SETTINGS_APPLIER.get(setting);
                tableParameter.settingsBuilder().put(settingsApplier.getDefault());
            }
            for (String mappingEntry : tableParameterInfo.supportedMappings()) {
                MappingsApplier mappingsApplier = MAPPINGS_APPLIER.get(mappingEntry);
                tableParameter.mappings().put(mappingsApplier.name, mappingsApplier.getDefault());
            }
        }
        if (properties.isPresent()) {
            Map<String, Expression> tableProperties = properties.get().properties();
            validateTableProperties(tableParameterInfo, tableProperties.keySet());

            for (String setting : tableParameterInfo.supportedSettings()) {
                String settingName = ES_TO_CRATE_SETTINGS_MAP.get(setting);
                if (tableProperties.containsKey(settingName)) {
                    SettingsApplier settingsApplier = SETTINGS_APPLIER.get(setting);
                    settingsApplier.apply(tableParameter.settingsBuilder(), parameters, tableProperties.get(settingName));
                }
            }
            for (String mappingEntry : tableParameterInfo.supportedMappings()) {
                String mappingName = ES_TO_CRATE_MAPPINGS_MAP.get(mappingEntry);
                if (tableProperties.containsKey(mappingName)) {
                    MappingsApplier mappingsApplier = MAPPINGS_APPLIER.get(mappingEntry);
                    mappingsApplier.apply(tableParameter.mappings(), parameters, tableProperties.get(mappingName));
                }
            }
        }

    }

    public void analyze(TableParameter tableParameter,
                        TableParameterInfo tableParameterInfo,
                        List<String> properties) {
        validateTableProperties(tableParameterInfo, properties);

        for (String setting : tableParameterInfo.supportedSettings()) {
            String settingName = ES_TO_CRATE_SETTINGS_MAP.get(setting);
            if (properties.contains(settingName)) {
                SettingsApplier settingsApplier = SETTINGS_APPLIER.get(setting);
                tableParameter.settingsBuilder().put(settingsApplier.getDefault());
            }
        }
        for (String mappingEntry : tableParameterInfo.supportedMappings()) {
            String mappingName = ES_TO_CRATE_MAPPINGS_MAP.get(mappingEntry);
            if (properties.contains(mappingName)) {
                MappingsApplier mappingsApplier = MAPPINGS_APPLIER.get(mappingEntry);
                tableParameter.mappings().put(mappingsApplier.name, mappingsApplier.getDefault());
            }
        }
    }

    private void validateTableProperties(TableParameterInfo tableParameterInfo, Collection<String> propertyNames) {
        List<String> supportedParameters = new ArrayList<>(tableParameterInfo.supportedSettings());
        supportedParameters.addAll(tableParameterInfo.supportedMappings());
        for (String propertyName : propertyNames) {
            String esName = CRATE_TO_ES_SETTINGS_MAP.get(propertyName);
            if (esName == null) {
                esName = CRATE_TO_ES_MAPPINGS_MAP.get(propertyName);
            }
            Preconditions.checkArgument(supportedParameters.contains(esName),
                    String.format(Locale.ENGLISH, "Invalid property \"%s\" passed to [ALTER | CREATE] TABLE statement",
                            propertyName));
        }
    }

    protected static class NumberOfReplicasSettingApplier extends SettingsAppliers.AbstractSettingsApplier {

        private static final Settings DEFAULT = ImmutableSettings.builder()
                .put(TableParameterInfo.NUMBER_OF_REPLICAS, 1)
                .put(TableParameterInfo.AUTO_EXPAND_REPLICAS, false)
                .build();

        public NumberOfReplicasSettingApplier() {
            super(ES_TO_CRATE_SETTINGS_MAP.get(TableParameterInfo.NUMBER_OF_REPLICAS), DEFAULT);
        }

        @Override
        public void apply(ImmutableSettings.Builder settingsBuilder,
                          Object[] parameters,
                          Expression expression) {
            Preconditions.checkArgument(!(expression instanceof ArrayLiteral),
                    String.format("array literal not allowed for \"%s\"", ES_TO_CRATE_SETTINGS_MAP.get(TableParameterInfo.NUMBER_OF_REPLICAS)));

            NumberOfReplicas numberOfReplicas;
            try {
                Integer numReplicas = ExpressionToNumberVisitor.convert(expression, parameters).intValue();
                numberOfReplicas = new NumberOfReplicas(numReplicas);
            } catch (IllegalArgumentException e) {
                String numReplicas = ExpressionToObjectVisitor.convert(expression, parameters).toString();
                numberOfReplicas = new NumberOfReplicas(numReplicas);
            }

            // in case the number_of_replicas is changing from auto_expand to a fixed number -> disable auto expand
            settingsBuilder.put(TableParameterInfo.AUTO_EXPAND_REPLICAS, false);
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
                .put(TableParameterInfo.REFRESH_INTERVAL, 1000).build(); // ms

        private RefreshIntervalSettingApplier() {
            super(ES_TO_CRATE_SETTINGS_MAP.get(TableParameterInfo.REFRESH_INTERVAL), DEFAULT);
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
            settingsBuilder.put(TableParameterInfo.REFRESH_INTERVAL, refreshIntervalValue.toString());
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
                .put(TableParameterInfo.NUMBER_OF_SHARDS, 5).build();

        private NumberOfShardsSettingsApplier() {
            super(ES_TO_CRATE_SETTINGS_MAP.get(TableParameterInfo.NUMBER_OF_SHARDS), DEFAULT);
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

            settingsBuilder.put(TableParameterInfo.NUMBER_OF_SHARDS, numberOfShardsValue);
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
            super(ES_TO_CRATE_SETTINGS_MAP.get(TableParameterInfo.BLOBS_PATH), ImmutableSettings.EMPTY);
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
            settingsBuilder.put(TableParameterInfo.BLOBS_PATH, blobPath);
        }

        @Override
        public void applyValue(ImmutableSettings.Builder settingsBuilder, Object value) {
            throw new UnsupportedOperationException("Not supported");
        }
    }

    private static class ColumnPolicyMappingApplier extends MappingsApplier {

        private ColumnPolicyMappingApplier() {
            super(TableParameterInfo.COLUMN_POLICY,
                    ES_TO_CRATE_MAPPINGS_MAP.get(TableParameterInfo.COLUMN_POLICY),
                    true);
        }

        @Override
        public void apply(Map<String, Object> mappings,
                          Object[] parameters,
                          Expression expression) {
            ColumnPolicy policy;
            try {
                String policyName = ExpressionToStringVisitor.convert(expression, parameters);
                policy = ColumnPolicy.byName(policyName);
            } catch (IllegalArgumentException e) {
                throw invalidException(e);
            }
            applyValue(mappings, policy.mappingValue());
        }

        @Override
        public Object validate(Object value) {
            if (value == ColumnPolicy.IGNORED.mappingValue()) {
                throw invalidException();
            }

            return value;
        }
    }

}
