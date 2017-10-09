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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.crate.analyze.expressions.ExpressionToNumberVisitor;
import io.crate.analyze.expressions.ExpressionToObjectVisitor;
import io.crate.analyze.expressions.ExpressionToStringVisitor;
import io.crate.data.Row;
import io.crate.metadata.settings.CrateTableSettings;
import io.crate.metadata.settings.SettingsApplier;
import io.crate.metadata.settings.SettingsAppliers;
import io.crate.metadata.table.ColumnPolicy;
import io.crate.sql.tree.ArrayLiteral;
import io.crate.sql.tree.Expression;
import io.crate.sql.tree.GenericProperties;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

public final class TablePropertiesAnalyzer {

    private TablePropertiesAnalyzer() {
    }

    private static final ImmutableBiMap<String, String> CRATE_TO_ES_SETTINGS_MAP =
        ImmutableBiMap.<String, String>builder()
            .put(stripIndexPrefix(TableParameterInfo.NUMBER_OF_REPLICAS), TableParameterInfo.NUMBER_OF_REPLICAS)
            .put(stripIndexPrefix(TableParameterInfo.REFRESH_INTERVAL), TableParameterInfo.REFRESH_INTERVAL)
            .put(stripIndexPrefix(TableParameterInfo.READ_ONLY), TableParameterInfo.READ_ONLY)
            .put(stripIndexPrefix(TableParameterInfo.BLOCKS_READ), TableParameterInfo.BLOCKS_READ)
            .put(stripIndexPrefix(TableParameterInfo.BLOCKS_WRITE), TableParameterInfo.BLOCKS_WRITE)
            .put(stripIndexPrefix(TableParameterInfo.BLOCKS_METADATA), TableParameterInfo.BLOCKS_METADATA)
            .put(stripIndexPrefix(TableParameterInfo.FLUSH_THRESHOLD_SIZE), TableParameterInfo.FLUSH_THRESHOLD_SIZE)
            .put(stripIndexPrefix(TableParameterInfo.ROUTING_ALLOCATION_ENABLE), TableParameterInfo.ROUTING_ALLOCATION_ENABLE)
            .put(stripIndexPrefix(TableParameterInfo.TRANSLOG_SYNC_INTERVAL), TableParameterInfo.TRANSLOG_SYNC_INTERVAL)
            .put(stripIndexPrefix(TableParameterInfo.TRANSLOG_DURABILITY), TableParameterInfo.TRANSLOG_DURABILITY)
            .put(stripIndexPrefix(TableParameterInfo.TOTAL_SHARDS_PER_NODE), TableParameterInfo.TOTAL_SHARDS_PER_NODE)
            .put(stripIndexPrefix(TableParameterInfo.MAPPING_TOTAL_FIELDS_LIMIT), TableParameterInfo.MAPPING_TOTAL_FIELDS_LIMIT)
            .put(stripIndexPrefix(TableParameterInfo.RECOVERY_INITIAL_SHARDS), TableParameterInfo.RECOVERY_INITIAL_SHARDS)
            .put(stripIndexPrefix(TableParameterInfo.WARMER_ENABLED), TableParameterInfo.WARMER_ENABLED)
            .put(stripIndexPrefix(TableParameterInfo.UNASSIGNED_NODE_LEFT_DELAYED_TIMEOUT), TableParameterInfo.UNASSIGNED_NODE_LEFT_DELAYED_TIMEOUT)
            .put(stripIndexPrefix(TableParameterInfo.NUMBER_OF_SHARDS), TableParameterInfo.NUMBER_OF_SHARDS)
            .put(stripIndexPrefix(TableParameterInfo.SETTING_WAIT_FOR_ACTIVE_SHARDS), TableParameterInfo.SETTING_WAIT_FOR_ACTIVE_SHARDS)
            .put(stripIndexPrefix(TableParameterInfo.ALLOCATION_MAX_RETRIES), TableParameterInfo.ALLOCATION_MAX_RETRIES)
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
            .put(TableParameterInfo.READ_ONLY, new SettingsAppliers.BooleanSettingsApplier(CrateTableSettings.READ_ONLY))
            .put(TableParameterInfo.BLOCKS_READ, new SettingsAppliers.BooleanSettingsApplier(CrateTableSettings.BLOCKS_READ))
            .put(TableParameterInfo.BLOCKS_WRITE, new SettingsAppliers.BooleanSettingsApplier(CrateTableSettings.BLOCKS_WRITE))
            .put(TableParameterInfo.BLOCKS_METADATA, new SettingsAppliers.BooleanSettingsApplier(CrateTableSettings.BLOCKS_METADATA))
            .put(TableParameterInfo.FLUSH_THRESHOLD_SIZE, new SettingsAppliers.ByteSizeSettingsApplier(CrateTableSettings.FLUSH_THRESHOLD_SIZE))
            .put(TableParameterInfo.ROUTING_ALLOCATION_ENABLE, new SettingsAppliers.StringSettingsApplier(CrateTableSettings.ROUTING_ALLOCATION_ENABLE))
            .put(TableParameterInfo.TRANSLOG_SYNC_INTERVAL, new SettingsAppliers.TimeSettingsApplier(CrateTableSettings.TRANSLOG_SYNC_INTERVAL))
            .put(TableParameterInfo.TRANSLOG_DURABILITY, new SettingsAppliers.StringSettingsApplier(CrateTableSettings.TRANSLOG_DURABILITY))
            .put(TableParameterInfo.TOTAL_SHARDS_PER_NODE, new SettingsAppliers.IntSettingsApplier(CrateTableSettings.TOTAL_SHARDS_PER_NODE))
            .put(TableParameterInfo.MAPPING_TOTAL_FIELDS_LIMIT, new SettingsAppliers.IntSettingsApplier(CrateTableSettings.TOTAL_FIELDS_LIMIT))
            .put(TableParameterInfo.RECOVERY_INITIAL_SHARDS, new RecoveryInitialShardsApplier())
            .put(TableParameterInfo.WARMER_ENABLED, new SettingsAppliers.BooleanSettingsApplier(CrateTableSettings.WARMER_ENABLED))
            .put(TableParameterInfo.UNASSIGNED_NODE_LEFT_DELAYED_TIMEOUT, new SettingsAppliers.TimeSettingsApplier(CrateTableSettings.UNASSIGNED_NODE_LEFT_DELAYED_TIMEOUT))
            .put(TableParameterInfo.NUMBER_OF_SHARDS, new NumberOfShardsSettingsApplier())
            .put(TableParameterInfo.SETTING_WAIT_FOR_ACTIVE_SHARDS, new SettingsAppliers.StringSettingsApplier(CrateTableSettings.SETTING_WAIT_FOR_ACTIVE_SHARDS))
            .put(TableParameterInfo.ALLOCATION_MAX_RETRIES, new SettingsAppliers.IntSettingsApplier(CrateTableSettings.ALLOCATION_MAX_RETRIES))
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

    public static String esToCrateSettingName(String esSettingName) {
        String val = ES_TO_CRATE_SETTINGS_MAP.get(esSettingName);
        return (val == null) ? esSettingName : val;
    }

    public static void analyze(TableParameter tableParameter,
                               TableParameterInfo tableParameterInfo,
                               Optional<GenericProperties> properties,
                               Row parameters) {
        analyze(tableParameter, tableParameterInfo, properties, parameters, false);
    }

    public static void analyze(TableParameter tableParameter,
                               TableParameterInfo tableParameterInfo,
                               Optional<GenericProperties> properties,
                               Row parameters,
                               boolean withDefaults) {
        if (withDefaults) {
            SettingsApplier settingsApplier = SETTINGS_APPLIER.get(TableParameterInfo.NUMBER_OF_REPLICAS);
            tableParameter.settingsBuilder().put(settingsApplier.getDefault());
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

    public static void analyze(TableParameter tableParameter,
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

    private static void validateTableProperties(TableParameterInfo tableParameterInfo, Collection<String> propertyNames) {
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

        private static final Settings DEFAULT = Settings.builder()
            .put(TableParameterInfo.NUMBER_OF_REPLICAS, 0)
            .put(TableParameterInfo.AUTO_EXPAND_REPLICAS,
                IndexMetaData.INDEX_AUTO_EXPAND_REPLICAS_SETTING.getDefaultRaw(Settings.EMPTY))
            .put(TableParameterInfo.SETTING_WAIT_FOR_ACTIVE_SHARDS,
                IndexMetaData.SETTING_WAIT_FOR_ACTIVE_SHARDS.getDefaultRaw(Settings.EMPTY))
            .build();

        public NumberOfReplicasSettingApplier() {
            super(ES_TO_CRATE_SETTINGS_MAP.get(TableParameterInfo.NUMBER_OF_REPLICAS), DEFAULT);
        }

        @Override
        public void apply(Settings.Builder settingsBuilder,
                          Row parameters,
                          Expression expression) {
            Preconditions.checkArgument(!(expression instanceof ArrayLiteral),
                String.format(Locale.ENGLISH, "array literal not allowed for \"%s\"", ES_TO_CRATE_SETTINGS_MAP.get(TableParameterInfo.NUMBER_OF_REPLICAS)));

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
        public void applyValue(Settings.Builder settingsBuilder, Object value) {
            throw new UnsupportedOperationException("Not supported");
        }
    }

    private static class RefreshIntervalSettingApplier extends SettingsAppliers.AbstractSettingsApplier {

        public static final Settings DEFAULT = Settings.builder()
            .put(TableParameterInfo.REFRESH_INTERVAL,
                CrateTableSettings.REFRESH_INTERVAL.defaultValue().millis() + "ms").build();

        private RefreshIntervalSettingApplier() {
            super(ES_TO_CRATE_SETTINGS_MAP.get(TableParameterInfo.REFRESH_INTERVAL), DEFAULT);
        }

        @Override
        public void apply(Settings.Builder settingsBuilder,
                          Row parameters,
                          Expression expression) {
            Number refreshIntervalValue;
            try {
                refreshIntervalValue = ExpressionToNumberVisitor.convert(expression, parameters);
            } catch (IllegalArgumentException e) {
                throw invalidException(e);
            }
            settingsBuilder.put(TableParameterInfo.REFRESH_INTERVAL, refreshIntervalValue.toString() + "ms");
        }

        @Override
        public Settings getDefault() {
            return DEFAULT;
        }

        @Override
        public void applyValue(Settings.Builder settingsBuilder, Object value) {
            throw new UnsupportedOperationException("Not supported");
        }
    }

    private static class RecoveryInitialShardsApplier extends SettingsAppliers.AbstractSettingsApplier {

        public ImmutableSet<String> ALLOWED_VALUES = ImmutableSet.of(
            "quorum",
            "quorum-1",
            "full",
            "full-1",
            "half"
        );

        public static final Settings DEFAULT = Settings.builder()
            .put(TableParameterInfo.RECOVERY_INITIAL_SHARDS, CrateTableSettings.RECOVERY_INITIAL_SHARDS.defaultValue())
            .build();

        private RecoveryInitialShardsApplier() {
            super(ES_TO_CRATE_SETTINGS_MAP.get(TableParameterInfo.RECOVERY_INITIAL_SHARDS), DEFAULT);
        }

        @SuppressWarnings("SuspiciousMethodCalls")
        @Override
        public void apply(Settings.Builder settingsBuilder, Row parameters, Expression expression) {
            Object shardsRecoverySettings;
            try {
                shardsRecoverySettings = ExpressionToNumberVisitor.convert(expression, parameters).intValue();
            } catch (IllegalArgumentException e) {
                shardsRecoverySettings = ExpressionToObjectVisitor.convert(expression, parameters).toString();
                if (!ALLOWED_VALUES.contains(shardsRecoverySettings)) {
                    throw invalidException();
                }
            }
            settingsBuilder.put(TableParameterInfo.RECOVERY_INITIAL_SHARDS, shardsRecoverySettings);
        }
    }

    private static class NumberOfShardsSettingsApplier extends SettingsAppliers.AbstractSettingsApplier {

        public static final Settings DEFAULT = Settings.builder()
            .put(TableParameterInfo.NUMBER_OF_SHARDS, 5).build();

        private NumberOfShardsSettingsApplier() {
            super(ES_TO_CRATE_SETTINGS_MAP.get(TableParameterInfo.NUMBER_OF_SHARDS), DEFAULT);
        }

        @Override
        public void apply(Settings.Builder settingsBuilder,
                          Row parameters,
                          Expression expression) {
            int numberOfShardsValue = 0;
            try {
                numberOfShardsValue = ExpressionToNumberVisitor.convert(expression, parameters).intValue();
            } catch (IllegalArgumentException e) {
                throw invalidException(e);
            }
            if (numberOfShardsValue < 1) {
                throw new IllegalArgumentException(String.format(Locale.ENGLISH, "%s must be greater than 0", name));
            }

            settingsBuilder.put(TableParameterInfo.NUMBER_OF_SHARDS, numberOfShardsValue);
        }

        @Override
        public void applyValue(Settings.Builder settingsBuilder, Object value) {
            throw new UnsupportedOperationException("Not supported");
        }

        @Override
        public Settings getDefault() {
            return DEFAULT;
        }
    }

    private static class BlobPathSettingApplier extends SettingsAppliers.AbstractSettingsApplier {

        private BlobPathSettingApplier() {
            super(ES_TO_CRATE_SETTINGS_MAP.get(TableParameterInfo.BLOBS_PATH), Settings.EMPTY);
        }

        @Override
        public void apply(Settings.Builder settingsBuilder,
                          Row parameters,
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
        public void applyValue(Settings.Builder settingsBuilder, Object value) {
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
                          Row parameters,
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
