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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.crate.core.NumberOfReplicas;
import io.crate.core.StringUtils;
import io.crate.metadata.table.ColumnPolicy;
import io.crate.sql.tree.ArrayLiteral;
import io.crate.sql.tree.Expression;
import io.crate.sql.tree.GenericProperties;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.service.InternalIndexShard;

import javax.annotation.Nullable;
import java.util.Locale;
import java.util.Map;

public class TablePropertiesAnalysis {

    public final static String NUMBER_OF_REPLICAS = IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
    public final static String AUTO_EXPAND_REPLICAS = IndexMetaData.SETTING_AUTO_EXPAND_REPLICAS;
    public final static String REFRESH_INTERVAL = InternalIndexShard.INDEX_REFRESH_INTERVAL;
    public final static String COLUMN_POLICY = "column_policy";

    public static class TableProperties {
        private final Settings settings;
        private final Optional<ColumnPolicy> columnPolicy;

        public TableProperties(Settings settings, @Nullable ColumnPolicy columnPolicy) {
            this.settings = settings;
            this.columnPolicy = Optional.fromNullable(columnPolicy);
        }

        public Settings settings() {
            return settings;
        }

        public Optional<ColumnPolicy> columnPolicy() {
            return columnPolicy;
        }
    }

    private static final ImmutableMap<String, SettingsApplier> supportedProperties =
            ImmutableMap.<String, SettingsApplier>builder()
                    .put(NUMBER_OF_REPLICAS, new NumberOfReplicasSettingApplier())
                    .put(REFRESH_INTERVAL, new RefreshIntervalSettingApplier())
                    .build();

    private static final ImmutableSet<String> IGNORED_PROPERTIES = ImmutableSet.of(COLUMN_POLICY);

    protected ImmutableMap<String, SettingsApplier> supportedProperties() {
        return supportedProperties;
    }

    public TableProperties tableProperties(GenericProperties properties, Object[] parameters) {
        return tableProperties(properties, parameters, false);
    }

    public TableProperties tableProperties(GenericProperties properties, Object[] parameters, boolean withDefaults) {
        ImmutableSettings.Builder builder = ImmutableSettings.builder();
        if (withDefaults) {
            for (SettingsApplier defaultSetting : supportedProperties().values()) {
                builder.put(defaultSetting.getDefault());
            }
        }
        for (Map.Entry<String, Expression> entry : properties.properties().entrySet()) {
            SettingsApplier settingsApplier = supportedProperties().get(normalizeKey(entry.getKey()));
            if (settingsApplier == null) {
                if (IGNORED_PROPERTIES.contains(entry.getKey())) {
                    continue;
                }
                throw new IllegalArgumentException(
                        String.format(Locale.ENGLISH, "Invalid property \"%s\" passed to [ALTER | CREATE] TABLE statement",
                                entry.getKey()));
            }
            settingsApplier.apply(builder, parameters, entry.getValue());
        }
        return new TableProperties(builder.build(), columnPolicy(properties, parameters, withDefaults));
    }

    public Settings getDefault(String property) {
        String normalizedKey = normalizeKey(property);

        Preconditions.checkArgument(supportedProperties().containsKey(normalizedKey),
                "TABLE doesn't have a property \"%s\"", property);

        return supportedProperties.get(normalizedKey).getDefault();
    }

    public static String normalizeKey(String property) {
        if (!property.startsWith("index.")) {
            return StringUtils.PATH_JOINER.join("index", property);
        }
        return property;
    }

    public static String denormalizeKey(String property) {
        if (property.startsWith("index.")) {
            return property.substring("index.".length());
        }
        return property;
    }

    protected ColumnPolicy defaultColumnPolicy() {
        return ColumnPolicy.DYNAMIC;
    }

    public @Nullable ColumnPolicy columnPolicy(GenericProperties properties, Object[] parameters, boolean withDefaults) {
        Expression expression = properties.get(COLUMN_POLICY);
        ColumnPolicy policy;
        if (expression == null) {
            policy = withDefaults ? defaultColumnPolicy() : null;
        } else {
            String policyName;
            try {
                policyName = ExpressionToStringVisitor.convert(expression, parameters);
                policy = ColumnPolicy.byName(policyName);
            } catch (IllegalArgumentException e) {
                throw invalidValueError(COLUMN_POLICY, e);
            }
            if (policy == ColumnPolicy.IGNORED) {
                throw invalidValueError(COLUMN_POLICY, null);
            }
        }
        return policy;
    }


    protected static class NumberOfReplicasSettingApplier implements SettingsApplier {

        private static final Settings DEFAULT = ImmutableSettings.builder()
                .put(NUMBER_OF_REPLICAS, 1)
                .put(AUTO_EXPAND_REPLICAS, false)
                .build();

        @Override
        public void apply(ImmutableSettings.Builder settingsBuilder,
                          Object[] parameters,
                          Expression expression) {
            Preconditions.checkArgument(!(expression instanceof ArrayLiteral),
                    String.format("array literal not allowed for \"%s\"", denormalizeKey(NUMBER_OF_REPLICAS)));

            NumberOfReplicas numberOfReplicas;
            try {
                Integer numReplicas = ExpressionToNumberVisitor.convert(expression, parameters).intValue();
                numberOfReplicas = new NumberOfReplicas(numReplicas);
            } catch (IllegalArgumentException e) {
                String numReplicas = ExpressionToObjectVisitor.convert(expression, parameters).toString();
                numberOfReplicas = new NumberOfReplicas(numReplicas);
            }

            // in case the number_of_replicas is changing from auto_expand to a fixed number -> disable auto expand
            settingsBuilder.put(AUTO_EXPAND_REPLICAS, false);
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

    private static class RefreshIntervalSettingApplier implements SettingsApplier {

        public static final Settings DEFAULT = ImmutableSettings.builder()
                .put(REFRESH_INTERVAL, 1000).build(); // ms

        @Override
        public void apply(ImmutableSettings.Builder settingsBuilder,
                          Object[] parameters,
                          Expression expression) {
            Number refreshIntervalValue;
            try {
                refreshIntervalValue = ExpressionToNumberVisitor.convert(expression, parameters);
            } catch (IllegalArgumentException e) {
                throw invalidValueError(denormalizeKey(REFRESH_INTERVAL), e);
            }
            settingsBuilder.put(REFRESH_INTERVAL, refreshIntervalValue.toString());
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

    private static RuntimeException invalidValueError(String setting, @Nullable Throwable cause) {
        String message = String.format(Locale.ENGLISH,
                "Invalid value for argument '%s'", setting);
        if (cause == null) {
            return new IllegalArgumentException(message);
        } else {
            return new IllegalArgumentException(message, cause);
        }
    }
 }
