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
import com.google.common.collect.ImmutableMap;
import io.crate.core.NumberOfReplicas;
import io.crate.core.StringUtils;
import io.crate.sql.tree.ArrayLiteral;
import io.crate.sql.tree.Expression;
import io.crate.sql.tree.GenericProperties;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.service.InternalIndexShard;

import java.util.Locale;
import java.util.Map;

public class TablePropertiesAnalysis {

    public final static String NUMBER_OF_REPLICAS = IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
    public final static String AUTO_EXPAND_REPLICAS = IndexMetaData.SETTING_AUTO_EXPAND_REPLICAS;
    public final static String REFRESH_INTERVAL = InternalIndexShard.INDEX_REFRESH_INTERVAL;

    private static final ImmutableMap<String, SettingsApplier> supportedProperties =
            ImmutableMap.<String, SettingsApplier>builder()
                    .put(NUMBER_OF_REPLICAS, new NumberOfReplicasSettingApplier())
                    .put(REFRESH_INTERVAL, new RefreshIntervalSettingApplier())
                    .build();

    protected ImmutableMap<String, SettingsApplier> supportedProperties() {
        return supportedProperties;
    }

    public Settings propertiesToSettings(GenericProperties properties, Object[] parameters) {
        return propertiesToSettings(properties, parameters, false);
    }

    public Settings propertiesToSettings(GenericProperties properties, Object[] parameters, boolean withDefaults) {
        ImmutableSettings.Builder builder = ImmutableSettings.builder();
        if (withDefaults) {
            for (SettingsApplier defaultSetting : supportedProperties().values()) {
                builder.put(defaultSetting.getDefault());
            }
        }
        for (Map.Entry<String, Expression> entry : properties.properties().entrySet()) {
            SettingsApplier settingsApplier = supportedProperties().get(normalizeKey(entry.getKey()));
            if (settingsApplier == null) {
                throw new IllegalArgumentException(
                        String.format(Locale.ENGLISH, "Invalid property \"%s\" passed to [ALTER | CREATE] TABLE statement",
                                entry.getKey()));
            }

            settingsApplier.apply(builder, parameters, entry.getValue());
        }

        return builder.build();
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
            Preconditions.checkArgument(!(expression instanceof ArrayLiteral),
                    String.format("array literal not allowed for \"%s\"", denormalizeKey(NUMBER_OF_REPLICAS)));

            Number refreshIntervalValue;
            try {
                refreshIntervalValue = ExpressionToNumberVisitor.convert(expression, parameters);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("Invalid value for argument '"
                        + denormalizeKey(REFRESH_INTERVAL) + "'", e);
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
}
