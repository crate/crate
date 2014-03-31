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
import io.crate.sql.tree.Expression;
import io.crate.sql.tree.GenericProperties;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;

import java.util.List;
import java.util.Locale;
import java.util.Map;

public class TablePropertiesAnalysis {

    private final static String NUMBER_OF_REPLICAS = "number_of_replicas";
    private final static String AUTO_EXPAND_REPLICAS = "auto_expand_replicas";
    public final static String REFRESH_INTERVAL = "refresh_interval";

    private static final ExpressionToObjectVisitor expressionVisitor = new ExpressionToObjectVisitor();

    private static final ImmutableMap<String, SettingsApplier> supportedProperties =
            ImmutableMap.<String, SettingsApplier>builder()
                    .put(NUMBER_OF_REPLICAS, new NumberOfReplicasSettingApplier())
                    .put(REFRESH_INTERVAL, new RefreshIntervalSettingApplier())
                    .build();

    private static final ImmutableMap<String, Object> defaultValues = ImmutableMap.<String, Object>builder()
            .put(NUMBER_OF_REPLICAS, 1)
            .put(REFRESH_INTERVAL, 1000) // ms
            .build();

    public static Settings propertiesToSettings(GenericProperties properties, Object[] parameters) {
        ImmutableSettings.Builder builder = ImmutableSettings.builder();

        for (Map.Entry<String, List<Expression>> entry : properties.properties().entrySet()) {
            SettingsApplier settingsApplier = supportedProperties.get(entry.getKey());
            if (settingsApplier == null) {
                throw new IllegalArgumentException(
                        String.format(Locale.ENGLISH, "TABLES don't have the \"%s\" property", entry.getKey()));
            }

            settingsApplier.apply(builder, parameters, entry.getValue());
        }

        return builder.build();
    }

    public static Object getDefault(String property) {
        Preconditions.checkArgument(defaultValues.containsKey(property),
                "TABLE doesn't have a property \"%s\"", property);

        return defaultValues.get(property);
    }


    private static class NumberOfReplicasSettingApplier implements SettingsApplier {

        @Override
        public void apply(ImmutableSettings.Builder settingsBuilder,
                          Object[] parameters,
                          List<Expression> expressions) {
            Preconditions.checkArgument(expressions.size() == 1,
                    String.format("Invalid number of arguments passed to \"%s\"", NUMBER_OF_REPLICAS));

            Object numReplicas = expressionVisitor.process(expressions.get(0), parameters);

            NumberOfReplicas numberOfReplicas = new NumberOfReplicas(numReplicas.toString());

            // in case the number_of_replicas is changing from auto_expand to a fixed number -> disable auto expand
            settingsBuilder.put(AUTO_EXPAND_REPLICAS, false);
            settingsBuilder.put(numberOfReplicas.esSettingKey(), numberOfReplicas.esSettingValue());
        }
    }

    private static class RefreshIntervalSettingApplier implements SettingsApplier {


        @Override
        public void apply(ImmutableSettings.Builder settingsBuilder,
                          Object[] parameters,
                          List<Expression> expressions) {
            Preconditions.checkArgument(expressions.size() == 1,
                    String.format("Invalid number of arguments passed to \"%s\"", REFRESH_INTERVAL));

            Object refreshIntervalValue = expressionVisitor.process(expressions.get(0), parameters);
            try {
                Long.parseLong(refreshIntervalValue.toString());
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Invalid value for argument '"
                        + REFRESH_INTERVAL + "'");
            }
            settingsBuilder.put(REFRESH_INTERVAL, refreshIntervalValue.toString());
        }
    }
}
