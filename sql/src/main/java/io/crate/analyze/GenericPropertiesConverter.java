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

import io.crate.analyze.expressions.ExpressionToStringVisitor;
import io.crate.data.Row;
import io.crate.metadata.settings.SettingsApplier;
import io.crate.sql.tree.ArrayLiteral;
import io.crate.sql.tree.Expression;
import io.crate.sql.tree.GenericProperties;
import org.elasticsearch.common.settings.Settings;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

public class GenericPropertiesConverter {


    /**
     * Put a genericProperty into a settings-structure
     */
    static void genericPropertyToSetting(Settings.Builder builder,
                                         String name,
                                         Expression value,
                                         Row parameters) {
        if (value instanceof ArrayLiteral) {
            ArrayLiteral array = (ArrayLiteral) value;
            List<String> values = new ArrayList<>(array.values().size());
            for (Expression expression : array.values()) {
                values.add(ExpressionToStringVisitor.convert(expression, parameters));
            }
            builder.putArray(name, values.toArray(new String[values.size()]));
        } else {
            builder.put(name, ExpressionToStringVisitor.convert(value, parameters));
        }
    }

    private static void genericPropertiesToSettings(Settings.Builder builder,
                                                    GenericProperties genericProperties,
                                                    Row parameters) {
        for (Map.Entry<String, Expression> entry : genericProperties.properties().entrySet()) {
            genericPropertyToSetting(builder, entry.getKey(), entry.getValue(), parameters);
        }
    }

    static Settings genericPropertiesToSettings(GenericProperties genericProperties, Row parameters) {
        Settings.Builder builder = Settings.builder();
        genericPropertiesToSettings(builder, genericProperties, parameters);
        return builder.build();
    }

    public static Settings.Builder settingsFromProperties(Optional<GenericProperties> properties,
                                                          ParameterContext parameterContext,
                                                          Map<String, ? extends SettingsApplier> settingAppliers) {
        Settings.Builder builder = Settings.builder();
        setDefaults(settingAppliers, builder);
        if (properties.isPresent()) {
            for (Map.Entry<String, Expression> entry : properties.get().properties().entrySet()) {
                SettingsApplier settingsApplier = settingAppliers.get(entry.getKey());
                if (settingsApplier == null) {
                    throw new IllegalArgumentException(String.format(Locale.ENGLISH, "setting '%s' not supported", entry.getKey()));
                }
                settingsApplier.apply(builder, parameterContext.parameters(), entry.getValue());
            }
        }
        return builder;
    }

    private static void setDefaults(Map<String, ? extends SettingsApplier> settingAppliers, Settings.Builder builder) {
        for (Map.Entry<String, ? extends SettingsApplier> entry : settingAppliers.entrySet()) {
            builder.put(entry.getValue().getDefault());
        }
    }
}
