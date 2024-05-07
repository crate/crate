/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.metadata.settings;

import static io.crate.analyze.TableParameters.stripIndexPrefix;

import java.util.function.Function;

import org.elasticsearch.cluster.metadata.AutoExpandReplicas;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;

import io.crate.analyze.NumberOfReplicas;
import io.crate.types.DataTypes;

public class NumberOfReplicasSetting extends Setting<Settings> {

    public static final String NAME = IndexMetadata.SETTING_NUMBER_OF_REPLICAS;

    private static final Settings DEFAULT = Settings.builder()
        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
        .put(AutoExpandReplicas.SETTING_KEY, AutoExpandReplicas.SETTING.getDefaultRaw(Settings.EMPTY))
        .build();

    public NumberOfReplicasSetting() {
        this(new SimpleKey(NAME), (s) -> "0-1", NumberOfReplicasSetting::parseValue, Property.ReplicatedIndexScope);
    }

    private NumberOfReplicasSetting(Key key, Function<Settings, String> defaultValue, Function<String, Settings> parser, Property... properties) {
        super(key, defaultValue, parser, DataTypes.STRING, properties);
    }

    private static Settings parseValue(String val) {
        Settings.Builder builder = Settings.builder();

        NumberOfReplicas numberOfReplicas;
        try {
            Integer numReplicas = parseInt(val, 0, stripIndexPrefix(NAME));
            numberOfReplicas = new NumberOfReplicas(numReplicas);
        } catch (NumberFormatException e) {
            numberOfReplicas = new NumberOfReplicas(val);
        }

        // in case the number_of_replicas is changing from auto_expand to a fixed number -> disable auto expand
        builder.put(AutoExpandReplicas.SETTING.getKey(), false);
        builder.put(numberOfReplicas.esSettingKey(), numberOfReplicas.esSettingValue());
        return builder.build();
    }

    @Override
    public Settings getDefault(Settings settings) {
        return DEFAULT;
    }
}
