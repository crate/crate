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

import java.util.Objects;
import java.util.regex.Pattern;

import org.elasticsearch.cluster.metadata.AutoExpandReplicas;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;

import io.crate.common.Booleans;
import io.crate.types.DataTypes;

/**
 * virtual "number_of_replicas" setting, internally made up of:
 * <ul>
 *  <li>index.number_of_replicas - an integer</li>
 *  <li>index.auto_expand_replicas - "false" or a range like "0-1" or "0-all" </li>
 * </ul>
 **/
public class NumberOfReplicas extends Setting<Settings> {

    private static final Pattern EXPAND_REPLICA_PATTERN = Pattern.compile("\\d+\\-(all|\\d+)");
    private static final Settings DEFAULT = Settings.builder()
        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
        .put(AutoExpandReplicas.SETTING_KEY, AutoExpandReplicas.SETTING.getDefaultRaw(Settings.EMPTY))
        .build();

    public static final NumberOfReplicas SETTING = new NumberOfReplicas();

    private NumberOfReplicas() {
        super(
            new SimpleKey(IndexMetadata.SETTING_NUMBER_OF_REPLICAS),
            (s) -> "0-1",
            NumberOfReplicas::parseValue,
            DataTypes.STRING,
            Property.ReplicatedIndexScope
        );
    }

    /**
     * Converts the external value (`0` or `0-1`) to the internal index. settings.
     */
    private static Settings parseValue(String value) {
        Settings.Builder builder = Settings.builder();

        // in case the number_of_replicas is changing from auto_expand to a fixed number -> disable auto expand
        builder.put(AutoExpandReplicas.SETTING.getKey(), false);

        try {
            int numReplicas = parseInt(value, 0, IndexMetadata.SETTING_NUMBER_OF_REPLICAS);
            builder.put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, numReplicas);
        } catch (NumberFormatException e) {
            validateExpandReplicaSetting(value);
            builder.put(AutoExpandReplicas.SETTING_KEY, value);
        }

        return builder.build();
    }

    @Override
    public Settings getDefault(Settings settings) {
        return DEFAULT;
    }

    /**
     * Reverse of {@link #parseValue(String). Converts {@code index.} settings into the user-facing value used in SQL statements.
     *
     * @return A single value (e.g {@code 2}) or a range (e.g {@code 0-1} or {@code 0-all})
     **/
    public static String getVirtualValue(Settings settings) {
        String numberOfReplicas;
        String autoExpandReplicas = settings.get(AutoExpandReplicas.SETTING_KEY);
        if (autoExpandReplicas != null && !Booleans.isFalse(autoExpandReplicas)) {
            validateExpandReplicaSetting(autoExpandReplicas);
            numberOfReplicas = autoExpandReplicas;
        } else {
            numberOfReplicas = Objects.requireNonNullElse(settings.get(IndexMetadata.SETTING_NUMBER_OF_REPLICAS), "1");
        }
        return numberOfReplicas;
    }

    public static int effectiveNumReplicas(Settings settings, DiscoveryNodes nodes) {
        int numDataNodes = nodes.getDataNodes().size();
        AutoExpandReplicas autoExpandReplicas = AutoExpandReplicas.SETTING.get(settings);
        if (autoExpandReplicas.isEnabled()) {
            final int min = autoExpandReplicas.getMinReplicas();
            final int max = autoExpandReplicas.getMaxReplicas(numDataNodes);
            int numberOfReplicas = numDataNodes - 1;
            if (numberOfReplicas < min) {
                return min;
            } else if (numberOfReplicas > max) {
                return max;
            }
        }
        return IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.get(settings);
    }

    private static void validateExpandReplicaSetting(String replicas) {
        if (!EXPAND_REPLICA_PATTERN.matcher(replicas).matches()) {
            throw new IllegalArgumentException("The \"number_of_replicas\" range \"" + replicas + "\" isn't valid");
        }
    }
}
