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

package io.crate.analyze;

import io.crate.common.Booleans;
import org.elasticsearch.cluster.metadata.AutoExpandReplicas;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;

import java.util.Objects;
import java.util.regex.Pattern;

public class NumberOfReplicas {

    public static final String NUMBER_OF_REPLICAS = IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
    public static final String AUTO_EXPAND_REPLICAS = IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS;

    private static final Pattern EXPAND_REPLICA_PATTERN = Pattern.compile("\\d+\\-(all|\\d+)");
    private final String esSettingKey;
    private final String esSettingsValue;

    public NumberOfReplicas(Integer numReplicas) {
        this.esSettingKey = NUMBER_OF_REPLICAS;
        this.esSettingsValue = numReplicas.toString();
    }

    public NumberOfReplicas(String numReplicas) {
        assert numReplicas != null : "numReplicas must not be null";
        validateExpandReplicaSetting(numReplicas);

        this.esSettingKey = AUTO_EXPAND_REPLICAS;
        this.esSettingsValue = numReplicas;
    }

    private static void validateExpandReplicaSetting(String replicas) {
        if (!EXPAND_REPLICA_PATTERN.matcher(replicas).matches()) {
            throw new IllegalArgumentException("The \"number_of_replicas\" range \"" + replicas + "\" isn't valid");
        }
    }

    public String esSettingKey() {
        return esSettingKey;
    }

    public String esSettingValue() {
        return esSettingsValue;
    }

    public static String fromSettings(Settings settings) {
        String numberOfReplicas;
        String autoExpandReplicas = settings.get(AUTO_EXPAND_REPLICAS);
        if (autoExpandReplicas != null && !Booleans.isFalse(autoExpandReplicas)) {
            validateExpandReplicaSetting(autoExpandReplicas);
            numberOfReplicas = autoExpandReplicas;
        } else {
            numberOfReplicas = Objects.requireNonNullElse(settings.get(NUMBER_OF_REPLICAS), "1");
        }
        return numberOfReplicas;
    }

    public static int fromSettings(Settings settings, int dataNodeCount) {
        AutoExpandReplicas autoExpandReplicas = IndexMetadata.INDEX_AUTO_EXPAND_REPLICAS_SETTING.get(settings);
        if (autoExpandReplicas.isEnabled()) {
            final int min = autoExpandReplicas.getMinReplicas();
            final int max = autoExpandReplicas.getMaxReplicas(dataNodeCount);
            int numberOfReplicas = dataNodeCount - 1;
            if (numberOfReplicas < min) {
                return min;
            } else if (numberOfReplicas > max) {
                return max;
            }
        }
        return IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.get(settings);
    }
}
