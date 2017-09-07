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

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.Booleans;
import org.elasticsearch.common.settings.Settings;

import java.util.regex.Pattern;

public class NumberOfReplicas {

    public static final String NUMBER_OF_REPLICAS = IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
    public static final String AUTO_EXPAND_REPLICAS = IndexMetaData.SETTING_AUTO_EXPAND_REPLICAS;

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
        Preconditions.checkArgument(EXPAND_REPLICA_PATTERN.matcher(replicas).matches(),
            "The \"number_of_replicas\" range \"%s\" isn't valid", replicas);
    }

    public String esSettingKey() {
        return esSettingKey;
    }

    public String esSettingValue() {
        return esSettingsValue;
    }

    public static BytesRef fromSettings(Settings settings) {
        BytesRef numberOfReplicas;
        String autoExpandReplicas = settings.get(AUTO_EXPAND_REPLICAS);
        if (autoExpandReplicas != null && !Booleans.isExplicitFalse(autoExpandReplicas)) {
            validateExpandReplicaSetting(autoExpandReplicas);
            numberOfReplicas = new BytesRef(autoExpandReplicas);
        } else {
            numberOfReplicas = new BytesRef(MoreObjects.firstNonNull(settings.get(NUMBER_OF_REPLICAS), "1"));
        }
        return numberOfReplicas;
    }
}
