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

package io.crate.core;

import com.google.common.base.Preconditions;
import org.elasticsearch.cluster.metadata.IndexMetaData;

import java.util.regex.Pattern;

public class NumberOfReplicas {

    public final static String NUMBER_OF_REPLICAS = IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
    public final static String AUTO_EXPAND_REPLICAS = IndexMetaData.SETTING_AUTO_EXPAND_REPLICAS;

    private static final Pattern EXPAND_REPLICA_PATTERN = Pattern.compile("\\d+\\-(all|\\d+)");
    private final String esSettingKey;
    private final String esSettingsValue;

    public NumberOfReplicas(Integer replicas) {
        this.esSettingKey = NUMBER_OF_REPLICAS;
        this.esSettingsValue = replicas.toString();
    }

    public NumberOfReplicas(String replicas) {
        assert replicas != null;
        try {
            int numReplicas = Integer.parseInt(replicas);
        } catch (NumberFormatException e) {
            Preconditions.checkArgument(EXPAND_REPLICA_PATTERN.matcher(replicas).matches(),
                    "The \"number_of_replicas\" range \"%s\" isn't valid", replicas);

            this.esSettingKey = AUTO_EXPAND_REPLICAS;
            this.esSettingsValue = replicas;
            return;
        }

        this.esSettingKey = NUMBER_OF_REPLICAS;
        this.esSettingsValue = replicas;
    }

    public String esSettingKey() {
        return esSettingKey;
    }

    public String esSettingValue() {
        return esSettingsValue;
    }
}
