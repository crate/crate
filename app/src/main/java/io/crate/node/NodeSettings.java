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

package io.crate.node;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.FailedToResolveConfigException;

import java.net.URL;

public class NodeSettings {

    /**
     * Crate default settings
     */
    public static void applyDefaultSettings(ImmutableSettings.Builder settingsBuilder) {

        // read also from crate.yml by default if no other config path has been set
        // if there is also a elasticsearch.yml file this file will be read first and the settings in crate.yml
        // will overwrite them.
        Environment environment = new Environment(settingsBuilder.build());
        if (System.getProperty("es.config") == null && System.getProperty("elasticsearch.config") == null) {
            // no explicit config path set
            try {
                URL crateConfigUrl = environment.resolveConfig("crate.yml");
                settingsBuilder.loadFromUrl(crateConfigUrl);
            } catch (FailedToResolveConfigException e) {
                // ignore
            }
        }

        // Set the default cluster name if not explicitly defined
        if (settingsBuilder.get(ClusterName.SETTING).equals(ClusterName.DEFAULT.value())) {
            settingsBuilder.put("cluster.name", "crate");
        }

        int availableProcessors = Math.min(32, Runtime.getRuntime().availableProcessors());
        int halfProcMaxAt10 = Math.min(((availableProcessors + 1) / 2), 10);
        settingsBuilder.put("threadpool.generic.size", halfProcMaxAt10);
    }
}