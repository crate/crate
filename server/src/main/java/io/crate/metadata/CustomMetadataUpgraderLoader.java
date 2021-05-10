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

package io.crate.metadata;

import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.settings.Settings;

import java.util.Iterator;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.function.UnaryOperator;

public class CustomMetadataUpgraderLoader implements UnaryOperator<Map<String, Metadata.Custom>> {

    private final Settings settings;
    private final Iterator<CustomMetadataUpgrader> upgraders;

    public CustomMetadataUpgraderLoader(Settings settings) {
        this.settings = settings;
        upgraders = ServiceLoader.load(CustomMetadataUpgrader.class).iterator();
    }

    @Override
    public Map<String, Metadata.Custom> apply(Map<String, Metadata.Custom> metadataCustoms) {
        while (upgraders.hasNext()) {
            upgraders.next().apply(settings, metadataCustoms);
        }
        return metadataCustoms;
    }
}
