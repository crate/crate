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

import org.elasticsearch.common.settings.Settings;

import java.util.Map;

public class TableParameter {

    private final Settings.Builder settingsBuilder;
    private final Settings.Builder mappingsBuilder;

    private Settings settings;
    private Map<String, Object> mappings;

    public TableParameter() {
        settingsBuilder = Settings.builder();
        mappingsBuilder = Settings.builder();
    }

    public Settings.Builder settingsBuilder() {
        return settingsBuilder;
    }

    public Settings settings() {
        if (settings == null) {
            settings = settingsBuilder.build();
        }
        return settings;
    }

    public Settings.Builder mappingsBuilder() {
        return mappingsBuilder;
    }

    public Map<String, Object> mappings() {
        if (mappings == null) {
            mappings = mappingsBuilder.build().getAsStructuredMap();
        }
        return mappings;
    }
}
