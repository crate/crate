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

import io.crate.data.Row;
import io.crate.sql.tree.GenericProperties;
import org.elasticsearch.common.settings.Setting;

import java.util.List;
import java.util.Map;

public final class TablePropertiesAnalyzer {

    private static final String INVALID_MESSAGE = "Invalid property \"%s\" passed to [ALTER | CREATE] TABLE statement";

    private TablePropertiesAnalyzer() {
    }

    public static void analyze(TableParameter tableParameter,
                               TableParameterInfo tableParameterInfo,
                               GenericProperties properties,
                               Row parameters) {
        analyze(tableParameter, tableParameterInfo, properties, parameters, false);
    }

    public static void analyze(TableParameter tableParameter,
                               TableParameterInfo tableParameterInfo,
                               GenericProperties properties,
                               Row parameters,
                               boolean withDefaults) {
        Map<String, Setting> settingMap = tableParameterInfo.supportedSettings();
        Map<String, Setting> mappingsMap = tableParameterInfo.supportedMappings();

        GenericPropertiesConverter.settingsFromProperties(
            tableParameter.settingsBuilder(),
            properties,
            parameters,
            settingMap,
            withDefaults,
            mappingsMap::containsKey,
            INVALID_MESSAGE);

        GenericPropertiesConverter.settingsFromProperties(
            tableParameter.mappingsBuilder(),
            properties,
            parameters,
            mappingsMap,
            withDefaults,
            settingMap::containsKey,
            INVALID_MESSAGE);
    }

    /**
     * Processes the property names which should be reset and updates the settings or mappings with the related
     * default value.
     */
    static void analyzeResetProperties(TableParameter tableParameter,
                                       TableParameterInfo tableParameterInfo,
                                       List<String> properties) {
        Map<String, Setting> settingMap = tableParameterInfo.supportedSettings();
        Map<String, Setting> mappingsMap = tableParameterInfo.supportedMappings();

        GenericPropertiesConverter.resetSettingsFromProperties(
            tableParameter.settingsBuilder(),
            properties,
            settingMap,
            mappingsMap::containsKey,
            INVALID_MESSAGE);

        GenericPropertiesConverter.resetSettingsFromProperties(
            tableParameter.mappingsBuilder(),
            properties,
            mappingsMap,
            settingMap::containsKey,
            INVALID_MESSAGE);
    }
}
