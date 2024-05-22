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

package org.elasticsearch.index;

import static org.elasticsearch.test.ESTestCase.createTestAnalysis;

import java.io.IOException;
import java.nio.file.Path;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.indices.mapper.MapperRegistry;
import org.elasticsearch.test.IndexSettingsModule;


public class MapperTestUtils {

    public static MapperService newMapperService(Path tempDir,
                                                 Settings settings,
                                                 String indexName) throws IOException {
        Settings.Builder settingsBuilder = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), tempDir)
            .put(settings);
        if (settings.get(IndexMetadata.SETTING_VERSION_CREATED) == null) {
            settingsBuilder.put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT);
        }
        Settings finalSettings = settingsBuilder.build();
        MapperRegistry mapperRegistry = new IndicesModule().getMapperRegistry();
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings(indexName, finalSettings);
        IndexAnalyzers indexAnalyzers = createTestAnalysis(indexSettings, finalSettings).indexAnalyzers;
        return new MapperService(
            indexSettings,
            indexAnalyzers,
            mapperRegistry
        );
    }
}
