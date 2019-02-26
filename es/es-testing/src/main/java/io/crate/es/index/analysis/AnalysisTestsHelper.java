/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.es.index.analysis;

import io.crate.es.Version;
import io.crate.es.cluster.metadata.IndexMetaData;
import io.crate.es.common.settings.Settings;
import io.crate.es.env.Environment;
import io.crate.es.index.IndexSettings;
import io.crate.es.indices.analysis.AnalysisModule;
import io.crate.es.plugins.AnalysisPlugin;
import io.crate.es.test.ESTestCase;
import io.crate.es.test.IndexSettingsModule;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;

public class AnalysisTestsHelper {

    public static ESTestCase.TestAnalysis createTestAnalysisFromClassPath(final Path baseDir,
                                                                          final String resource,
                                                                          final AnalysisPlugin... plugins) throws IOException {
        final Settings settings = Settings.builder()
                .loadFromStream(resource, AnalysisTestsHelper.class.getResourceAsStream(resource), false)
                .put(Environment.PATH_HOME_SETTING.getKey(), baseDir.toString())
                .build();

        return createTestAnalysisFromSettings(settings, plugins);
    }

    public static ESTestCase.TestAnalysis createTestAnalysisFromSettings(
            final Settings settings, final AnalysisPlugin... plugins) throws IOException {
        return createTestAnalysisFromSettings(settings, null, plugins);
    }

    public static ESTestCase.TestAnalysis createTestAnalysisFromSettings(
            final Settings settings,
            final Path configPath,
            final AnalysisPlugin... plugins) throws IOException {
        final Settings actualSettings;
        if (settings.get(IndexMetaData.SETTING_VERSION_CREATED) == null) {
            actualSettings = Settings.builder().put(settings).put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT).build();
        } else {
            actualSettings = settings;
        }
        final IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("test", actualSettings);
        final AnalysisRegistry analysisRegistry =
                new AnalysisModule(new Environment(actualSettings, configPath), Arrays.asList(plugins)).getAnalysisRegistry();
        return new ESTestCase.TestAnalysis(analysisRegistry.build(indexSettings),
                analysisRegistry.buildTokenFilterFactories(indexSettings),
                analysisRegistry.buildTokenizerFactories(indexSettings),
                analysisRegistry.buildCharFilterFactories(indexSettings));
    }

}
