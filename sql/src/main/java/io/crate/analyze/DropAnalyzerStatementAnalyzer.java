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

package io.crate.analyze;

import io.crate.exceptions.AnalyzerUnknownException;
import io.crate.metadata.FulltextAnalyzerResolver;
import org.elasticsearch.common.settings.Settings;

import static io.crate.metadata.settings.AnalyzerSettings.CUSTOM_ANALYSIS_SETTINGS_PREFIX;

public class DropAnalyzerStatementAnalyzer {

    private final FulltextAnalyzerResolver ftResolver;

    DropAnalyzerStatementAnalyzer(FulltextAnalyzerResolver ftResolver) {
        this.ftResolver = ftResolver;
    }

    public DropAnalyzerStatement analyze(String analyzerName) {
        if (ftResolver.hasBuiltInAnalyzer(analyzerName)) {
            throw new IllegalArgumentException("Cannot drop a built-in analyzer");
        }
        if (ftResolver.hasCustomAnalyzer(analyzerName) == false) {
            throw new AnalyzerUnknownException(analyzerName);
        }
        Settings.Builder builder = Settings.builder();
        builder.putNull(CUSTOM_ANALYSIS_SETTINGS_PREFIX + "analyzer." + analyzerName);

        Settings settings = ftResolver.getCustomAnalyzer(analyzerName);

        String tokenizerName = settings.get("index.analysis.analyzer." + analyzerName + ".tokenizer");
        if (tokenizerName != null
            && ftResolver.hasCustomThingy(tokenizerName, FulltextAnalyzerResolver.CustomType.TOKENIZER)) {
            builder.putNull(CUSTOM_ANALYSIS_SETTINGS_PREFIX + "tokenizer." + tokenizerName);
        }

        for (String tokenFilterName : settings.getAsList("index.analysis.analyzer." + analyzerName + ".filter")) {
            if (ftResolver.hasCustomThingy(tokenFilterName, FulltextAnalyzerResolver.CustomType.TOKEN_FILTER)) {
                builder.putNull(CUSTOM_ANALYSIS_SETTINGS_PREFIX + "filter." + tokenFilterName);
            }
        }

        for (String charFilterName : settings.getAsList("index.analysis.analyzer." + analyzerName + ".char_filter")) {
            if (ftResolver.hasCustomThingy(charFilterName, FulltextAnalyzerResolver.CustomType.CHAR_FILTER)) {
                builder.putNull(CUSTOM_ANALYSIS_SETTINGS_PREFIX + "char_filter." + charFilterName);
            }
        }

        return new DropAnalyzerStatement(builder.build());
    }
}
