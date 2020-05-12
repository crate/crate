/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.analysis;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.synonym.SynonymGraphFilter;
import org.apache.lucene.analysis.synonym.SynonymMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;

import java.io.IOException;
import java.util.List;
import java.util.function.Function;

public class SynonymGraphTokenFilterFactory extends SynonymTokenFilterFactory {

    public SynonymGraphTokenFilterFactory(IndexSettings indexSettings, Environment env, AnalysisRegistry analysisRegistry,
                                     String name, Settings settings) throws IOException {
        super(indexSettings, env, analysisRegistry, name, settings);
    }

    @Override
    public TokenStream create(TokenStream tokenStream) {
        throw new IllegalStateException("Call createPerAnalyzerSynonymGraphFactory to specialize this factory for an analysis chain first");
    }

    @Override
    public TokenFilterFactory getChainAwareTokenFilterFactory(TokenizerFactory tokenizer, List<CharFilterFactory> charFilters,
                                                              List<TokenFilterFactory> previousTokenFilters,
                                                              Function<String, TokenFilterFactory> allFilters) {
        final Analyzer analyzer = buildSynonymAnalyzer(tokenizer, charFilters, previousTokenFilters);
        final SynonymMap synonyms = buildSynonyms(analyzer, getRulesFromSettings(environment));
        final String name = name();
        return new TokenFilterFactory() {
            @Override
            public String name() {
                return name;
            }

            @Override
            public TokenStream create(TokenStream tokenStream) {
                // fst is null means no synonyms
                return synonyms.fst == null ? tokenStream : new SynonymGraphFilter(tokenStream, synonyms, ignoreCase);
            }
        };
    }

}
