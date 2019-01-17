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

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.TextFieldMapper;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * A custom analyzer that is built out of a single {@link org.apache.lucene.analysis.Tokenizer} and a list
 * of {@link org.apache.lucene.analysis.TokenFilter}s.
 */
public class CustomAnalyzerProvider extends AbstractIndexAnalyzerProvider<CustomAnalyzer> {

    private final Settings analyzerSettings;
    private final Environment environment;

    private CustomAnalyzer customAnalyzer;

    public CustomAnalyzerProvider(IndexSettings indexSettings,
                                  String name, Settings settings, Environment environment) {
        super(indexSettings, name, settings);
        this.analyzerSettings = settings;
        this.environment = environment;
    }

    public void build(final Map<String, TokenizerFactory> tokenizers, final Map<String, CharFilterFactory> charFilters,
                      final Map<String, TokenFilterFactory> tokenFilters) {
        String tokenizerName = analyzerSettings.get("tokenizer");
        if (tokenizerName == null) {
            throw new IllegalArgumentException("Custom Analyzer [" + name() + "] must be configured with a tokenizer");
        }

        TokenizerFactory tokenizer = tokenizers.get(tokenizerName);
        if (tokenizer == null) {
            throw new IllegalArgumentException("Custom Analyzer [" + name() + "] failed to find tokenizer under name [" + tokenizerName + "]");
        }

        List<String> charFilterNames = analyzerSettings.getAsList("char_filter");
        List<CharFilterFactory> charFiltersList = new ArrayList<>(charFilterNames.size());
        for (String charFilterName : charFilterNames) {
            CharFilterFactory charFilter = charFilters.get(charFilterName);
            if (charFilter == null) {
                throw new IllegalArgumentException("Custom Analyzer [" + name() + "] failed to find char_filter under name [" + charFilterName + "]");
            }
            charFiltersList.add(charFilter);
        }

        int positionIncrementGap = TextFieldMapper.Defaults.POSITION_INCREMENT_GAP;

        positionIncrementGap = analyzerSettings.getAsInt("position_increment_gap", positionIncrementGap);

        int offsetGap = analyzerSettings.getAsInt("offset_gap", -1);

        List<String> tokenFilterNames = analyzerSettings.getAsList("filter");
        List<TokenFilterFactory> tokenFilterList = new ArrayList<>(tokenFilterNames.size());
        for (String tokenFilterName : tokenFilterNames) {
            TokenFilterFactory tokenFilter = tokenFilters.get(tokenFilterName);
            if (tokenFilter == null) {
                throw new IllegalArgumentException("Custom Analyzer [" + name() + "] failed to find filter under name [" + tokenFilterName + "]");
            }
            tokenFilter = tokenFilter.getChainAwareTokenFilterFactory(tokenizer, charFiltersList, tokenFilterList, tokenFilters::get);
            tokenFilterList.add(tokenFilter);
        }

        this.customAnalyzer = new CustomAnalyzer(tokenizerName, tokenizer,
                charFiltersList.toArray(new CharFilterFactory[charFiltersList.size()]),
                tokenFilterList.toArray(new TokenFilterFactory[tokenFilterList.size()]),
                positionIncrementGap,
                offsetGap
        );
    }

    @Override
    public CustomAnalyzer get() {
        return this.customAnalyzer;
    }
}
