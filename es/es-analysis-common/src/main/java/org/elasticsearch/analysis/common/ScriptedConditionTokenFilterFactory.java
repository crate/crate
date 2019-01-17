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

package org.elasticsearch.analysis.common;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.miscellaneous.ConditionalTokenFilter;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AbstractTokenFilterFactory;
import org.elasticsearch.index.analysis.CharFilterFactory;
import org.elasticsearch.index.analysis.TokenFilterFactory;
import org.elasticsearch.index.analysis.TokenizerFactory;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.ScriptType;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * A factory for a conditional token filter that only applies child filters if the underlying token
 * matches an {@link AnalysisPredicateScript}
 */
public class ScriptedConditionTokenFilterFactory extends AbstractTokenFilterFactory {

    private final AnalysisPredicateScript.Factory factory;
    private final List<String> filterNames;

    ScriptedConditionTokenFilterFactory(IndexSettings indexSettings, String name,
                                               Settings settings, ScriptService scriptService) {
        super(indexSettings, name, settings);

        Settings scriptSettings = settings.getAsSettings("script");
        Script script = Script.parse(scriptSettings);
        if (script.getType() != ScriptType.INLINE) {
            throw new IllegalArgumentException("Cannot use stored scripts in tokenfilter [" + name + "]");
        }
        this.factory = scriptService.compile(script, AnalysisPredicateScript.CONTEXT);

        this.filterNames = settings.getAsList("filter");
        if (this.filterNames.isEmpty()) {
            throw new IllegalArgumentException("Empty list of filters provided to tokenfilter [" + name + "]");
        }
    }

    @Override
    public TokenStream create(TokenStream tokenStream) {
        throw new UnsupportedOperationException("getChainAwareTokenFilterFactory should be called first");
    }

    @Override
    public TokenFilterFactory getChainAwareTokenFilterFactory(TokenizerFactory tokenizer, List<CharFilterFactory> charFilters,
                                                              List<TokenFilterFactory> previousTokenFilters,
                                                              Function<String, TokenFilterFactory> allFilters) {
        List<TokenFilterFactory> filters = new ArrayList<>();
        List<TokenFilterFactory> existingChain = new ArrayList<>(previousTokenFilters);
        for (String filter : filterNames) {
            TokenFilterFactory tff = allFilters.apply(filter);
            if (tff == null) {
                throw new IllegalArgumentException("ScriptedConditionTokenFilter [" + name() +
                    "] refers to undefined token filter [" + filter + "]");
            }
            tff = tff.getChainAwareTokenFilterFactory(tokenizer, charFilters, existingChain, allFilters);
            filters.add(tff);
            existingChain.add(tff);
        }

        return new TokenFilterFactory() {
            @Override
            public String name() {
                return ScriptedConditionTokenFilterFactory.this.name();
            }

            @Override
            public TokenStream create(TokenStream tokenStream) {
                Function<TokenStream, TokenStream> filter = in -> {
                    for (TokenFilterFactory tff : filters) {
                        in = tff.create(in);
                    }
                    return in;
                };
                return new ScriptedConditionTokenFilter(tokenStream, filter, factory.newInstance());
            }
        };
    }

    private static class ScriptedConditionTokenFilter extends ConditionalTokenFilter {

        private final AnalysisPredicateScript script;
        private final AnalysisPredicateScript.Token token;

        ScriptedConditionTokenFilter(TokenStream input, Function<TokenStream, TokenStream> inputFactory,
                                     AnalysisPredicateScript script) {
            super(input, inputFactory);
            this.script = script;
            this.token = new AnalysisPredicateScript.Token(this);
        }

        @Override
        protected boolean shouldFilter() {
            token.updatePosition();
            return script.execute(token);
        }
    }

}
