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

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.miscellaneous.DisableGraphAttribute;
import org.apache.lucene.analysis.shingle.ShingleFilter;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;

public class ShingleTokenFilterFactory extends AbstractTokenFilterFactory {

    private final Factory factory;

    public ShingleTokenFilterFactory(IndexSettings indexSettings, Environment environment, String name, Settings settings) {
        super(indexSettings, name, settings);
        int maxAllowedShingleDiff = indexSettings.getMaxShingleDiff();
        Integer maxShingleSize = settings.getAsInt("max_shingle_size", ShingleFilter.DEFAULT_MAX_SHINGLE_SIZE);
        Integer minShingleSize = settings.getAsInt("min_shingle_size", ShingleFilter.DEFAULT_MIN_SHINGLE_SIZE);
        Boolean outputUnigrams = settings.getAsBooleanLenientForPreEs6Indices(indexSettings.getIndexVersionCreated(), "output_unigrams", true, deprecationLogger);
        Boolean outputUnigramsIfNoShingles = settings.getAsBooleanLenientForPreEs6Indices(indexSettings.getIndexVersionCreated(), "output_unigrams_if_no_shingles", false, deprecationLogger);

        int shingleDiff = maxShingleSize - minShingleSize + (outputUnigrams ? 1 : 0);
        if (shingleDiff > maxAllowedShingleDiff) {
            deprecationLogger.deprecated("Deprecated big difference between maxShingleSize and minShingleSize in Shingle TokenFilter,"
                + "expected difference must be less than or equal to: [" + maxAllowedShingleDiff + "]");
        }
        String tokenSeparator = settings.get("token_separator", ShingleFilter.DEFAULT_TOKEN_SEPARATOR);
        String fillerToken = settings.get("filler_token", ShingleFilter.DEFAULT_FILLER_TOKEN);
        factory = new Factory("shingle", minShingleSize, maxShingleSize, outputUnigrams, outputUnigramsIfNoShingles, tokenSeparator, fillerToken);
    }


    @Override
    public TokenStream create(TokenStream tokenStream) {
        return factory.create(tokenStream);
    }


    public Factory getInnerFactory() {
        return this.factory;
    }

    public static final class Factory implements TokenFilterFactory {
        private final int maxShingleSize;

        private final boolean outputUnigrams;

        private final boolean outputUnigramsIfNoShingles;

        private final String tokenSeparator;
        private final String fillerToken;

        private int minShingleSize;

        private final String name;

        public Factory(String name) {
            this(name, ShingleFilter.DEFAULT_MIN_SHINGLE_SIZE, ShingleFilter.DEFAULT_MAX_SHINGLE_SIZE, true, false, ShingleFilter.DEFAULT_TOKEN_SEPARATOR, ShingleFilter.DEFAULT_FILLER_TOKEN);
        }

        Factory(String name, int minShingleSize, int maxShingleSize, boolean outputUnigrams, boolean outputUnigramsIfNoShingles, String tokenSeparator, String fillerToken) {
            this.maxShingleSize = maxShingleSize;
            this.outputUnigrams = outputUnigrams;
            this.outputUnigramsIfNoShingles = outputUnigramsIfNoShingles;
            this.tokenSeparator = tokenSeparator;
            this.minShingleSize = minShingleSize;
            this.fillerToken = fillerToken;
            this.name = name;
        }

        @Override
        public TokenStream create(TokenStream tokenStream) {
            ShingleFilter filter = new ShingleFilter(tokenStream, minShingleSize, maxShingleSize);
            filter.setOutputUnigrams(outputUnigrams);
            filter.setOutputUnigramsIfNoShingles(outputUnigramsIfNoShingles);
            filter.setTokenSeparator(tokenSeparator);
            filter.setFillerToken(fillerToken);
            if (outputUnigrams || (minShingleSize != maxShingleSize)) {
                /**
                 * We disable the graph analysis on this token stream
                 * because it produces shingles of different size.
                 * Graph analysis on such token stream is useless and dangerous as it may create too many paths
                 * since shingles of different size are not aligned in terms of positions.
                 */
                filter.addAttribute(DisableGraphAttribute.class);
            }
            return filter;
        }

        public int getMaxShingleSize() {
            return maxShingleSize;
        }

        public int getMinShingleSize() {
            return minShingleSize;
        }

        public boolean getOutputUnigrams() {
            return outputUnigrams;
        }

        public boolean getOutputUnigramsIfNoShingles() {
            return outputUnigramsIfNoShingles;
        }

        @Override
        public String name() {
            return name;
        }
    }
}
