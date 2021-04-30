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

package io.crate.lucene.match;

import org.apache.lucene.search.Query;
import org.elasticsearch.index.query.MultiMatchQueryType;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.search.MatchQuery;
import org.elasticsearch.index.search.MultiMatchQuery;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Locale;
import java.util.Map;

public final class MatchQueries {


    private static final Map<String, MultiMatchQueryType> SUPPORTED_TYPES = Map.of(
            "best_fields", MultiMatchQueryType.BEST_FIELDS,
            "most_fields", MultiMatchQueryType.MOST_FIELDS,
            "cross_fields", MultiMatchQueryType.CROSS_FIELDS,
            "phrase", MultiMatchQueryType.PHRASE,
            "phrase_prefix", MultiMatchQueryType.PHRASE_PREFIX
        );

    public static Query singleMatch(QueryShardContext queryShardContext,
                                    String fieldName,
                                    String queryString,
                                    @Nullable String matchType,
                                    @Nullable Map<String, Object> options) throws IOException {
        MultiMatchQueryType type = getType(matchType);
        ParsedOptions parsedOptions = OptionParser.parse(type, options);

        MatchQuery matchQuery = new MatchQuery(queryShardContext);

        if (parsedOptions.analyzer() != null) {
            matchQuery.setAnalyzer(parsedOptions.analyzer());
        }
        matchQuery.setCommonTermsCutoff(parsedOptions.commonTermsCutoff());
        matchQuery.setFuzziness(parsedOptions.fuzziness());
        matchQuery.setFuzzyPrefixLength(parsedOptions.prefixLength());
        matchQuery.setFuzzyRewriteMethod(parsedOptions.rewriteMethod());
        matchQuery.setMaxExpansions(parsedOptions.maxExpansions());
        matchQuery.setPhraseSlop(parsedOptions.phraseSlop());
        matchQuery.setTranspositions(parsedOptions.transpositions());
        matchQuery.setZeroTermsQuery(parsedOptions.zeroTermsQuery());
        matchQuery.setOccur(parsedOptions.operator());

        MatchQuery.Type matchQueryType = type.matchQueryType();
        return matchQuery.parse(matchQueryType, fieldName, queryString);
    }

    public static Query multiMatch(QueryShardContext queryShardContext,
                                   @Nullable String matchType,
                                   Map<String, Float> fieldNames,
                                   String queryString,
                                   Map<String, Object> options) throws IOException {
        MultiMatchQueryType type = MatchQueries.getType(matchType);
        ParsedOptions parsedOptions = OptionParser.parse(type, options);

        MultiMatchQuery multiMatchQuery = new MultiMatchQuery(queryShardContext);
        Float tieBreaker = parsedOptions.tieBreaker();
        if (tieBreaker != null) {
            multiMatchQuery.setTieBreaker(tieBreaker);
        }
        String analyzer = parsedOptions.analyzer();
        if (analyzer != null) {
            multiMatchQuery.setAnalyzer(analyzer);
        }
        multiMatchQuery.setCommonTermsCutoff(parsedOptions.commonTermsCutoff());
        multiMatchQuery.setFuzziness(parsedOptions.fuzziness());
        multiMatchQuery.setFuzzyPrefixLength(parsedOptions.prefixLength());
        multiMatchQuery.setFuzzyRewriteMethod(parsedOptions.rewriteMethod());
        multiMatchQuery.setMaxExpansions(parsedOptions.maxExpansions());
        multiMatchQuery.setPhraseSlop(parsedOptions.phraseSlop());
        multiMatchQuery.setTranspositions(parsedOptions.transpositions());
        multiMatchQuery.setZeroTermsQuery(parsedOptions.zeroTermsQuery());
        multiMatchQuery.setOccur(parsedOptions.operator());

        return multiMatchQuery.parse(type, fieldNames, queryString, parsedOptions.minimumShouldMatch());
    }

    private static MultiMatchQueryType getType(@Nullable String matchType) {
        if (matchType == null) {
            return MultiMatchQueryType.BEST_FIELDS;
        }
        MultiMatchQueryType type = SUPPORTED_TYPES.get(matchType);
        if (type == null) {
            throw illegalMatchType(matchType);
        }
        return type;
    }

    private static IllegalArgumentException illegalMatchType(String matchType) {
        String matchTypes = String.join(", ", SUPPORTED_TYPES.keySet());
        throw new IllegalArgumentException(String.format(
            Locale.ENGLISH,
            "Unknown matchType \"%s\". Possible matchTypes are: %s",
            matchType,
            matchTypes));
    }
}
