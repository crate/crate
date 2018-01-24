/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

import com.google.common.collect.ImmutableMap;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.search.MatchQuery;
import org.elasticsearch.index.search.MultiMatchQuery;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

public final class MatchQueries {


    private static final Map<BytesRef, MultiMatchQueryBuilder.Type> SUPPORTED_TYPES =
        ImmutableMap.<BytesRef, MultiMatchQueryBuilder.Type>builder()
            .put(new BytesRef("best_fields"), MultiMatchQueryBuilder.Type.BEST_FIELDS)
            .put(new BytesRef("most_fields"), MultiMatchQueryBuilder.Type.MOST_FIELDS)
            .put(new BytesRef("cross_fields"), MultiMatchQueryBuilder.Type.CROSS_FIELDS)
            .put(new BytesRef("phrase"), MultiMatchQueryBuilder.Type.PHRASE)
            .put(new BytesRef("phrase_prefix"), MultiMatchQueryBuilder.Type.PHRASE_PREFIX)
            .build();

    public static Query singleMatch(QueryShardContext queryShardContext,
                                    String fieldName,
                                    BytesRef queryString,
                                    @Nullable BytesRef matchType,
                                    @Nullable Map<String, Object> options) throws IOException {
        MultiMatchQueryBuilder.Type type = getType(matchType);
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

        MatchQuery.Type matchQueryType = type.matchQueryType();
        return matchQuery.parse(matchQueryType, fieldName, queryString.utf8ToString());
    }

    public static Query multiMatch(QueryShardContext queryShardContext,
                                   @Nullable BytesRef matchType,
                                   Map<String, Float> fieldNames,
                                   String queryString,
                                   Map<String, Object> options) throws IOException {
        MultiMatchQueryBuilder.Type type = MatchQueries.getType(matchType);
        ParsedOptions parsedOptions = OptionParser.parse(type, options);

        MultiMatchQuery multiMatchQuery = new MultiMatchQuery(queryShardContext);
        Float tieBreaker = parsedOptions.tieBreaker();
        if (tieBreaker != null) {
            multiMatchQuery.setTieBreaker(tieBreaker);
        }
        multiMatchQuery.setAnalyzer(parsedOptions.analyzer());
        multiMatchQuery.setCommonTermsCutoff(parsedOptions.commonTermsCutoff());
        multiMatchQuery.setFuzziness(parsedOptions.fuzziness());
        multiMatchQuery.setFuzzyPrefixLength(parsedOptions.prefixLength());
        multiMatchQuery.setFuzzyRewriteMethod(parsedOptions.rewriteMethod());
        multiMatchQuery.setMaxExpansions(parsedOptions.maxExpansions());
        multiMatchQuery.setPhraseSlop(parsedOptions.phraseSlop());
        multiMatchQuery.setTranspositions(parsedOptions.transpositions());
        multiMatchQuery.setZeroTermsQuery(parsedOptions.zeroTermsQuery());
        return multiMatchQuery.parse(type, fieldNames, queryString, parsedOptions.minimumShouldMatch());
    }

    private static MultiMatchQueryBuilder.Type getType(@Nullable BytesRef matchType) {
        if (matchType == null) {
            return MultiMatchQueryBuilder.Type.BEST_FIELDS;
        }
        MultiMatchQueryBuilder.Type type = SUPPORTED_TYPES.get(matchType);
        if (type == null) {
            throw illegalMatchType(BytesRefs.toString(matchType));
        }
        return type;
    }

    private static IllegalArgumentException illegalMatchType(String matchType) {
        String matchTypes = SUPPORTED_TYPES.keySet()
            .stream()
            .map(BytesRefs::toString)
            .collect(Collectors.joining(", "));
        throw new IllegalArgumentException(String.format(
            Locale.ENGLISH,
            "Unknown matchType \"%s\". Possible matchTypes are: %s",
            matchType,
            matchTypes));
    }
}
