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

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;
import io.crate.types.BooleanType;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.support.QueryParsers;
import org.elasticsearch.index.search.MatchQuery;

import javax.annotation.Nullable;
import java.util.*;

public class OptionParser {

    private static class OPTIONS {
        static final String ANALYZER = "analyzer";
        static final String BOOST = "boost";
        static final String OPERATOR = "operator";
        static final String CUTOFF_FREQUENCY = "cutoff_frequency";
        static final String MINIMUM_SHOULD_MATCH = "minimum_should_match";
        static final String FUZZINESS = "fuzziness";
        static final String PREFIX_LENGTH = "prefix_length";
        static final String MAX_EXPANSIONS = "max_expansions";
        static final String REWRITE = "rewrite";
        static final String SLOP = "slop";
        static final String TIE_BREAKER = "tie_breaker";
        static final String ZERO_TERMS_QUERY = "zero_terms_query";
        static final String FUZZY_REWRITE = "fuzzy_rewrite";
        static final String FUZZY_TRANSPOSITIONS = "fuzzy_transpositions";
    }

    private static final ImmutableSet<String> SUPPORTED_OPTIONS = ImmutableSet.<String>builder().add(
        OPTIONS.ANALYZER, OPTIONS.BOOST, OPTIONS.OPERATOR, OPTIONS.CUTOFF_FREQUENCY,
        OPTIONS.MINIMUM_SHOULD_MATCH, OPTIONS.FUZZINESS, OPTIONS.PREFIX_LENGTH,
        OPTIONS.MAX_EXPANSIONS, OPTIONS.REWRITE, OPTIONS.SLOP, OPTIONS.TIE_BREAKER,
        OPTIONS.ZERO_TERMS_QUERY, OPTIONS.FUZZY_REWRITE, OPTIONS.FUZZY_TRANSPOSITIONS
    ).build();

    public static ParsedOptions parse(MultiMatchQueryBuilder.Type matchType,
                                      @Nullable Map options) throws IllegalArgumentException {
        if (options == null) {
            options = Collections.emptyMap();
        } else {
            // need a copy. Otherwise manipulations on a shared option will lead to strange race conditions.
            options = new HashMap(options);
        }
        ParsedOptions parsedOptions = new ParsedOptions(
            floatValue(options, OPTIONS.BOOST, null),
            analyzer(options.remove(OPTIONS.ANALYZER)),
            zeroTermsQuery(options.remove(OPTIONS.ZERO_TERMS_QUERY)),
            intValue(options, OPTIONS.MAX_EXPANSIONS, FuzzyQuery.defaultMaxExpansions),
            fuzziness(options.remove(OPTIONS.FUZZINESS)),
            intValue(options, OPTIONS.PREFIX_LENGTH, FuzzyQuery.defaultPrefixLength),
            transpositions(options.remove(OPTIONS.FUZZY_TRANSPOSITIONS))
        );

        switch (matchType.matchQueryType()) {
            case BOOLEAN:
                parsedOptions.commonTermsCutoff(floatValue(options, OPTIONS.CUTOFF_FREQUENCY, null));
                parsedOptions.operator(operator(options.remove(OPTIONS.OPERATOR)));
                parsedOptions.minimumShouldMatch(minimumShouldMatch(options.remove(OPTIONS.MINIMUM_SHOULD_MATCH)));
                break;
            case PHRASE:
                parsedOptions.phraseSlop(intValue(options, OPTIONS.SLOP, 0));
                parsedOptions.tieBreaker(floatValue(options, OPTIONS.TIE_BREAKER, null));
                break;
            case PHRASE_PREFIX:
                parsedOptions.phraseSlop(intValue(options, OPTIONS.SLOP, 0));
                parsedOptions.tieBreaker(floatValue(options, OPTIONS.TIE_BREAKER, null));
                parsedOptions.rewrite(rewrite(options.remove(OPTIONS.REWRITE)));
                break;
        }
        if (!options.isEmpty()) {
            raiseIllegalOptions(matchType, options);
        }
        return parsedOptions;
    }

    private static Boolean transpositions(@Nullable Object transpositions) {
        if (transpositions == null) {
            return false;
        }
        return BooleanType.INSTANCE.value(transpositions);
    }

    @Nullable
    private static String minimumShouldMatch(@Nullable Object minimumShouldMatch) {
        return BytesRefs.toString(minimumShouldMatch);
    }

    private static BooleanClause.Occur operator(@Nullable Object operator) {
        if (operator == null) {
            return BooleanClause.Occur.SHOULD;
        }
        String op = BytesRefs.toString(operator);
        if ("or".equalsIgnoreCase(op)) {
            return BooleanClause.Occur.SHOULD;
        } else if ("and".equalsIgnoreCase(op)) {
            return BooleanClause.Occur.MUST;
        }
        throw new IllegalArgumentException(String.format(Locale.ENGLISH,
            "value for operator must be either \"or\" or \"and\" not \"%s\"", op));
    }

    private static Float floatValue(Map options, String optionName, Float defaultValue) {
        Object o = options.remove(optionName);
        if (o == null) {
            return defaultValue;
        } else if (o instanceof Float) {
            return (Float) o;
        } else if (o instanceof Number) {
            return ((Number) o).floatValue();
        }
        throw new IllegalArgumentException(String.format(Locale.ENGLISH, "value for %s must be a number", optionName));
    }

    private static Integer intValue(Map options, String optionName, Integer defaultValue) {
        Object o = options.remove(optionName);
        if (o == null) {
            return defaultValue;
        } else if (o instanceof Number) {
            return ((Number) o).intValue();
        }
        throw new IllegalArgumentException(String.format(Locale.ENGLISH, "value for %s must be a number", optionName));
    }

    private static org.apache.lucene.search.MultiTermQuery.RewriteMethod rewrite(@Nullable Object fuzzyRewrite) {
        String rewrite = BytesRefs.toString(fuzzyRewrite);
        return QueryParsers.parseRewriteMethod(rewrite);
    }

    @Nullable
    private static Fuzziness fuzziness(@Nullable Object fuzziness) {
        if (fuzziness == null) {
            return null;
        }
        return Fuzziness.build(BytesRefs.toString(fuzziness));
    }

    private static MatchQuery.ZeroTermsQuery zeroTermsQuery(@Nullable Object zeroTermsQuery) {
        String value = BytesRefs.toString(zeroTermsQuery);
        if (value == null || "none".equalsIgnoreCase(value)) {
            return MatchQuery.ZeroTermsQuery.NONE;
        } else if ("all".equalsIgnoreCase(value)) {
            return MatchQuery.ZeroTermsQuery.ALL;
        }
        throw new IllegalArgumentException(String.format(Locale.ENGLISH,
            "Unsupported value for %s option. Valid are \"none\" and \"all\"", OPTIONS.ZERO_TERMS_QUERY));
    }

    @Nullable
    private static String analyzer(@Nullable Object analyzer) {
        if (analyzer == null) {
            return null;
        }
        if (analyzer instanceof String || analyzer instanceof BytesRef) {
            return BytesRefs.toString(analyzer);
        }
        throw new IllegalArgumentException("value for analyzer must be a string");
    }

    private static void raiseIllegalOptions(MultiMatchQueryBuilder.Type matchType, Map options) {
        List<String> unknownOptions = new ArrayList<>();
        List<String> invalidOptions = new ArrayList<>();
        for (Object o : options.keySet()) {
            assert o instanceof String : "option must be String";
            if (!SUPPORTED_OPTIONS.contains(o)) {
                unknownOptions.add((String) o);
            } else {
                invalidOptions.add((String) o);
            }
        }
        if (!unknownOptions.isEmpty()) {
            throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                "match predicate doesn't support any of the given options: %s",
                Joiner.on(", ").join(unknownOptions)));
        } else {
            throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                "match predicate option(s) \"%s\" cannot be used with matchType \"%s\"",
                Joiner.on(", ").join(invalidOptions),
                matchType.name().toLowerCase(Locale.ENGLISH)
            ));
        }
    }
}
