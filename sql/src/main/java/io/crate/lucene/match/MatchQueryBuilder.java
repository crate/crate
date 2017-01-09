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
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.ExtendedCommonTermsQuery;
import org.apache.lucene.search.*;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.QueryBuilder;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.lucene.search.MultiPhrasePrefixQuery;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.support.QueryParsers;
import org.elasticsearch.index.search.MatchQuery;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

public class MatchQueryBuilder {

    protected final MapperService mapperService;
    protected final ParsedOptions options;

    final MultiMatchQueryBuilder.Type matchType;

    private static final ImmutableMap<BytesRef, MultiMatchQueryBuilder.Type> SUPPORTED_TYPES =
        ImmutableMap.<BytesRef, MultiMatchQueryBuilder.Type>builder()
            .put(new BytesRef("best_fields"), MultiMatchQueryBuilder.Type.BEST_FIELDS)
            .put(new BytesRef("most_fields"), MultiMatchQueryBuilder.Type.MOST_FIELDS)
            .put(new BytesRef("cross_fields"), MultiMatchQueryBuilder.Type.CROSS_FIELDS)
            .put(new BytesRef("phrase"), MultiMatchQueryBuilder.Type.PHRASE)
            .put(new BytesRef("phrase_prefix"), MultiMatchQueryBuilder.Type.PHRASE_PREFIX)
            .build();

    public MatchQueryBuilder(MapperService mapperService,
                             @Nullable BytesRef matchType,
                             @Nullable Map options) throws IOException {
        this.mapperService = mapperService;
        if (matchType == null) {
            this.matchType = MultiMatchQueryBuilder.Type.BEST_FIELDS;
        } else {
            this.matchType = SUPPORTED_TYPES.get(matchType);
            if (this.matchType == null) {
                throw illegalMatchType(BytesRefs.toString(matchType));
            }
        }
        this.options = OptionParser.parse(this.matchType, options);
    }

    public Query query(Map<String, Object> fields, BytesRef queryString) throws IOException {
        assert fields.size() == 1 : "number of fields must be 1";
        Map.Entry<String, Object> entry = fields.entrySet().iterator().next();
        Query query = singleQueryAndApply(
            matchType.matchQueryType(), entry.getKey(), queryString, floatOrNull(entry.getValue()));
        Float boost = this.options.boost();
        if (boost != null) {
            return new BoostQuery(query, boost);
        }
        return query;
    }

    IllegalArgumentException illegalMatchType(String matchType) {
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

    @Nullable
    static Float floatOrNull(Object value) {
        if (value == null) {
            return null;
        }
        return ((Number) value).floatValue();
    }

    private Query singleQuery(MatchQuery.Type type, String fieldName, BytesRef queryString) {
        String field;
        MappedFieldType fieldType = mapperService.fullName(fieldName);
        if (fieldType == null) {
            field = fieldName;
        } else {
            field = fieldType.name();
        }

        if (fieldType != null && !fieldType.tokenized() && !forceAnalyzeQueryString()) {
            try {
                return fieldType.termQuery(queryString, null);
            } catch (RuntimeException e) {
                return null;
            }
        }

        Analyzer analyzer = getAnalyzer(fieldType);
        InnerQueryBuilder builder = new InnerQueryBuilder(analyzer, fieldType);

        Query query;
        switch (type) {
            case BOOLEAN:
                if (options.commonTermsCutoff() == null) {
                    query = builder.createBooleanQuery(field, BytesRefs.toString(queryString), options.operator());
                } else {
                    query = builder.createCommonTermsQuery(
                        field,
                        BytesRefs.toString(queryString),
                        options.operator(),
                        options.operator(),
                        options.commonTermsCutoff(),
                        fieldType
                    );
                }
                break;
            case PHRASE:
                query = builder.createPhraseQuery(field, BytesRefs.toString(queryString), options.phraseSlop());
                break;
            case PHRASE_PREFIX:
                query = builder.createPhrasePrefixQuery(
                    field,
                    BytesRefs.toString(queryString),
                    options.phraseSlop(),
                    options.maxExpansions()
                );
                break;
            default:
                throw new IllegalArgumentException("invalid type: " + type.toString());
        }

        if (query == null) {
            return zeroTermsQuery();
        } else {
            return query;
        }
    }

    Query singleQueryAndApply(MatchQuery.Type type,
                              String fieldName,
                              BytesRef queryString,
                              Float boost) {
        Query query = singleQuery(type, fieldName, queryString);
        if (query instanceof BooleanQuery && !((BooleanQuery) query).isCoordDisabled()) {
            query = Queries.applyMinimumShouldMatch((BooleanQuery) query, options.minimumShouldMatch());
        } else if (query instanceof ExtendedCommonTermsQuery) {
            ((ExtendedCommonTermsQuery)query).setLowFreqMinimumNumberShouldMatch(options.minimumShouldMatch());
        }
        if (boost != null && query != null) {
            return new BoostQuery(query, boost);
        }
        return query;
    }

    private Query zeroTermsQuery() {
        return options.zeroTermsQuery() == MatchQuery.ZeroTermsQuery.NONE ?
            Queries.newMatchNoDocsQuery("ZeroTermsQuery") :
            Queries.newMatchAllQuery();
    }

    Analyzer getAnalyzer(MappedFieldType fieldType) {
        if (options.analyzer() == null) {
            if (fieldType != null) {
                if (fieldType.searchAnalyzer() != null) {
                    return fieldType.searchAnalyzer();
                }
            }
            return mapperService.searchAnalyzer();
        }

        Analyzer analyzer = mapperService.analysisService().analyzer(options.analyzer());
        if (analyzer == null) {
            throw new IllegalArgumentException(
                String.format(Locale.ENGLISH, "Analyzer \"%s\" not found.", options.analyzer()));
        }
        return analyzer;
    }


    private class InnerQueryBuilder extends QueryBuilder {


        @Nullable
        private final MappedFieldType fieldType;

        InnerQueryBuilder(Analyzer analyzer, @Nullable MappedFieldType fieldType) {
            super(analyzer);
            this.fieldType = fieldType;
        }

        @Override
        protected Query newTermQuery(Term term) {
            return blendTermQuery(term, fieldType);
        }

        Query createCommonTermsQuery(String field,
                                     String queryText,
                                     BooleanClause.Occur highFreqOccur,
                                     BooleanClause.Occur lowFreqOccur,
                                     Float maxTermFrequency,
                                     MappedFieldType mapper) {
            Query booleanQuery = createBooleanQuery(field, queryText, lowFreqOccur);
            if (booleanQuery != null && booleanQuery instanceof BooleanQuery) {
                BooleanQuery bq = (BooleanQuery) booleanQuery;
                ExtendedCommonTermsQuery query = new ExtendedCommonTermsQuery(
                    highFreqOccur, lowFreqOccur, maxTermFrequency,
                    ((BooleanQuery) booleanQuery).isCoordDisabled(), mapper);
                for (BooleanClause clause : bq.clauses()) {
                    if (!(clause.getQuery() instanceof TermQuery)) {
                        return booleanQuery;
                    }
                    query.add(((TermQuery) clause.getQuery()).getTerm());
                }
                return query;
            }
            return booleanQuery;
        }

        Query createPhrasePrefixQuery(String field, String queryText, int phraseSlop, int maxExpansions) {
            final Query query = createFieldQuery(getAnalyzer(), BooleanClause.Occur.MUST, field, queryText, true, phraseSlop);
            final MultiPhrasePrefixQuery prefixQuery = new MultiPhrasePrefixQuery();
            prefixQuery.setMaxExpansions(maxExpansions);
            prefixQuery.setSlop(phraseSlop);
            if (query instanceof PhraseQuery) {
                PhraseQuery pq = (PhraseQuery) query;
                Term[] terms = pq.getTerms();
                int[] positions = pq.getPositions();
                for (int i = 0; i < terms.length; i++) {
                    prefixQuery.add(new Term[]{terms[i]}, positions[i]);
                }
                return prefixQuery;
            } else if (query instanceof MultiPhraseQuery) {
                MultiPhraseQuery pq = (MultiPhraseQuery) query;
                Term[][] terms = pq.getTermArrays();
                int[] positions = pq.getPositions();
                for (int i = 0; i < terms.length; i++) {
                    prefixQuery.add(terms[i], positions[i]);
                }
                return prefixQuery;
            } else if (query instanceof TermQuery) {
                prefixQuery.add(((TermQuery) query).getTerm());
                return prefixQuery;
            }
            return query;
        }
    }

    protected Query blendTermQuery(Term term, MappedFieldType fieldType) {
        Fuzziness fuzziness = options.fuzziness();
        if (fuzziness != null) {
            if (fieldType != null) {
                Query query = fieldType.fuzzyQuery(
                    term.text(), fuzziness, options.prefixLength(), options.maxExpansions(), options.transpositions());
                if (query instanceof FuzzyQuery) {
                    QueryParsers.setRewriteMethod(((FuzzyQuery) query), options.rewriteMethod());
                }
                return query;
            }
            int edits = fuzziness.asDistance(term.text());
            FuzzyQuery query = new FuzzyQuery(
                term, edits, options.prefixLength(), options.maxExpansions(), options.transpositions());
            QueryParsers.setRewriteMethod(query, options.rewriteMethod());
            return query;
        }
        if (fieldType != null) {
            Query termQuery = fieldType.queryStringTermQuery(term);
            if (termQuery != null) {
                return termQuery;
            }
        }
        return new TermQuery(term);
    }

    protected boolean forceAnalyzeQueryString() {
        return false;
    }
}
