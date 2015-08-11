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

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.ExtendedCommonTermsQuery;
import org.apache.lucene.search.*;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.QueryBuilder;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.lucene.search.MultiPhrasePrefixQuery;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.lucene.search.XFilteredQuery;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.cache.IndexCache;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.query.support.QueryParsers;
import org.elasticsearch.index.search.MatchQuery;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class MatchQueryBuilder {

    protected final IndexCache indexCache;
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
                             IndexCache indexCache,
                             @Nullable BytesRef matchType,
                             @Nullable Map options) throws IOException {
        this.mapperService = mapperService;
        this.indexCache = indexCache;
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
        assert fields.size() == 1;
        Map.Entry<String, Object> entry = fields.entrySet().iterator().next();
        Query query = singleQueryAndApply(
            matchType.matchQueryType(), entry.getKey(), queryString, floatOrNull(entry.getValue()));
        Float boost = this.options.boost();
        if (boost != null) {
            query.setBoost(boost);
        }
        return query;
    }

    protected IllegalArgumentException illegalMatchType(String matchType) {
        throw new IllegalArgumentException(String.format(
                "Unknown matchType \"%s\". Possible matchTypes are: %s", matchType,
                Joiner.on(", ").join(Iterables.transform(SUPPORTED_TYPES.keySet(), new Function<BytesRef, String>() {
                            @Nullable
                            @Override
                            public String apply(@Nullable BytesRef input) {
                                return BytesRefs.toString(input);
                            }
                        }

                ))));
    }

    @Nullable
    protected static Float floatOrNull(Object value) {
        if (value == null) {
            return null;
        }
        return ((Number) value).floatValue();
    }

    protected Query singleQuery(MatchQuery.Type type, String fieldName, BytesRef queryString) {
        FieldMapper mapper = null;
        final String field;
        MapperService.SmartNameFieldMappers smartNameFieldMappers = mapperService.smartName(fieldName);
        if (smartNameFieldMappers != null && smartNameFieldMappers.hasMapper()) {
            mapper = smartNameFieldMappers.mapper();
            field = mapper.names().indexName();
        } else {
            field = fieldName;
        }

        if (mapper != null && mapper.useTermQueryWithQueryString() && !forceAnalyzeQueryString()) {
            if (smartNameFieldMappers.explicitTypeInNameWithDocMapper()) {
                String[] previousTypes = QueryParseContext.setTypesWithPrevious(
                        new String[]{smartNameFieldMappers.docMapper().type()});
                try {
                    return wrapSmartNameQuery(
                            mapper.termQuery(queryString, null),
                            smartNameFieldMappers,
                            indexCache
                    );
                } catch (RuntimeException e) {
                    return null;
                } finally {
                    QueryParseContext.setTypes(previousTypes);
                }
            } else {
                try {
                    return wrapSmartNameQuery(
                            mapper.termQuery(queryString, null),
                            smartNameFieldMappers,
                            indexCache
                    );
                } catch (RuntimeException e) {
                    return null;
                }
            }
        }

        Analyzer analyzer = getAnalyzer(mapper, smartNameFieldMappers);
        InnerQueryBuilder builder = new InnerQueryBuilder(analyzer, mapper);

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
                            mapper
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
            return wrapSmartNameQuery(query, smartNameFieldMappers, indexCache);
        }
    }

    private static Query wrapSmartNameQuery(Query query,
                                            @Nullable MapperService.SmartNameFieldMappers smartNameFieldMappers,
                                            IndexCache indexCache) {
        if (query == null) {
            return null;
        }
        if (smartNameFieldMappers == null) {
            return query;
        }
        if (!smartNameFieldMappers.explicitTypeInNameWithDocMapper()) {
            return query;
        }
        DocumentMapper documentMapper = smartNameFieldMappers.docMapper();
        return new XFilteredQuery(query, indexCache.filter().cache(documentMapper.typeFilter()));
    }

    protected Query singleQueryAndApply(MatchQuery.Type type,
                                        String fieldName,
                                        BytesRef queryString,
                                        Float boost) {
        Query query = singleQuery(type, fieldName, queryString);
        if (query instanceof BooleanQuery) {
            Queries.applyMinimumShouldMatch((BooleanQuery) query, options.minimumShouldMatch());
        }
        if (boost != null && query != null) {
            query.setBoost(boost);
        }
        return query;
    }

    private Query zeroTermsQuery() {
        return options.zeroTermsQuery() == MatchQuery.ZeroTermsQuery.NONE ?
                Queries.newMatchNoDocsQuery() :
                Queries.newMatchAllQuery();
    }


    protected Analyzer getAnalyzer(@Nullable FieldMapper mapper,
                                   MapperService.SmartNameFieldMappers smartNameFieldMappers) {
        Analyzer analyzer = null;
        if (options.analyzer() == null) {
            if (mapper != null) {
                analyzer = mapper.searchAnalyzer();
            }
            if (analyzer == null && smartNameFieldMappers != null) {
                analyzer = smartNameFieldMappers.searchAnalyzer();
            }
            if (analyzer == null) {
                analyzer = mapperService.searchAnalyzer();
            }
        } else {
            analyzer = mapperService.analysisService().analyzer(options.analyzer());
            if (analyzer == null) {
                throw new IllegalArgumentException(
                        String.format("Analyzer \"%s\" not found.", options.analyzer()));
            }
        }
        return analyzer;
    }



    private class InnerQueryBuilder extends QueryBuilder {

        @Nullable
        private final FieldMapper mapper;

        public InnerQueryBuilder(Analyzer analyzer, @Nullable FieldMapper mapper) {
            super(analyzer);
            this.mapper = mapper;
        }

        @Override
        protected Query newTermQuery(Term term) {
            return blendTermQuery(term, mapper);
        }

        public Query createCommonTermsQuery(String field,
                                            String queryText,
                                            BooleanClause.Occur highFreqOccur,
                                            BooleanClause.Occur lowFreqOccur,
                                            Float maxTermFrequency,
                                            FieldMapper mapper) {
            Query booleanQuery = createBooleanQuery(field, queryText, lowFreqOccur);
            if (booleanQuery != null && booleanQuery instanceof BooleanQuery) {
                BooleanQuery bq = (BooleanQuery) booleanQuery;
                ExtendedCommonTermsQuery query = new ExtendedCommonTermsQuery(
                        highFreqOccur, lowFreqOccur, maxTermFrequency,
                        ((BooleanQuery)booleanQuery).isCoordDisabled(), mapper);
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

        public Query createPhrasePrefixQuery(String field, String queryText, int phraseSlop, int maxExpansions) {
            final Query query = createFieldQuery(getAnalyzer(), BooleanClause.Occur.MUST, field, queryText, true, phraseSlop);
            final MultiPhrasePrefixQuery prefixQuery = new MultiPhrasePrefixQuery();
            prefixQuery.setMaxExpansions(maxExpansions);
            prefixQuery.setSlop(phraseSlop);
            if (query instanceof PhraseQuery) {
                PhraseQuery pq = (PhraseQuery)query;
                Term[] terms = pq.getTerms();
                int[] positions = pq.getPositions();
                for (int i = 0; i < terms.length; i++) {
                    prefixQuery.add(new Term[] {terms[i]}, positions[i]);
                }
                return prefixQuery;
            } else if (query instanceof MultiPhraseQuery) {
                MultiPhraseQuery pq = (MultiPhraseQuery)query;
                List<Term[]> terms = pq.getTermArrays();
                int[] positions = pq.getPositions();
                for (int i = 0; i < terms.size(); i++) {
                    prefixQuery.add(terms.get(i), positions[i]);
                }
                return prefixQuery;
            } else if (query instanceof TermQuery) {
                prefixQuery.add(((TermQuery) query).getTerm());
                return prefixQuery;
            }
            return query;
        }
    }

    protected Query blendTermQuery(Term term, FieldMapper mapper) {
        Fuzziness fuzziness = options.fuzziness();
        if (fuzziness != null) {
            int edits = fuzziness.asDistance(term.text());
            FuzzyQuery query = new FuzzyQuery(
                    term, edits, options.prefixLength(), options.maxExpansions(), options.transpositions());
            QueryParsers.setRewriteMethod(query, options.rewriteMethod());
            return query;
        }
        if (mapper != null) {
            Query termQuery = mapper.queryStringTermQuery(term);
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
