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

package io.crate.lucene;

import com.google.common.base.Objects;
import io.crate.operation.predicate.MatchPredicate;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.ExtendedCommonTermsQuery;
import org.apache.lucene.search.*;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.QueryBuilder;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.search.MatchQuery;
import org.elasticsearch.search.internal.SearchContext;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MatchQueryBuilder {

    private final SearchContext searchContext;
    private final Query query;
    private Float commonTermsCutoff = null;
    private MatchQuery.ZeroTermsQuery zeroTermsQuery;
    private BooleanClause.Occur occur = BooleanClause.Occur.SHOULD;
    private Float groupTieBreaker = null;
    private String minimumShouldMatch;
    private String analyzer;

    private static class Types {
        private static final BytesRef BOOLEAN = new BytesRef("boolean");
        private static final BytesRef PHRASE = new BytesRef("phrase");
        private static final BytesRef PHRASE_PREFIX = new BytesRef("phrase_prefix");
    }

    public MatchQueryBuilder(SearchContext searchContext,
                             Map<String, Object> fields,
                             BytesRef queryString,
                             @Nullable BytesRef matchType,
                             Map options) throws IOException {
        this.searchContext = searchContext;
        parseOptions(options);

        if (fields.size() == 1) {
            Map.Entry<String, Object> field = fields.entrySet().iterator().next();
            MatchQuery.Type type = parseSingleType(matchType);
            query = singleQueryAndApply(type, field.getKey(), queryString, toFloat(field.getValue()));
        } else {
            matchType =  Objects.firstNonNull(matchType, MatchPredicate.DEFAULT_MATCH_TYPE);
            MultiMatchQueryBuilder.Type type = MultiMatchQueryBuilder.Type.parse(BytesRefs.toString(matchType));
            query = multiQuery(type, fields, queryString);
        }

        Number boost = (Number) options.get("boost");
        if (boost != null) {
            query.setBoost(boost.floatValue());
        }
    }

    @Nullable
    private static Float toFloat(Object value) {
        if (value == null) {
            return null;
        }
        return ((Number) value).floatValue();
    }

    private MatchQuery.Type parseSingleType(BytesRef matchType) {
        if (matchType == null || matchType.equals(Types.BOOLEAN)) {
            return MatchQuery.Type.BOOLEAN;
        }

        // TODO: remove this one, shouldn't be necessary
        if (matchType.equals(MatchPredicate.DEFAULT_MATCH_TYPE)) {
            return MatchQuery.Type.BOOLEAN;
        }

        if (matchType.equals(Types.PHRASE)) {
            return MatchQuery.Type.PHRASE;
        }
        if (matchType.equals(Types.PHRASE_PREFIX)) {
            return MatchQuery.Type.PHRASE_PREFIX;
        }

        throw new IllegalArgumentException(String.format(
                "No match_type named \"%s\" for match predicate found.", BytesRefs.toString(matchType)));
    }

    private void parseOptions(Map options) {
        zeroTermsQuery((String) options.get("zero_terms_query"));
        commonTermsCutoff((Float) options.get("cutoff_frequency"));
        operator((String) options.get("operator"));
        minimumShouldMatch((String) options.get("minimum_should_match"));
        this.analyzer = (String) options.get("analyzer");
    }

    private void minimumShouldMatch(String minimumShouldMatch) {
        this.minimumShouldMatch = minimumShouldMatch;
    }

    private void operator(@Nullable String operator) {
        if (operator == null) {
            return;
        }

        if ("or".equalsIgnoreCase(operator)) {
            this.occur = BooleanClause.Occur.SHOULD;
        } else if ("and".equalsIgnoreCase(operator)) {
            this.occur = BooleanClause.Occur.MUST;
        } else {
            throw new IllegalArgumentException(String.format(
                    "Invalid operator option \"%s\" in match predicate. Supported values are \"or\" and \"and\"", operator));
        }
    }

    private void commonTermsCutoff(Float cutOffFrequency) {
        commonTermsCutoff = cutOffFrequency;
    }

    private void zeroTermsQuery(String zeroTermsQuery) {
        if (zeroTermsQuery == null || "none".equalsIgnoreCase(zeroTermsQuery)) {
            this.zeroTermsQuery = MatchQuery.ZeroTermsQuery.NONE;
        } else if ("all".equalsIgnoreCase(zeroTermsQuery)) {
            this.zeroTermsQuery = MatchQuery.ZeroTermsQuery.ALL;
        } else {
            throw new IllegalArgumentException(
                    "Unsupported zero_terms_query option. Valid are \"none\" or \"all\"");
        }
    }

    private Query multiQuery(MultiMatchQueryBuilder.Type type,
                            Map<String, Object> fields,
                            BytesRef queryString) throws IOException {
        final float tieBreaker = groupTieBreaker == null ? type.tieBreaker() : groupTieBreaker;

        GroupQueryBuilder builder;
        switch (type) {
            case PHRASE:
            case PHRASE_PREFIX:
            case BEST_FIELDS:
            case MOST_FIELDS:
                builder = new GroupQueryBuilder(tieBreaker);
                break;
            case CROSS_FIELDS:
                builder = new CrossFieldsQueryBuilder(tieBreaker);
                break;
            default:
                throw new IllegalArgumentException(String.format(
                        "type \"%s\" not supported", type));
        }

        List<Query> queries = builder.buildGroupedQueries(type, fields, queryString);
        return builder.combineGrouped(queries);
    }

    private Query singleQuery(MatchQuery.Type type, String fieldName, BytesRef queryString) {
        FieldMapper mapper = null;
        final String field;
        MapperService.SmartNameFieldMappers smartNameFieldMappers = searchContext.smartFieldMappers(fieldName);
        if (smartNameFieldMappers != null && smartNameFieldMappers.hasMapper()) {
            mapper = smartNameFieldMappers.mapper();
            field = mapper.names().indexName();
        } else {
            field = fieldName;
        }

        if (mapper != null && mapper.useTermQueryWithQueryString()) { // !forceAnalyzeQueryString?
        }
        Analyzer analyzer = getAnalyzer(mapper, smartNameFieldMappers);
        InnerMatchQueryBuilder builder = new InnerMatchQueryBuilder(analyzer);

        Query query;
        switch (type) {
            case BOOLEAN:
                if (commonTermsCutoff == null) {
                    query = builder.createBooleanQuery(field, BytesRefs.toString(queryString), occur);
                } else {
                    query = builder.createCommonTermsQuery(
                            field, BytesRefs.toString(queryString), occur, occur, commonTermsCutoff, mapper);
                }
                break;

            case PHRASE:
                throw new UnsupportedOperationException("NYI match phrase");
            case PHRASE_PREFIX:
                throw new UnsupportedOperationException("NYI match phrase prefix");
            default:
                throw new UnsupportedOperationException("invalid type");
        }

        if (query == null) {
            return zeroTermsQuery();
        } else {
            return query; // TODO: add filter cache
        }
    }

    private Query singleQueryAndApply(MatchQuery.Type type,
                                      String fieldName,
                                      BytesRef queryString,
                                      Float boost) {
        Query query = singleQuery(type, fieldName, queryString);
        if (query instanceof BooleanQuery) {
            Queries.applyMinimumShouldMatch((BooleanQuery) query, minimumShouldMatch);
        }
        if (boost != null && query != null) {
            query.setBoost(boost);
        }
        return query;
    }

    private Query zeroTermsQuery() {
        return zeroTermsQuery == MatchQuery.ZeroTermsQuery.NONE ?
                Queries.newMatchNoDocsQuery() :
                Queries.newMatchAllQuery();
    }

    private Analyzer getAnalyzer(@Nullable FieldMapper mapper,
                                 MapperService.SmartNameFieldMappers smartNameFieldMappers) {
        Analyzer analyzer = null;
        if (this.analyzer == null) {
            if (mapper != null) {
                analyzer = mapper.searchAnalyzer();
            }
            if (analyzer == null && smartNameFieldMappers != null) {
                analyzer = smartNameFieldMappers.searchAnalyzer();
            }
            if (analyzer == null) {
                analyzer = searchContext.mapperService().searchAnalyzer();
            }
        } else {
            analyzer = searchContext.mapperService().analysisService().analyzer(this.analyzer);
            if (analyzer == null) {
                throw new IllegalArgumentException(
                        String.format("Analyzer \"%s\" not found.", this.analyzer));
            }
        }
        return analyzer;
    }

    private class GroupQueryBuilder {
        private final boolean groupDismax;
        private final float tieBreaker;

        public GroupQueryBuilder(float tieBreaker) {
            this(tieBreaker != 1.0f, tieBreaker);
        }

        private GroupQueryBuilder(boolean groupDismax, float tieBreaker) {
            this.groupDismax = groupDismax;
            this.tieBreaker = tieBreaker;
        }

        public List<Query> buildGroupedQueries(MultiMatchQueryBuilder.Type type,
                                               Map<String, Object> fieldNames,
                                               BytesRef queryString) throws IOException {
            List<Query> queries = new ArrayList<>();
            for (Map.Entry<String, Object> entry : fieldNames.entrySet()) {
                Query query = singleQueryAndApply(
                        type.matchQueryType(), entry.getKey(), queryString, toFloat(entry.getValue()));
                if (query != null) {
                    queries.add(query);
                }
            }
            return queries;
        }

        public Query combineGrouped(List<Query> queries) {
            if (queries == null || queries.isEmpty()) {
                return null;
            }
            if (queries.size() == 1) {
                return queries.get(0);
            }
            if (groupDismax) {
                return new DisjunctionMaxQuery(queries, tieBreaker);
            } else {
                final BooleanQuery booleanQuery = new BooleanQuery();
                for (Query query : queries) {
                    booleanQuery.add(query, BooleanClause.Occur.SHOULD);
                }
                return booleanQuery;
            }
        }
    }

    private class CrossFieldsQueryBuilder extends GroupQueryBuilder {
        private FieldAndMapper[] blendedFields;

        public CrossFieldsQueryBuilder(float tieBreaker) {
            super(false, tieBreaker);
        }

        @Override
        public List<Query> buildGroupedQueries(MultiMatchQueryBuilder.Type type,
                                               Map<String, Object> fieldNames,
                                               BytesRef queryString) throws IOException {
            Map<Analyzer, List<FieldAndMapper>> groups = new HashMap<>();
            List<Tuple<String, Float>> missing = new ArrayList<>();
            for (Map.Entry<String, Object> entry : fieldNames.entrySet()) {
                String name = entry.getKey();
                MapperService.SmartNameFieldMappers smartNameFieldMappers = searchContext.smartFieldMappers(name);
                if (smartNameFieldMappers != null && smartNameFieldMappers.hasMapper()) {
                    Analyzer actualAnalyzer = getAnalyzer(smartNameFieldMappers.mapper(), smartNameFieldMappers);
                    name = smartNameFieldMappers.mapper().names().indexName();
                    if (!groups.containsKey(actualAnalyzer)) {
                        groups.put(actualAnalyzer, new ArrayList<FieldAndMapper>());
                    }
                    Float boost = toFloat(entry.getValue());
                    boost = boost == null ? Float.valueOf(1.0f) : boost;
                    groups.get(actualAnalyzer).add(new FieldAndMapper(name, smartNameFieldMappers.mapper(), boost));
                } else {
                    missing.add(new Tuple(name, entry.getValue()));
                }
            }
            List<Query> queries = new ArrayList<>();
            for (Tuple<String, Float> tuple : missing) {
                Query q = singleQueryAndApply(
                        type.matchQueryType(), tuple.v1(), queryString, tuple.v2());
                if (q != null) {
                    queries.add(q);
                }
            }
            for (List<FieldAndMapper> group : groups.values()) {
                if (group.size() > 1) {
                    blendedFields = new FieldAndMapper[group.size()];
                    int i = 0;
                    for (FieldAndMapper fieldAndMapper : group) {
                        blendedFields[i++] = fieldAndMapper;
                    }
                } else {
                    blendedFields = null;
                }
                final FieldAndMapper fieldAndMapper= group.get(0);
                Query q = singleQueryAndApply(
                        type.matchQueryType(), fieldAndMapper.field, queryString, fieldAndMapper.boost);
                if (q != null) {
                    queries.add(q);
                }
            }
            return queries;
        }
    }

    private static class FieldAndMapper {
        private final String field;
        private final FieldMapper mapper;
        private final float boost;

        private FieldAndMapper(String field, FieldMapper mapper, float boost) {
            this.field = field;
            this.mapper = mapper;
            this.boost = boost;
        }

        public Term newTerm(String value) {
            try {
                final BytesRef bytesRef = mapper.indexedValueForSearch(value);
                return new Term(field, bytesRef);
            } catch (Exception ex) {
                // we can't parse it just use the incoming value -- it will
                // just have a DF of 0 at the end of the day and will be ignored
            }
            return new Term(field, value);
        }
    }

    private static class InnerMatchQueryBuilder extends QueryBuilder {

        public InnerMatchQueryBuilder(Analyzer analyzer) {
            super(analyzer);
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
    }

    public Query query() {
        return query;
    }

}
