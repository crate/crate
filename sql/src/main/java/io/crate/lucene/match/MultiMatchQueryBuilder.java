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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.BlendedTermQuery;
import org.apache.lucene.search.*;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MultiMatchQueryBuilder extends MatchQueryBuilder {

    private GroupQueryBuilder builder = null;

    public MultiMatchQueryBuilder(MapperService mapperService,
                                  @Nullable BytesRef matchType,
                                  @Nullable Map options) throws IOException {
        super(mapperService, matchType, options);
    }

    @Override
    public Query query(Map<String, Object> fields, BytesRef queryString) throws IOException {
        final float tieBreaker = options.tieBreaker() == null ? matchType.tieBreaker() : options.tieBreaker();
        switch (matchType) {
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
                throw illegalMatchType(matchType.toString());
        }
        List<Query> queries = builder.buildGroupedQueries(matchType, fields, queryString);
        Query query = builder.combineGrouped(queries);
        builder = null;
        Float boost = options.boost();
        if (boost != null) {
            return new BoostQuery(query, boost);
        }
        return query;
    }

    @Override
    protected Query blendTermQuery(Term term, MappedFieldType fieldType) {
        if (builder == null) {
            super.blendTermQuery(term, fieldType);
        }
        return builder.blendTerm(term, fieldType);
    }

    @Override
    protected boolean forceAnalyzeQueryString() {
        return builder == null ? super.forceAnalyzeQueryString() : builder.forceAnalyzeQueryString();
    }

    private class GroupQueryBuilder {
        private final boolean groupDismax;
        protected final float tieBreaker;

        protected GroupQueryBuilder(float tieBreaker) {
            this(tieBreaker != 1.0f, tieBreaker);
        }

        protected GroupQueryBuilder(boolean groupDismax, float tieBreaker) {
            this.groupDismax = groupDismax;
            this.tieBreaker = tieBreaker;
        }

        protected List<Query> buildGroupedQueries(org.elasticsearch.index.query.MultiMatchQueryBuilder.Type type,
                                                  Map<String, Object> fieldNames,
                                                  BytesRef queryString) throws IOException {
            List<Query> queries = new ArrayList<>();
            for (Map.Entry<String, Object> entry : fieldNames.entrySet()) {
                Query query = singleQueryAndApply(
                    type.matchQueryType(), entry.getKey(), queryString, floatOrNull(entry.getValue()));
                if (query != null) {
                    queries.add(query);
                }
            }
            return queries;
        }

        protected Query combineGrouped(List<Query> queries) {
            if (queries == null || queries.isEmpty()) {
                return null;
            }
            if (queries.size() == 1) {
                return queries.get(0);
            }
            if (groupDismax) {
                return new DisjunctionMaxQuery(queries, tieBreaker);
            } else {
                BooleanQuery.Builder booleanQuery = new BooleanQuery.Builder();
                for (Query query : queries) {
                    booleanQuery.add(query, BooleanClause.Occur.SHOULD);
                }
                return booleanQuery.build();
            }
        }

        protected Query blendTerm(Term term, MappedFieldType fieldType) {
            return MultiMatchQueryBuilder.super.blendTermQuery(term, fieldType);
        }

        protected boolean forceAnalyzeQueryString() {
            return false;
        }
    }

    private class CrossFieldsQueryBuilder extends GroupQueryBuilder {
        private FieldAndFieldType[] blendedFields;

        private CrossFieldsQueryBuilder(float tieBreaker) {
            super(false, tieBreaker);
        }

        @Override
        protected List<Query> buildGroupedQueries(org.elasticsearch.index.query.MultiMatchQueryBuilder.Type type,
                                                  Map<String, Object> fieldNames,
                                                  BytesRef queryString) throws IOException {
            Map<Analyzer, List<FieldAndFieldType>> groups = new HashMap<>();
            List<Tuple<String, Float>> missing = new ArrayList<>();
            for (Map.Entry<String, Object> entry : fieldNames.entrySet()) {
                String name = entry.getKey();
                MappedFieldType fieldType = mapperService.fullName(name);
                if (fieldType != null) {
                    Analyzer actualAnalyzer = getAnalyzer(fieldType);
                    name = fieldType.name();
                    if (!groups.containsKey(actualAnalyzer)) {
                        groups.put(actualAnalyzer, new ArrayList<>());
                    }
                    Float boost = floatOrNull(entry.getValue());
                    boost = boost == null ? Float.valueOf(1.0f) : boost;
                    groups.get(actualAnalyzer).add(new FieldAndFieldType(name, fieldType, boost));
                } else {
                    missing.add(new Tuple<>(name, floatOrNull(entry.getValue())));
                }
            }
            List<Query> queries = new ArrayList<>();
            for (Tuple<String, Float> tuple : missing) {
                Query q = singleQueryAndApply(type.matchQueryType(), tuple.v1(), queryString, tuple.v2());
                if (q != null) {
                    queries.add(q);
                }
            }
            for (List<FieldAndFieldType> group : groups.values()) {
                if (group.size() > 1) {
                    blendedFields = new FieldAndFieldType[group.size()];
                    int i = 0;
                    for (FieldAndFieldType fieldAndFieldType : group) {
                        blendedFields[i++] = fieldAndFieldType;
                    }
                } else {
                    blendedFields = null;
                }
                final FieldAndFieldType fieldAndMapper = group.get(0);
                Query q = singleQueryAndApply(
                    type.matchQueryType(), fieldAndMapper.field, queryString, fieldAndMapper.boost);
                if (q != null) {
                    queries.add(q);
                }
            }
            return queries;
        }

        @Override
        protected boolean forceAnalyzeQueryString() {
            return blendedFields != null;
        }

        @Override
        protected Query blendTerm(Term term, MappedFieldType fieldType) {
            if (blendedFields == null) {
                return super.blendTerm(term, fieldType);
            }
            final Term[] terms = new Term[blendedFields.length];
            float[] blendedBoost = new float[blendedFields.length];
            for (int i = 0; i < blendedFields.length; i++) {
                terms[i] = blendedFields[i].newTerm(term.text());
                blendedBoost[i] = blendedFields[i].boost;
            }
            if (options.commonTermsCutoff() != null) {
                return BlendedTermQuery.commonTermsBlendedQuery(
                    terms, blendedBoost, false, options.commonTermsCutoff());
            }

            if (tieBreaker == 1.0f) {
                return BlendedTermQuery.booleanBlendedQuery(terms, blendedBoost, false);
            }
            return BlendedTermQuery.dismaxBlendedQuery(terms, blendedBoost, tieBreaker);
        }
    }

    private static final class FieldAndFieldType {
        final String field;
        final MappedFieldType fieldType;
        final float boost;

        private FieldAndFieldType(String field, MappedFieldType fieldType, float boost) {
            this.field = field;
            this.fieldType = fieldType;
            this.boost = boost;
        }

        private Term newTerm(String value) {
            try {
                final BytesRef bytesRef = (BytesRef) fieldType.valueForSearch(value);
                return new Term(field, bytesRef);
            } catch (Exception ex) {
                // we can't parse it just use the incoming value -- it will
                // just have a DF of 0 at the end of the day and will be ignored
            }
            return new Term(field, value);
        }
    }
}
