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

package org.elasticsearch.index.search;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.index.query.MultiMatchQueryType;

import io.crate.lucene.BlendedTermQuery;
import io.crate.lucene.LuceneQueryBuilder;
import io.crate.lucene.match.ParsedOptions;
import io.crate.metadata.IndexReference;
import io.crate.metadata.Reference;

public class MultiMatchQuery extends MatchQuery {

    public MultiMatchQuery(LuceneQueryBuilder.Context context, ParsedOptions parsedOptions) {
        super(context, parsedOptions);
    }

    private Query parseAndApply(Type type, String fieldName, Object value, String minimumShouldMatch, Float boostValue) {
        Query query = parse(type, fieldName, value);
        query = Queries.maybeApplyMinimumShouldMatch(query, minimumShouldMatch);
        if (query != null && boostValue != null && boostValue != DEFAULT_BOOST && query instanceof MatchNoDocsQuery == false) {
            query = new BoostQuery(query, boostValue);
        }
        return query;
    }

    public Query parse(MultiMatchQueryType type, Map<String, Float> fieldNames, Object value, String minimumShouldMatch) {
        assert fieldNames.size() > 1 : "Must have more than one fieldName if using MultiMatchQuery";
        Float groupTieBreaker = parsedOptions.tieBreaker();
        final float tieBreaker = groupTieBreaker == null ? type.tieBreaker() : groupTieBreaker;
        switch (type) {
            case PHRASE:
            case PHRASE_PREFIX:
            case BEST_FIELDS:
            case MOST_FIELDS:
                queryBuilder = new QueryBuilder(tieBreaker);
                break;
            case CROSS_FIELDS:
                queryBuilder = new CrossFieldsQueryBuilder(tieBreaker);
                break;
            default:
                throw new IllegalStateException("No such type: " + type);
        }
        final List<? extends Query> queries = queryBuilder.buildGroupedQueries(type, fieldNames, value, minimumShouldMatch);
        return queryBuilder.combineGrouped(queries);
    }

    private QueryBuilder queryBuilder;

    public class QueryBuilder {

        protected final float tieBreaker;

        public QueryBuilder(float tieBreaker) {
            this.tieBreaker = tieBreaker;
        }

        public List<Query> buildGroupedQueries(MultiMatchQueryType type,
                                               Map<String,
                                               Float> fieldNames,
                                               Object value, String minimumShouldMatch) {
            List<Query> queries = new ArrayList<>();
            for (String fieldName : fieldNames.keySet()) {
                Float boostValue = fieldNames.get(fieldName);
                Query query = parseGroup(type.matchQueryType(), fieldName, boostValue, value, minimumShouldMatch);
                if (query != null) {
                    queries.add(query);
                }
            }
            return queries;
        }

        public Query parseGroup(Type type, String field, Float boostValue, Object value, String minimumShouldMatch) {
            return parseAndApply(type, field, value, minimumShouldMatch, boostValue);
        }

        private Query combineGrouped(List<? extends Query> groupQuery) {
            if (groupQuery == null || groupQuery.isEmpty()) {
                return zeroTermsQuery();
            }
            if (groupQuery.size() == 1) {
                return groupQuery.get(0);
            }
            List<Query> queries = new ArrayList<>();
            for (Query query : groupQuery) {
                queries.add(query);
            }
            return new DisjunctionMaxQuery(queries, tieBreaker);
        }

        public Query blendTerm(Term term, Reference ref) {
            return MultiMatchQuery.super.blendTermQuery(term, ref);
        }

        public Query blendTerms(Term[] terms, Reference ref) {
            return MultiMatchQuery.super.blendTermsQuery(terms, ref);
        }

        public Query blendPhrase(PhraseQuery query, Reference ref) {
            return MultiMatchQuery.super.blendPhraseQuery(query, ref);
        }
    }

    final class CrossFieldsQueryBuilder extends QueryBuilder {
        private RefAndBoost[] blendedFields;

        CrossFieldsQueryBuilder(float tiebreaker) {
            super(tiebreaker);
        }

        @Override
        public List<Query> buildGroupedQueries(MultiMatchQueryType type, Map<String, Float> fieldNames, Object value, String minimumShouldMatch) {
            Map<Analyzer, List<RefAndBoost>> groups = new HashMap<>();
            List<Query> queries = new ArrayList<>();
            for (Map.Entry<String, Float> entry : fieldNames.entrySet()) {
                String name = entry.getKey();
                Reference reference = context.getRef(name);
                if (reference != null) {
                    Analyzer actualAnalyzer;
                    if (reference instanceof IndexReference indexRef) {
                        actualAnalyzer = context.getAnalyzer(indexRef.analyzer());
                    } else {
                        actualAnalyzer = Lucene.KEYWORD_ANALYZER;
                    }
                    if (!groups.containsKey(actualAnalyzer)) {
                        groups.put(actualAnalyzer, new ArrayList<>());
                    }
                    Float boost = entry.getValue();
                    boost = boost == null ? Float.valueOf(1.0f) : boost;
                    groups.get(actualAnalyzer).add(new RefAndBoost(reference, boost));
                } else {
                    queries.add(new MatchNoDocsQuery("unknown field " + name));
                }
            }
            for (List<RefAndBoost> group : groups.values()) {
                if (group.size() > 1) {
                    blendedFields = new RefAndBoost[group.size()];
                    int i = 0;
                    for (RefAndBoost fieldAndFieldType : group) {
                        blendedFields[i++] = fieldAndFieldType;
                    }
                } else {
                    blendedFields = null;
                }
                /*
                 * We have to pick some field to pass through the superclass so
                 * we just pick the first field. It shouldn't matter because
                 * fields are already grouped by their analyzers/types.
                 */
                String representativeField = group.get(0).ref.storageIdent();
                Query q = parseGroup(type.matchQueryType(), representativeField, 1f, value, minimumShouldMatch);
                if (q != null) {
                    queries.add(q);
                }
            }

            return queries.isEmpty() ? null : queries;
        }

        @Override
        public Query blendTerms(Term[] terms, Reference ref) {
            if (blendedFields == null || blendedFields.length == 1) {
                return super.blendTerms(terms, ref);
            }
            BytesRef[] values = new BytesRef[terms.length];
            for (int i = 0; i < terms.length; i++) {
                values[i] = terms[i].bytes();
            }
            Float commonTermsCutoff = parsedOptions.commonTermsCutoff();
            return MultiMatchQuery.blendTerms(context, values, commonTermsCutoff, tieBreaker, blendedFields);
        }

        @Override
        public Query blendTerm(Term term, Reference ref) {
            if (blendedFields == null) {
                return super.blendTerm(term, ref);
            }
            Float commonTermsCutoff = parsedOptions.commonTermsCutoff();
            return MultiMatchQuery.blendTerm(context, term.bytes(), commonTermsCutoff, tieBreaker, blendedFields);
        }

        @Override
        public Query blendPhrase(PhraseQuery query, Reference ref) {
            if (blendedFields == null) {
                return super.blendPhrase(query, ref);
            }
            /**
             * We build phrase queries for multi-word synonyms when {@link QueryBuilder#autoGenerateSynonymsPhraseQuery} is true.
             */
            return MultiMatchQuery.blendPhrase(query, tieBreaker, blendedFields);
        }
    }

    static Query blendTerm(LuceneQueryBuilder.Context context,
                           BytesRef value,
                           Float commonTermsCutoff,
                           float tieBreaker,
                           RefAndBoost... blendedFields) {
        return blendTerms(context, new BytesRef[] {value}, commonTermsCutoff, tieBreaker, blendedFields);
    }

    static Query blendTerms(LuceneQueryBuilder.Context context,
                            BytesRef[] values,
                            Float commonTermsCutoff,
                            float tieBreaker,
                            RefAndBoost... blendedFields) {
        List<Query> queries = new ArrayList<>();
        Term[] terms = new Term[blendedFields.length * values.length];
        float[] blendedBoost = new float[blendedFields.length * values.length];
        int i = 0;
        for (RefAndBoost ft : blendedFields) {
            for (BytesRef term : values) {
                Query query;
                try {
                    query = new TermQuery(new Term(ft.ref.storageIdent(), term));
                } catch (IllegalArgumentException e) {
                    // the query expects a certain class of values such as numbers
                    // of ip addresses and the value can't be parsed, so ignore this
                    // field
                    continue;
                } catch (ElasticsearchParseException parseException) {
                    // date fields throw an ElasticsearchParseException with the
                    // underlying IAE as the cause, ignore this field if that is
                    // the case
                    if (parseException.getCause() instanceof IllegalArgumentException) {
                        continue;
                    }
                    throw parseException;
                }
                float boost = ft.boost;
                while (query instanceof BoostQuery) {
                    BoostQuery bq = (BoostQuery) query;
                    query = bq.getQuery();
                    boost *= bq.getBoost();
                }
                if (query.getClass() == TermQuery.class) {
                    terms[i] = ((TermQuery) query).getTerm();
                    blendedBoost[i] = boost;
                    i++;
                } else {
                    if (boost != 1f && query instanceof MatchNoDocsQuery == false) {
                        query = new BoostQuery(query, boost);
                    }
                    queries.add(query);
                }
            }
        }
        if (i > 0) {
            terms = Arrays.copyOf(terms, i);
            blendedBoost = Arrays.copyOf(blendedBoost, i);
            if (commonTermsCutoff != null) {
                queries.add(BlendedTermQuery.commonTermsBlendedQuery(terms, blendedBoost, commonTermsCutoff));
            } else {
                queries.add(BlendedTermQuery.dismaxBlendedQuery(terms, blendedBoost, tieBreaker));
            }
        }
        if (queries.size() == 1) {
            return queries.get(0);
        } else {
            // best effort: add clauses that are not term queries so that they have an opportunity to match
            // however their score contribution will be different
            // TODO: can we improve this?
            return new DisjunctionMaxQuery(queries, 1.0f);
        }
    }

    /**
     * Expand a {@link PhraseQuery} to multiple fields that share the same analyzer.
     * Returns a {@link DisjunctionMaxQuery} with a disjunction for each expanded field.
     */
    static Query blendPhrase(PhraseQuery query, float tiebreaker, RefAndBoost... fields) {
        List<Query> disjunctions = new ArrayList<>();
        for (RefAndBoost field : fields) {
            int[] positions = query.getPositions();
            Term[] terms = query.getTerms();
            PhraseQuery.Builder builder = new PhraseQuery.Builder();
            for (int i = 0; i < terms.length; i++) {
                builder.add(new Term(field.ref.storageIdent(), terms[i].bytes()), positions[i]);
            }
            Query q = builder.build();
            if (field.boost != MultiMatchQuery.DEFAULT_BOOST) {
                q = new BoostQuery(q, field.boost);
            }
            disjunctions.add(q);
        }
        return new DisjunctionMaxQuery(disjunctions, tiebreaker);
    }

    @Override
    protected Query blendTermQuery(Term term, Reference ref) {
        assert queryBuilder != null : "Must have called parse";
        return queryBuilder.blendTerm(term, ref);
    }

    @Override
    protected Query blendTermsQuery(Term[] terms, Reference ref) {
        assert queryBuilder != null : "Must have called parse";
        return queryBuilder.blendTerms(terms, ref);
    }

    @Override
    protected Query blendPhraseQuery(PhraseQuery query, Reference ref) {
        assert queryBuilder != null : "Must have called parse";
        return queryBuilder.blendPhrase(query, ref);
    }

    static final record RefAndBoost(Reference ref, float boost) {
    }
}
