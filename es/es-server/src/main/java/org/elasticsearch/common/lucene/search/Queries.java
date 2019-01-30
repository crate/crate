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

package org.elasticsearch.common.lucene.search;

import org.apache.lucene.index.Term;
import org.apache.lucene.queries.ExtendedCommonTermsQuery;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.index.mapper.TypeFieldMapper;

import java.util.List;
import java.util.regex.Pattern;

public class Queries {

    public static Query newMatchAllQuery() {
        return new MatchAllDocsQuery();
    }

    /** Return a query that matches no document. */
    public static Query newMatchNoDocsQuery(String reason) {
        return new MatchNoDocsQuery(reason);
    }


    public static Query newUnmappedFieldQuery(String field) {
        return Queries.newMatchNoDocsQuery("unmapped field [" + (field != null ? field : "null") + "]");
    }

    public static Query newLenientFieldQuery(String field, RuntimeException e) {
        String message = ElasticsearchException.getExceptionName(e) + ":[" + e.getMessage() + "]";
        return Queries.newMatchNoDocsQuery("failed [" + field + "] query, caused by " + message);
    }

    public static Query newNestedFilter() {
        return new PrefixQuery(new Term(TypeFieldMapper.NAME, new BytesRef("__")));
    }

    /**
     * Creates a new non-nested docs query
     * @param indexVersionCreated the index version created since newer indices can identify a parent field more efficiently
     */
    public static Query newNonNestedFilter(Version indexVersionCreated) {
        if (indexVersionCreated.onOrAfter(Version.V_6_1_0)) {
            return new DocValuesFieldExistsQuery(SeqNoFieldMapper.PRIMARY_TERM_NAME);
        } else {
            return new BooleanQuery.Builder()
                .add(new MatchAllDocsQuery(), Occur.FILTER)
                .add(newNestedFilter(), Occur.MUST_NOT)
                .build();
        }
    }

    public static BooleanQuery filtered(@Nullable Query query, @Nullable Query filter) {
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        if (query != null) {
            builder.add(new BooleanClause(query, Occur.MUST));
        }
        if (filter != null) {
            builder.add(new BooleanClause(filter, Occur.FILTER));
        }
        return builder.build();
    }

    /** Return a query that matches all documents but those that match the given query. */
    public static Query not(Query q) {
        return new BooleanQuery.Builder()
            .add(new MatchAllDocsQuery(), Occur.MUST)
            .add(q, Occur.MUST_NOT)
            .build();
    }

    private static boolean isNegativeQuery(Query q) {
        if (!(q instanceof BooleanQuery)) {
            return false;
        }
        List<BooleanClause> clauses = ((BooleanQuery) q).clauses();
        if (clauses.isEmpty()) {
            return false;
        }
        for (BooleanClause clause : clauses) {
            if (!clause.isProhibited()) return false;
        }
        return true;
    }

    public static Query fixNegativeQueryIfNeeded(Query q) {
        if (isNegativeQuery(q)) {
            BooleanQuery bq = (BooleanQuery) q;
            BooleanQuery.Builder builder = new BooleanQuery.Builder();
            for (BooleanClause clause : bq) {
                builder.add(clause);
            }
            builder.add(newMatchAllQuery(), BooleanClause.Occur.MUST);
            return builder.build();
        }
        return q;
    }

    public static Query applyMinimumShouldMatch(BooleanQuery query, @Nullable String minimumShouldMatch) {
        if (minimumShouldMatch == null) {
            return query;
        }
        int optionalClauses = 0;
        for (BooleanClause c : query.clauses()) {
            if (c.getOccur() == BooleanClause.Occur.SHOULD) {
                optionalClauses++;
            }
        }

        int msm = calculateMinShouldMatch(optionalClauses, minimumShouldMatch);
        if (0 < msm) {
            BooleanQuery.Builder builder = new BooleanQuery.Builder();
            for (BooleanClause clause : query) {
                builder.add(clause);
            }
            builder.setMinimumNumberShouldMatch(msm);
            return builder.build();
        } else {
            return query;
        }
    }

    /**
     * Potentially apply minimum should match value if we have a query that it can be applied to,
     * otherwise return the original query.
     */
    public static Query maybeApplyMinimumShouldMatch(Query query, @Nullable String minimumShouldMatch) {
        if (query instanceof BooleanQuery) {
            return applyMinimumShouldMatch((BooleanQuery) query, minimumShouldMatch);
        } else if (query instanceof ExtendedCommonTermsQuery) {
            ((ExtendedCommonTermsQuery)query).setLowFreqMinimumNumberShouldMatch(minimumShouldMatch);
        }
        return query;
    }

    private static Pattern spaceAroundLessThanPattern = Pattern.compile("(\\s+<\\s*)|(\\s*<\\s+)");
    private static Pattern spacePattern = Pattern.compile(" ");
    private static Pattern lessThanPattern = Pattern.compile("<");

    public static int calculateMinShouldMatch(int optionalClauseCount, String spec) {
        int result = optionalClauseCount;
        spec = spec.trim();

        if (-1 < spec.indexOf("<")) {
            /* we have conditional spec(s) */
            spec = spaceAroundLessThanPattern.matcher(spec).replaceAll("<");
            for (String s : spacePattern.split(spec)) {
                String[] parts = lessThanPattern.split(s, 0);
                int upperBound = Integer.parseInt(parts[0]);
                if (optionalClauseCount <= upperBound) {
                    return result;
                } else {
                    result = calculateMinShouldMatch
                            (optionalClauseCount, parts[1]);
                }
            }
            return result;
        }

        /* otherwise, simple expression */

        if (-1 < spec.indexOf('%')) {
            /* percentage - assume the % was the last char.  If not, let Integer.parseInt fail. */
            spec = spec.substring(0, spec.length() - 1);
            int percent = Integer.parseInt(spec);
            float calc = (result * percent) * (1 / 100f);
            result = calc < 0 ? result + (int) calc : (int) calc;
        } else {
            int calc = Integer.parseInt(spec);
            result = calc < 0 ? result + calc : calc;
        }

        return result < 0 ? 0 : result;
    }
}
