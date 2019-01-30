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
package org.apache.lucene.queries;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanWeight;
import org.apache.lucene.search.spans.Spans;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * A {@link SpanQuery} that matches no documents.
 */
public class SpanMatchNoDocsQuery extends SpanQuery {
    private final String field;
    private final String reason;

    public SpanMatchNoDocsQuery(String field, String reason) {
        this.field = field;
        this.reason = reason;
    }

    @Override
    public String getField() {
        return field;
    }

    @Override
    public String toString(String field) {
        return "SpanMatchNoDocsQuery(\"" + reason + "\")";
    }

    @Override
    public boolean equals(Object o) {
        return sameClassAs(o);
    }

    @Override
    public int hashCode() {
        return classHash();
    }

    @Override
    public SpanWeight createWeight(IndexSearcher searcher, boolean needsScores, float boost) throws IOException {
        return new SpanWeight(this, searcher, Collections.emptyMap(), boost) {
            @Override
            public void extractTermContexts(Map<Term, TermContext> contexts) {}

            @Override
            public Spans getSpans(LeafReaderContext ctx, Postings requiredPostings) {
                return null;
            }

            @Override
            public void extractTerms(Set<Term> terms) {}

            @Override
            public boolean isCacheable(LeafReaderContext ctx) {
                return true;
            }
        };
    }
}
