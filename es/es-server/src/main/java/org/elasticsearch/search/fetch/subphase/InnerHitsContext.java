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

package org.elasticsearch.search.fetch.subphase;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.ConjunctionDISI;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.SubSearchContext;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Context used for inner hits retrieval
 */
public final class InnerHitsContext {
    private final Map<String, InnerHitSubContext> innerHits;

    public InnerHitsContext() {
        this.innerHits = new HashMap<>();
    }

    InnerHitsContext(Map<String, InnerHitSubContext> innerHits) {
        this.innerHits = Objects.requireNonNull(innerHits);
    }

    public Map<String, InnerHitSubContext> getInnerHits() {
        return innerHits;
    }

    public void addInnerHitDefinition(InnerHitSubContext innerHit) {
        if (innerHits.containsKey(innerHit.getName())) {
            throw new IllegalArgumentException("inner_hit definition with the name [" + innerHit.getName() +
                    "] already exists. Use a different inner_hit name or define one explicitly");
        }

        innerHits.put(innerHit.getName(), innerHit);
    }

    /**
     * A {@link SubSearchContext} that associates {@link TopDocs} to each {@link SearchHit}
     * in the parent search context
     */
    public abstract static class InnerHitSubContext extends SubSearchContext {

        private final String name;
        protected final SearchContext context;
        private InnerHitsContext childInnerHits;

        // TODO: when types are complete removed just use String instead for the id:
        private Uid uid;

        protected InnerHitSubContext(String name, SearchContext context) {
            super(context);
            this.name = name;
            this.context = context;
        }

        public abstract TopDocs[] topDocs(SearchHit[] hits) throws IOException;

        public String getName() {
            return name;
        }

        @Override
        public InnerHitsContext innerHits() {
            return childInnerHits;
        }

        public void setChildInnerHits(Map<String, InnerHitSubContext> childInnerHits) {
            this.childInnerHits = new InnerHitsContext(childInnerHits);
        }

        protected Weight createInnerHitQueryWeight() throws IOException {
            final boolean needsScores = size() != 0 && (sort() == null || sort().sort.needsScores());
            return context.searcher().createNormalizedWeight(query(), needsScores);
        }

        public SearchContext parentSearchContext() {
            return context;
        }

        public Uid getUid() {
            return uid;
        }

        public void setUid(Uid uid) {
            this.uid = uid;
        }
    }

    public static void intersect(Weight weight, Weight innerHitQueryWeight, Collector collector, LeafReaderContext ctx) throws IOException {
        ScorerSupplier scorerSupplier = weight.scorerSupplier(ctx);
        if (scorerSupplier == null) {
            return;
        }
        // use low leadCost since this scorer will be consumed on a minority of documents
        Scorer scorer = scorerSupplier.get(0);

        ScorerSupplier innerHitQueryScorerSupplier = innerHitQueryWeight.scorerSupplier(ctx);
        if (innerHitQueryScorerSupplier == null) {
            return;
        }
        // use low loadCost since this scorer will be consumed on a minority of documents
        Scorer innerHitQueryScorer = innerHitQueryScorerSupplier.get(0);

        final LeafCollector leafCollector;
        try {
            leafCollector = collector.getLeafCollector(ctx);
            // Just setting the innerHitQueryScorer is ok, because that is the actual scoring part of the query
            leafCollector.setScorer(innerHitQueryScorer);
        } catch (CollectionTerminatedException e) {
            return;
        }

        try {
            Bits acceptDocs = ctx.reader().getLiveDocs();
            DocIdSetIterator iterator = ConjunctionDISI.intersectIterators(Arrays.asList(innerHitQueryScorer.iterator(),
                scorer.iterator()));
            for (int docId = iterator.nextDoc(); docId < DocIdSetIterator.NO_MORE_DOCS; docId = iterator.nextDoc()) {
                if (acceptDocs == null || acceptDocs.get(docId)) {
                    leafCollector.collect(docId);
                }
            }
        } catch (CollectionTerminatedException e) {
            // ignore and continue
        }
    }
}
