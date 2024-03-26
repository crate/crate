/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.execution.engine.collect.collectors;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.lucene.MinimumScoreCollector;
import org.elasticsearch.index.shard.ShardId;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import io.crate.common.collections.Lists;
import io.crate.common.exceptions.Exceptions;
import io.crate.data.Input;
import io.crate.data.Row;
import io.crate.data.breaker.RamAccounting;
import io.crate.execution.engine.distribution.merge.KeyIterable;
import io.crate.expression.reference.doc.lucene.CollectorContext;
import io.crate.expression.reference.doc.lucene.LuceneCollectorExpression;

public class LuceneOrderedDocCollector extends OrderedDocCollector {

    private static final Logger LOGGER = LogManager.getLogger(LuceneOrderedDocCollector.class);
    private static final int OPTIMIZE_BATCH_SIZE_THRESHOLD = 1000;
    private static final long FIELD_DOC_SIZE = RamUsageEstimator.shallowSizeOfInstance(FieldDoc.class);

    private final Query query;
    private final Float minScore;
    private final boolean doDocsScores;
    private final RamAccounting ramAccounting;
    private final CollectorContext collectorContext;
    private final Function<FieldDoc, Query> searchAfterQueryOptimize;
    private final Sort sort;
    private final Collection<? extends LuceneCollectorExpression<?>> expressions;
    private final ScoreDocRowFunction rowFunction;
    private final DummyScorer scorer;
    private final IndexSearcher searcher;
    private final AtomicReference<Throwable> killed = new AtomicReference<>();
    private final List<? extends Input<?>> inputs;

    private int batchSize;
    private boolean batchSizeReduced = false;

    @Nullable
    private FieldDoc lastDoc = null;

    public LuceneOrderedDocCollector(ShardId shardId,
                                     IndexSearcher searcher,
                                     Query query,
                                     Float minScore,
                                     boolean doDocsScores,
                                     int batchSize,
                                     RamAccounting ramAccounting,
                                     CollectorContext collectorContext,
                                     Function<FieldDoc, Query> searchAfterQueryOptimize,
                                     Sort sort,
                                     List<? extends Input<?>> inputs,
                                     Collection<? extends LuceneCollectorExpression<?>> expressions) {
        super(shardId);
        this.searcher = searcher;
        this.query = query;
        this.minScore = minScore;
        this.doDocsScores = doDocsScores;
        this.ramAccounting = ramAccounting;
        // We don't want to pre-allocate for more records than what can possible be returned
        // (+1) to make sure `exhausted` is set to `true` if all records match on the first `collect` call.
        this.batchSize = Math.min(batchSize, searcher.getIndexReader().numDocs() + 1);
        this.collectorContext = collectorContext;
        this.searchAfterQueryOptimize = searchAfterQueryOptimize;
        this.sort = sort;
        this.scorer = new DummyScorer();
        this.expressions = expressions;
        this.inputs = inputs;
        this.rowFunction = new ScoreDocRowFunction(
            searcher.getIndexReader(),
            inputs,
            expressions,
            scorer,
            this::raiseIfKilled
        );
    }

    /**
     * On the first call this will do an initial search and provide {@link #batchSize} number of rows
     * (or less if there aren't more available)
     * </p>
     * On subsequent calls it will return more rows (max {@link #batchSize} or less.
     * These rows are always the rows that come after the last row of the previously returned rows
     * <p/>
     * Basically, calling this function multiple times pages through the shard in batches.
     */
    @Override
    public KeyIterable<ShardId, Row> collect() {
        try {
            if (lastDoc == null) {
                return initialSearch();
            }
            return searchMore();
        } catch (Exception e) {
            Exceptions.rethrowUnchecked(e);
            return null;
        }
    }

    @Override
    public void close() {
        for (var input : inputs) {
            input.close();
        }
    }

    @Override
    public void kill(@NotNull Throwable t) {
        killed.set(t);
    }

    private KeyIterable<ShardId, Row> initialSearch() throws IOException {
        if (batchSize > OPTIMIZE_BATCH_SIZE_THRESHOLD && !batchSizeReduced) {
            batchSizeReduced = true;
            // + 1 because TopFieldCollector doesn't work with size=0 and we need to set the `exhausted` flag properly.
            batchSize = Math.min(batchSize, searcher.count(query) + 1);
        }
        for (LuceneCollectorExpression<?> expression : expressions) {
            expression.startCollect(collectorContext);
            expression.setScorer(scorer);
        }
        ramAccounting.addBytes(batchSize * FIELD_DOC_SIZE);
        TopFieldCollector topFieldCollector = TopFieldCollector.create(
            sort,
            batchSize,
            0 // do not process any hits
        );
        return doSearch(topFieldCollector, minScore, query);
    }

    private KeyIterable<ShardId, Row> searchMore() throws IOException {
        if (exhausted()) {
            LOGGER.trace("searchMore but EXHAUSTED");
            return empty();
        }
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("searchMore from [{}]", lastDoc);
        }
        ramAccounting.addBytes(batchSize * FIELD_DOC_SIZE);
        TopFieldCollector topFieldCollector = TopFieldCollector.create(
            sort,
            batchSize,
            lastDoc,
            0 // do not process any hits
        );
        return doSearch(topFieldCollector, minScore, query(lastDoc));
    }

    private KeyIterable<ShardId, Row> doSearch(TopFieldCollector topFieldCollector,
                                               Float minScore,
                                               Query query) throws IOException {
        Collector collector = topFieldCollector;
        if (minScore != null) {
            collector = new MinimumScoreCollector(collector, minScore);
        }
        collector = new KillableCollector(collector, this::raiseIfKilled);
        searcher.search(query, collector);
        ScoreDoc[] scoreDocs = topFieldCollector.topDocs().scoreDocs;
        if (doDocsScores) {
            TopFieldCollector.populateScores(scoreDocs, searcher, query);
        }
        return scoreDocToIterable(scoreDocs);
    }

    private KeyIterable<ShardId, Row> scoreDocToIterable(ScoreDoc[] scoreDocs) {
        exhausted = scoreDocs.length < batchSize;
        if (scoreDocs.length > 0) {
            lastDoc = (FieldDoc) scoreDocs[scoreDocs.length - 1];
        }
        return new KeyIterable<>(shardId(), Lists.mapLazy(Arrays.asList(scoreDocs), rowFunction));
    }

    private Query query(FieldDoc lastDoc) {
        Query optimizedQuery = searchAfterQueryOptimize.apply(lastDoc);
        if (optimizedQuery == null) {
            return this.query;
        }
        BooleanQuery.Builder searchAfterQuery = new BooleanQuery.Builder();
        searchAfterQuery.add(this.query, BooleanClause.Occur.MUST);
        searchAfterQuery.add(optimizedQuery, BooleanClause.Occur.MUST_NOT);
        return searchAfterQuery.build();
    }

    private void raiseIfKilled() {
        var t = killed.get();
        if (t != null) {
            Exceptions.rethrowUnchecked(t);
        }
    }

    private static class KillableCollector implements Collector {

        private final Collector delegate;
        private final Runnable raiseIfKilled;

        public KillableCollector(Collector delegate, Runnable raiseIfKilled) {
            this.delegate = delegate;
            this.raiseIfKilled = raiseIfKilled;
        }

        @Override
        public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
            raiseIfKilled.run();
            return new KillableLeafCollector(delegate.getLeafCollector(context), raiseIfKilled);
        }

        @Override
        public ScoreMode scoreMode() {
            return delegate.scoreMode();
        }
    }

    private static class KillableLeafCollector implements LeafCollector {

        private final LeafCollector delegate;
        private final Runnable raiseIfKilled;

        public KillableLeafCollector(LeafCollector delegate, Runnable raiseIfKilled) {
            this.delegate = delegate;
            this.raiseIfKilled = raiseIfKilled;
        }

        @Override
        public void setScorer(Scorable scorer) throws IOException {
            raiseIfKilled.run();
            delegate.setScorer(scorer);
        }

        @Override
        public void collect(int doc) throws IOException {
            raiseIfKilled.run();
            delegate.collect(doc);
        }
    }
}
