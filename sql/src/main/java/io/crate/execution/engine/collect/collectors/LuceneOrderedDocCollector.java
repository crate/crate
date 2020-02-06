/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.execution.engine.collect.collectors;

import com.google.common.collect.Iterables;
import io.crate.breaker.RamAccounting;
import io.crate.data.Input;
import io.crate.data.Row;
import io.crate.exceptions.Exceptions;
import io.crate.execution.engine.distribution.merge.KeyIterable;
import io.crate.expression.reference.doc.lucene.CollectorContext;
import io.crate.expression.reference.doc.lucene.LuceneCollectorExpression;
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

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;

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

    private int batchSize;
    private boolean batchSizeReduced = false;

    @Nullable
    private volatile CancelableCollector currentCollector;

    @Nullable
    private volatile Throwable cancelled;

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
        this.rowFunction = new ScoreDocRowFunction(
            searcher.getIndexReader(),
            inputs,
            expressions,
            scorer,
            () -> raiseIfCancelled(cancelled)
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
        raiseIfCancelled(cancelled);
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
    }

    @Override
    public void cancel(Throwable t) {
        cancelled = t;
        var collector = currentCollector;
        if (collector != null) {
            collector.cancel(t);
        }
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
        var cancelableCollector = new CancelableCollector(collector, cancelled);
        currentCollector = cancelableCollector;
        searcher.search(query, cancelableCollector);
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
        return new KeyIterable<>(shardId(), Iterables.transform(Arrays.asList(scoreDocs), rowFunction));
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

    private static void raiseIfCancelled(@Nullable Throwable cancelledException) {
        if (cancelledException != null) {
            Exceptions.rethrowUnchecked(cancelledException);
        }
    }

    private static class CancelableCollector implements Collector {

        private final Collector delegate;

        @Nullable
        private volatile CancelableLeafCollector currentLeafCollector;

        @Nullable
        private volatile Throwable cancelled;

        public CancelableCollector(Collector delegate, @Nullable Throwable cancelled) {
            this.delegate = delegate;
            this.cancelled = cancelled;
        }

        @Override
        public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
            raiseIfCancelled(cancelled);
            currentLeafCollector = new CancelableLeafCollector(delegate.getLeafCollector(context), cancelled);
            return currentLeafCollector;
        }

        @Override
        public ScoreMode scoreMode() {
            return delegate.scoreMode();
        }

        public void cancel(Throwable t) {
            if (cancelled != null) {
                return;
            }
            cancelled = t;
            var leafCollector = currentLeafCollector;
            if (leafCollector != null) {
                leafCollector.cancel(t);
            }
        }
    }

    private static class CancelableLeafCollector implements LeafCollector {

        private final LeafCollector delegate;

        @Nullable
        private volatile Throwable cancelled;

        public CancelableLeafCollector(LeafCollector delegate, @Nullable Throwable cancelled) {
            this.delegate = delegate;
            this.cancelled = cancelled;
        }

        @Override
        public void setScorer(Scorable scorer) throws IOException {
            raiseIfCancelled(cancelled);
            delegate.setScorer(scorer);
        }

        @Override
        public void collect(int doc) throws IOException {
            raiseIfCancelled(cancelled);
            delegate.collect(doc);
        }

        public void cancel(Throwable t) {
            if (cancelled != null) {
                return;
            }
            cancelled = t;
        }
    }
}
