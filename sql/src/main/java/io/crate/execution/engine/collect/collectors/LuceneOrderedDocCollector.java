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
import io.crate.data.Input;
import io.crate.data.Row;
import io.crate.execution.engine.distribution.merge.KeyIterable;
import io.crate.operation.reference.doc.lucene.CollectorContext;
import io.crate.operation.reference.doc.lucene.LuceneCollectorExpression;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopFieldCollector;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.lucene.MinimumScoreCollector;
import org.elasticsearch.index.shard.ShardId;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;

public class LuceneOrderedDocCollector extends OrderedDocCollector {

    private static final Logger LOGGER = Loggers.getLogger(LuceneOrderedDocCollector.class);

    private final Query query;
    private final Float minScore;
    private final boolean doDocsScores;
    private final int batchSize;
    private final CollectorContext collectorContext;
    private final Function<FieldDoc, Query> searchAfterQueryOptimize;
    private final Sort sort;
    private final Collection<? extends LuceneCollectorExpression<?>> expressions;
    private final ScoreDocRowFunction rowFunction;
    private final DummyScorer scorer;
    private final IndexSearcher searcher;


    @Nullable
    private volatile FieldDoc lastDoc = null;

    public LuceneOrderedDocCollector(ShardId shardId,
                                     IndexSearcher searcher,
                                     Query query,
                                     Float minScore,
                                     boolean doDocsScores,
                                     int batchSize,
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
        this.batchSize = batchSize;
        this.collectorContext = collectorContext;
        this.searchAfterQueryOptimize = searchAfterQueryOptimize;
        this.sort = sort;
        this.scorer = new DummyScorer();
        this.expressions = expressions;
        this.rowFunction = new ScoreDocRowFunction(
            searcher.getIndexReader(),
            inputs,
            expressions,
            scorer
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
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
    }

    private KeyIterable<ShardId, Row> initialSearch() throws IOException {
        for (LuceneCollectorExpression<?> expression : expressions) {
            expression.startCollect(collectorContext);
            expression.setScorer(scorer);
        }
        TopFieldCollector topFieldCollector = TopFieldCollector.create(sort, batchSize, true, doDocsScores, doDocsScores);
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
        TopFieldCollector topFieldCollector = TopFieldCollector.create(
            sort, batchSize, lastDoc, true, doDocsScores, doDocsScores);
        return doSearch(topFieldCollector, minScore, query(lastDoc));
    }

    private KeyIterable<ShardId, Row> doSearch(TopFieldCollector topFieldCollector,
                                               Float minScore,
                                               Query query) throws IOException {
        Collector collector = topFieldCollector;
        if (minScore != null) {
            collector = new MinimumScoreCollector(collector, minScore);
        }
        searcher.search(query, collector);
        return scoreDocToIterable(topFieldCollector.topDocs().scoreDocs);
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
}
