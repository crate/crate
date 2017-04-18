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

package io.crate.operation.collect.collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;
import io.crate.analyze.OrderBy;
import io.crate.analyze.symbol.Symbol;
import io.crate.data.Input;
import io.crate.data.Row;
import io.crate.lucene.FieldTypeLookup;
import io.crate.metadata.Reference;
import io.crate.operation.merge.KeyIterable;
import io.crate.operation.reference.doc.lucene.CollectorContext;
import io.crate.operation.reference.doc.lucene.LuceneCollectorExpression;
import io.crate.operation.reference.doc.lucene.LuceneMissingValue;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.*;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.lucene.MinimumScoreCollector;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.shard.ShardId;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class LuceneOrderedDocCollector extends OrderedDocCollector {

    private static final Logger LOGGER = Loggers.getLogger(LuceneOrderedDocCollector.class);

    private final Query query;
    private final Float minScore;
    private final boolean doDocsScores;
    private final int batchSize;
    private final FieldTypeLookup fieldTypeLookup;
    private final CollectorContext collectorContext;
    private final OrderBy orderBy;
    private final Sort sort;
    private final Collection<? extends LuceneCollectorExpression<?>> expressions;
    private final ScoreDocRowFunction rowFunction;
    private final DummyScorer scorer;
    private final IndexSearcher searcher;

    private final Object[] missingValues;
    private final QueryShardContext queryShardContext;

    @Nullable
    private volatile FieldDoc lastDoc = null;

    public LuceneOrderedDocCollector(ShardId shardId,
                                     IndexSearcher searcher,
                                     Query query,
                                     QueryShardContext queryShardContext,
                                     Float minScore,
                                     boolean doDocsScores,
                                     int batchSize,
                                     FieldTypeLookup fieldTypeLookup,
                                     CollectorContext collectorContext,
                                     OrderBy orderBy,
                                     Sort sort,
                                     List<? extends Input<?>> inputs,
                                     Collection<? extends LuceneCollectorExpression<?>> expressions) {
        super(shardId);
        this.searcher = searcher;
        this.query = query;
        this.queryShardContext = queryShardContext;
        this.minScore = minScore;
        this.doDocsScores = doDocsScores;
        this.batchSize = batchSize;
        this.fieldTypeLookup = fieldTypeLookup;
        this.collectorContext = collectorContext;
        this.orderBy = orderBy;
        this.sort = sort;
        this.scorer = new DummyScorer();
        this.expressions = expressions;
        this.rowFunction = new ScoreDocRowFunction(
            searcher.getIndexReader(),
            inputs,
            expressions,
            scorer
        );
        missingValues = new Object[orderBy.orderBySymbols().size()];
        for (int i = 0; i < orderBy.orderBySymbols().size(); i++) {
            missingValues[i] = LuceneMissingValue.missingValue(orderBy, i);
        }
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
        Collector collector = topFieldCollector;
        if (minScore != null) {
            collector = new MinimumScoreCollector(collector, minScore);
        }
        searcher.search(query, collector);
        return scoreDocToIterable(topFieldCollector.topDocs().scoreDocs);
    }

    private KeyIterable<ShardId, Row> searchMore() throws IOException {
        if (exhausted()) {
            LOGGER.trace("searchMore but EXHAUSTED");
            return empty();
        }
        LOGGER.debug("searchMore from [{}]", lastDoc);
        TopDocs topDocs = searcher.searchAfter(lastDoc, query(lastDoc), batchSize, sort, doDocsScores, false);
        return scoreDocToIterable(topDocs.scoreDocs);
    }

    private KeyIterable<ShardId, Row> scoreDocToIterable(ScoreDoc[] scoreDocs) {
        exhausted = scoreDocs.length < batchSize;
        if (scoreDocs.length > 0) {
            lastDoc = (FieldDoc) scoreDocs[scoreDocs.length - 1];
        }
        return new KeyIterable<>(shardId(), Iterables.transform(Arrays.asList(scoreDocs), rowFunction));
    }

    private Query query(FieldDoc lastDoc) {
        Query query = nextPageQuery(lastDoc, orderBy, missingValues, fieldTypeLookup, queryShardContext);
        if (query == null) {
            return this.query;
        }
        BooleanQuery.Builder searchAfterQuery = new BooleanQuery.Builder();
        searchAfterQuery.add(this.query, BooleanClause.Occur.MUST);
        searchAfterQuery.add(query, BooleanClause.Occur.MUST_NOT);
        return searchAfterQuery.build();
    }

    @Nullable
    @VisibleForTesting
    static Query nextPageQuery(FieldDoc lastCollected, OrderBy orderBy, Object[] missingValues,
                               FieldTypeLookup fieldTypeLookup, QueryShardContext queryShardContext) {
        BooleanQuery.Builder queryBuilder = new BooleanQuery.Builder();
        for (int i = 0; i < orderBy.orderBySymbols().size(); i++) {
            Symbol order = orderBy.orderBySymbols().get(i);
            Object value = lastCollected.fields[i];
            if (order instanceof Reference) {
                boolean nullsFirst = orderBy.nullsFirst()[i] == null ? false : orderBy.nullsFirst()[i];
                value = value == null || value.equals(missingValues[i]) ? null : value;
                if (nullsFirst && value == null) {
                    // no filter needed
                    continue;
                }
                String columnName = ((Reference) order).ident().columnIdent().fqn();
                MappedFieldType fieldType = requireNonNull(
                    fieldTypeLookup.get(columnName), "Column must exist: " + columnName);

                Query orderQuery;
                // nulls already gone, so they should be excluded
                if (nullsFirst) {
                    BooleanQuery.Builder booleanQuery = new BooleanQuery.Builder();
                    booleanQuery.add(new MatchAllDocsQuery(), BooleanClause.Occur.MUST);
                    if (orderBy.reverseFlags()[i]) {
                        booleanQuery.add(fieldType.rangeQuery(null, value, false, true, queryShardContext), BooleanClause.Occur.MUST_NOT);
                    } else {
                        booleanQuery.add(fieldType.rangeQuery(value, null, true, false, queryShardContext), BooleanClause.Occur.MUST_NOT);
                    }
                    orderQuery = booleanQuery.build();
                } else {
                    if (orderBy.reverseFlags()[i]) {
                        orderQuery = fieldType.rangeQuery(value, null, false, false, queryShardContext);
                    } else {
                        orderQuery = fieldType.rangeQuery(null, value, false, false, queryShardContext);
                    }
                }
                queryBuilder.add(orderQuery, BooleanClause.Occur.MUST);
            }
        }
        BooleanQuery query = queryBuilder.build();
        if (query.clauses().size() > 0) {
            return query;
        } else {
            return null;
        }
    }
}
