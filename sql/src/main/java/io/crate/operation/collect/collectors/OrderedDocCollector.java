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

import com.google.common.collect.Iterables;
import io.crate.analyze.OrderBy;
import io.crate.analyze.symbol.Reference;
import io.crate.analyze.symbol.Symbol;
import io.crate.core.collections.Row;
import io.crate.lucene.QueryBuilderHelper;
import io.crate.operation.Input;
import io.crate.operation.merge.NumberedIterable;
import io.crate.operation.reference.doc.lucene.CollectorContext;
import io.crate.operation.reference.doc.lucene.LuceneCollectorExpression;
import io.crate.operation.reference.doc.lucene.LuceneMissingValue;
import org.apache.lucene.search.*;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.lucene.MinimumScoreCollector;
import org.elasticsearch.search.internal.ContextIndexSearcher;
import org.elasticsearch.search.internal.SearchContext;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;

public class OrderedDocCollector implements Callable<NumberedIterable<Row>>, AutoCloseable {

    private static final ESLogger LOGGER = Loggers.getLogger(OrderedDocCollector.class);

    private final SearchContext searchContext;
    private final boolean doDocsScores;
    private final int batchSize;
    private final OrderBy orderBy;
    private final CollectorContext collectorContext;
    private final Sort sort;
    private final Collection<LuceneCollectorExpression<?>> expressions;
    private final NumberedIterable<Row> empty;
    private final int shardId;
    private final ScoreDocRowFunction rowFunction;
    private final DummyScorer scorer;
    private final ContextIndexSearcher searcher;

    private final Object[] missingValues;


    @Nullable
    private volatile FieldDoc lastDoc = null;

    volatile boolean exhausted = false;


    public OrderedDocCollector(SearchContext searchContext,
                               boolean doDocsScores,
                               int batchSize,
                               CollectorContext collectorContext,
                               OrderBy orderBy,
                               Sort sort,
                               List<Input<?>> inputs,
                               Collection<LuceneCollectorExpression<?>> expressions) {
        this.searchContext = searchContext;
        this.shardId = searchContext.indexShard().shardId().id();
        this.doDocsScores = doDocsScores;
        this.batchSize = batchSize;
        this.orderBy = orderBy;
        searcher = searchContext.searcher();
        this.collectorContext = collectorContext;
        this.sort = sort;
        this.scorer = new DummyScorer();
        this.expressions = expressions;
        this.rowFunction = new ScoreDocRowFunction(
                searcher.getIndexReader(),
                inputs,
                expressions,
                scorer
        );
        empty = new NumberedIterable<>(shardId, Collections.<Row>emptyList());
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
     *
     * Basically, calling this function multiple times pages through the shard in batches.
     */
    @Override
    public NumberedIterable<Row> call() throws Exception {
        if (exhausted) {
            return empty;
        }
        if (lastDoc == null) {
            return initialSearch();
        }
        return searchMore();
    }

    @Override
    public void close() {
        searchContext.clearReleasables(SearchContext.Lifetime.PHASE);
        searchContext.close();
    }

    private NumberedIterable<Row> scoreDocToIterable(ScoreDoc[] scoreDocs) {
        exhausted = scoreDocs.length < batchSize;
        if (scoreDocs.length > 0) {
            lastDoc = (FieldDoc) scoreDocs[scoreDocs.length - 1];
        }
        return new NumberedIterable<>(shardId, Iterables.transform(Arrays.asList(scoreDocs), rowFunction));
    }

    private NumberedIterable<Row> searchMore() throws IOException {
        if (exhausted) {
            LOGGER.trace("searchMore but EXHAUSTED");
            return empty;
        }
        LOGGER.debug("searchMore from [{}]", lastDoc);
        TopDocs topDocs = searcher.searchAfter(lastDoc, query(lastDoc), batchSize, sort, doDocsScores, false);
        return scoreDocToIterable(topDocs.scoreDocs);
    }

    private NumberedIterable<Row> initialSearch() throws IOException {
        for (LuceneCollectorExpression<?> expression : expressions) {
            expression.startCollect(collectorContext);
            expression.setScorer(scorer);
        }
        TopFieldCollector topFieldCollector = TopFieldCollector.create(sort, batchSize, true, doDocsScores, doDocsScores);
        Collector collector = topFieldCollector;
        if (searchContext.minimumScore() != null) {
            collector = new MinimumScoreCollector(collector, searchContext.minimumScore());
        }
        assert searchContext.queryCollectors().isEmpty() : "queryCollectors not supported";
        assert searchContext.parsedPostFilter() == null : "parsedPostFilter not supported";

        searcher.search(searchContext.query(), collector);
        return scoreDocToIterable(topFieldCollector.topDocs().scoreDocs);
    }

    private Query query(FieldDoc lastDoc) {
        Query query = nextPageQuery(lastDoc, orderBy, missingValues);
        if (query == null) {
            return searchContext.query();
        }
        BooleanQuery.Builder searchAfterQuery = new BooleanQuery.Builder();
        searchAfterQuery.add(searchContext.query(), BooleanClause.Occur.MUST);
        searchAfterQuery.add(query, BooleanClause.Occur.MUST_NOT);
        return searchAfterQuery.build();
    }

    @Nullable
    public static Query nextPageQuery(FieldDoc lastCollected, OrderBy orderBy, Object[] missingValues) {
        BooleanQuery.Builder queryBuilder = new BooleanQuery.Builder();
        for (int i = 0; i < orderBy.orderBySymbols().size(); i++) {
            Symbol order = orderBy.orderBySymbols().get(i);
            Object value = lastCollected.fields[i];
            if (order instanceof Reference) {
                boolean nullsFirst = orderBy.nullsFirst()[i] == null ? false : orderBy.nullsFirst()[i];
                value = value.equals(missingValues[i]) ? null : value;
                if (nullsFirst && value == null) {
                   // no filter needed
                   continue;
                }
                QueryBuilderHelper helper = QueryBuilderHelper.forType(order.valueType());
                String columnName = ((Reference) order).info().ident().columnIdent().fqn();

                Query orderQuery;
                // nulls already gone, so they should be excluded
                if (nullsFirst && value != null) {
                    BooleanQuery.Builder booleanQuery = new BooleanQuery.Builder();
                    if (orderBy.reverseFlags()[i]) {
                        booleanQuery.add(helper.rangeQuery(columnName, null, value, false, true), BooleanClause.Occur.MUST_NOT);
                    } else {
                        booleanQuery.add(helper.rangeQuery(columnName, value, null, true, false), BooleanClause.Occur.MUST_NOT);
                    }
                    orderQuery = booleanQuery.build();
                } else {
                    if (orderBy.reverseFlags()[i]) {
                        orderQuery = helper.rangeQuery(columnName, value, null, false, false);
                    } else {
                        orderQuery = helper.rangeQuery(columnName, null, value, false, false);
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

    public int shardId() {
        return shardId;
    }
}
