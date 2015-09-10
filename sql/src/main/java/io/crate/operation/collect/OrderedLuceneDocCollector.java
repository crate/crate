/*
 * Licensed to CRATE.IO GmbH ("Crate") under one or more contributor
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

package io.crate.operation.collect;

import io.crate.action.sql.query.CrateSearchContext;
import io.crate.action.sql.query.LuceneSortGenerator;
import io.crate.analyze.OrderBy;
import io.crate.breaker.RamAccountingContext;
import io.crate.jobs.KeepAliveListener;
import io.crate.lucene.QueryBuilderHelper;
import io.crate.operation.Input;
import io.crate.operation.projectors.RowReceiver;
import io.crate.operation.reference.doc.lucene.LuceneCollectorExpression;
import io.crate.operation.reference.doc.lucene.OrderByCollectorExpression;
import io.crate.planner.node.dql.CollectPhase;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.search.*;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;

public class OrderedLuceneDocCollector extends LuceneDocCollector {

    private static final ESLogger LOGGER = Loggers.getLogger(OrderedLuceneDocCollector.class);

    private final OrderBy orderBy;
    private final List<OrderByCollectorExpression> orderByCollectorExpressions = new ArrayList<>();
    private final CollectInputSymbolVisitor<?> inputSymbolVisitor;
    private final int batchSizeHint;

    private InternalCollectContext internalCollectContext;

    public OrderedLuceneDocCollector(ThreadPool threadPool,
                                     CrateSearchContext searchContext,
                                     KeepAliveListener keepAliveListener,
                                     List<Input<?>> inputs,
                                     List<LuceneCollectorExpression<?>> collectorExpressions,
                                     CollectInputSymbolVisitor<?> inputSymbolVisitor,
                                     CollectPhase collectNode,
                                     RowReceiver downStreamProjector,
                                     RamAccountingContext ramAccountingContext,
                                     int batchSizeHint) throws Exception {
        super(threadPool, searchContext, keepAliveListener, inputs, collectorExpressions, collectNode,
                downStreamProjector, ramAccountingContext);
        orderBy = collectNode.orderBy();
        this.batchSizeHint = batchSizeHint;
        this.inputSymbolVisitor = inputSymbolVisitor;
        for (LuceneCollectorExpression expr : collectorExpressions) {
            if ( expr instanceof OrderByCollectorExpression) {
                orderByCollectorExpressions.add((OrderByCollectorExpression)expr);
            }
        }

    }

    @Override
    protected boolean searchAndCollect() throws IOException {
        LOGGER.trace("Collecting data: batchSize: {}", batchSizeHint);
        while (limit == null || rowCount < limit) {
            if (killed) {
                throw new CancellationException();
            }

            ScoreDoc scoreDoc = null;
            if (internalCollectContext.topFieldDocs == null) {
                if (shouldPause()) {
                    return false;
                }
                if (internalCollectContext.lastCollected == null) {
                    internalCollectContext.topFieldDocs = searchContext.searcher().search(internalCollectContext.buildQuery(), batchSize(), internalCollectContext.sort);
                } else {
                    internalCollectContext.topFieldDocs = (TopFieldDocs) searchContext.searcher().searchAfter(internalCollectContext.lastCollected,
                            internalCollectContext.buildQuery(), batchSize(), internalCollectContext.sort);
                }
                if (internalCollectContext.topFieldDocs.scoreDocs.length == 0) {
                    // no (more) hits
                    break;
                }
                internalCollectContext.topFieldPosition = 0;

                IndexReaderContext indexReaderContext = searchContext.searcher().getTopReaderContext();
                if (!indexReaderContext.leaves().isEmpty()) {
                    setScorer(internalCollectContext.scorer);
                    scoreDoc = emitTopFieldDocs();
                } else {
                    break;
                }
            } else {
                scoreDoc = emitTopFieldDocs();

            }
            if (paused.get()) {
                return false;
            }
            if (internalCollectContext.topFieldDocs.scoreDocs.length < batchSize()) {
                // last batch, no more hits
                break;
            }
            internalCollectContext.lastCollected = scoreDoc;
            internalCollectContext.topFieldDocs = null;
        }
        return true;
    }

    private ScoreDoc emitTopFieldDocs() throws IOException {
        ScoreDoc scoreDoc = null;
        for (; internalCollectContext.topFieldPosition < internalCollectContext.topFieldDocs.scoreDocs.length; internalCollectContext.topFieldPosition++) {
            scoreDoc = internalCollectContext.topFieldDocs.scoreDocs[internalCollectContext.topFieldPosition];
            int readerIndex = ReaderUtil.subIndex(scoreDoc.doc, searchContext.searcher().getIndexReader().leaves());
            AtomicReaderContext subReaderContext = searchContext.searcher().getIndexReader().leaves().get(readerIndex);
            int subDoc = scoreDoc.doc - subReaderContext.docBase;
            setNextReader(subReaderContext);
            setNextOrderByValues(scoreDoc);
            internalCollectContext.scorer.score(scoreDoc.score);
            if (shouldPause()) {
                break;
            }
            doCollect(subDoc);
        }
        return scoreDoc;

    }

    @Override
    protected void buildContext(Query query) {
        if (internalCollectContext == null) {
            Sort sort = LuceneSortGenerator.generateLuceneSort(searchContext, orderBy, inputSymbolVisitor);
            internalCollectContext = new InternalCollectContext(query, orderBy, sort);
        }
    }

    @Override
    protected boolean skipDoc(int doc) {
        return false;
    }

    @Override
    protected void postCollectDoc(int doc) {
        // do nothing, all done in searchAndCollect()
    }

    @Override
    protected void skipSegmentReader() {
        // do nothing, all done in searchAndCollect()
    }

    public void setNextOrderByValues(ScoreDoc scoreDoc) {
        for (OrderByCollectorExpression expr : orderByCollectorExpressions) {
            expr.setNextFieldDoc((FieldDoc) scoreDoc);
        }
    }

    private int batchSize() {
        return limit == null ? batchSizeHint : Math.min(batchSizeHint, limit - rowCount);
    }

    protected static class InternalCollectContext extends LuceneDocCollector.InternalCollectContext {

        private final DummyScorer scorer = new DummyScorer();

        @Nullable
        private final Sort sort;
        @Nullable private final OrderBy orderBy;

        private TopFieldDocs topFieldDocs;
        private int topFieldPosition = 0;

        public InternalCollectContext(Query query, @Nullable OrderBy orderBy, @Nullable Sort sort) {
            super(query);
            this.sort = sort;
            this.orderBy = orderBy;
        }

        public Query buildQuery() {
            if (lastCollected != null) {
                Query nextPageQuery = nextPageQuery((FieldDoc) lastCollected);
                if (nextPageQuery != null) {
                    BooleanQuery searchAfterQuery = new BooleanQuery();
                    searchAfterQuery.add(query, BooleanClause.Occur.MUST);
                    searchAfterQuery.add(nextPageQuery, BooleanClause.Occur.MUST_NOT);
                    return searchAfterQuery;
                }
            }
            return query;
        }

        @Nullable
        private Query nextPageQuery(FieldDoc lastCollected) {
            if (orderBy == null) {
                return null;
            }
            BooleanQuery query = new BooleanQuery();
            for (int i = 0; i < orderBy.orderBySymbols().size(); i++) {
                Symbol order = orderBy.orderBySymbols().get(i);
                Object value = lastCollected.fields[i];
                // only filter for null values if nulls last
                if (order instanceof Reference && (value != null || !orderBy.nullsFirst()[i])) {
                    QueryBuilderHelper helper = QueryBuilderHelper.forType(order.valueType());
                    String columnName = ((Reference) order).info().ident().columnIdent().fqn();
                    if (orderBy.reverseFlags()[i]) {
                        query.add(helper.rangeQuery(columnName, value, null, false, false), BooleanClause.Occur.MUST);
                    } else {
                        query.add(helper.rangeQuery(columnName, null, value, false, false), BooleanClause.Occur.MUST);
                    }
                }
            }
            if (query.clauses().size() > 0) {
                return query;
            } else {
                return null;
            }
        }
    }

    /**
     * Dummy {@link org.apache.lucene.search.Scorer} implementation just for passing the
     * <code>score</code> float value of a {@link org.apache.lucene.search.ScoreDoc} to a
     * {@link io.crate.operation.reference.doc.lucene.ScoreCollectorExpression}.
     */
    private static class DummyScorer extends Scorer {

        private float score;

        public DummyScorer() {
            super(null);
        }

        public void score(float score) {
            this.score = score;
        }

        @Override
        public float score() throws IOException {
            return score;
        }

        @Override
        public int advance(int target) throws IOException {
            return 0;
        }

        @Override
        public int freq() throws IOException {
            return 0;
        }

        @Override
        public int docID() {
            return 0;
        }

        @Override
        public int nextDoc() throws IOException {
            return 0;
        }

        @Override
        public long cost() {
            return 0;
        }
    }

}
