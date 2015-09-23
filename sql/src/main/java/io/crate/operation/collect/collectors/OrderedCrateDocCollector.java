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

import io.crate.action.sql.query.CrateSearchContext;
import io.crate.action.sql.query.LuceneSortGenerator;
import io.crate.analyze.OrderBy;
import io.crate.core.collections.Row;
import io.crate.jobs.KeepAliveListener;
import io.crate.lucene.QueryBuilderHelper;
import io.crate.operation.Input;
import io.crate.operation.InputRow;
import io.crate.operation.collect.CollectInputSymbolVisitor;
import io.crate.operation.collect.CrateCollector;
import io.crate.operation.projectors.RowReceiver;
import io.crate.operation.reference.doc.lucene.CollectorContext;
import io.crate.operation.reference.doc.lucene.LuceneCollectorExpression;
import io.crate.operation.reference.doc.lucene.OrderByCollectorExpression;
import io.crate.planner.node.dql.CollectPhase;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.search.*;
import org.elasticsearch.search.internal.ContextIndexSearcher;
import org.elasticsearch.search.internal.SearchContext;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Executor;

public class OrderedCrateDocCollector implements CrateCollector {

    private static final int KEEP_ALIVE_AFTER_ROWS = 1_000_000;

    private final CrateSearchContext searchContext;
    private final RowReceiver rowReceiver;
    private final int batchSizeHint;
    private final Collection<? extends LuceneCollectorExpression<?>> expressions;
    private final CollectorContext collectorContext;
    private final Integer limit;
    private final Sort sort;
    private final List<OrderByCollectorExpression> orderByCollectorExpressions = new ArrayList<>();
    private final Row inputRow;
    private final Scorer scorer;
    private final DummyScorer dummyScorer;
    private final State state = new State();
    private final TopRowUpstream upstream;
    private final OrderBy orderBy;
    private final KeepAliveListener keepAliveListener;

    private int rowCount = 0;


    public OrderedCrateDocCollector(CrateSearchContext searchContext,
                                    KeepAliveListener keepAliveListener,
                                    Executor executor,
                                    CollectPhase collectPhase,
                                    RowReceiver rowReceiver,
                                    List<Input<?>> inputs,
                                    Collection<? extends LuceneCollectorExpression<?>> expressions,
                                    CollectInputSymbolVisitor<?> inputSymbolVisitor,
                                    int batchSizeHint) {
        this.keepAliveListener = keepAliveListener;
        this.upstream = new TopRowUpstream(executor, new Runnable() {
            @Override
            public void run() {
                innerCollect(state.scoreDocs, state.scoreDocPos, state.lastDoc);
            }
        });
        this.searchContext = searchContext;
        this.rowReceiver = rowReceiver;
        this.batchSizeHint = batchSizeHint;
        this.expressions = expressions;
        collectorContext = new CollectorContext(
                searchContext.mapperService(),
                searchContext.fieldData(),
                new CollectorFieldsVisitor(expressions.size()),
                ((int) searchContext.id())
        );
        orderBy = collectPhase.orderBy();
        limit = collectPhase.limit();
        inputRow = new InputRow(inputs);
        dummyScorer = new DummyScorer();
        if (searchContext.minimumScore() == null) {
            scorer = dummyScorer;
        } else {
            scorer = new ScoreCachingWrappingScorer(dummyScorer);
        }
        sort = LuceneSortGenerator.generateLuceneSort(collectorContext, collectPhase.orderBy(), inputSymbolVisitor);

        rowReceiver.setUpstream(upstream);
        addOrderByExpressions(expressions);
    }

    private void addOrderByExpressions(Collection<? extends LuceneCollectorExpression<?>> expressions) {
        for (LuceneCollectorExpression<?> expression : expressions) {
            if (expression instanceof OrderByCollectorExpression) {
                orderByCollectorExpressions.add((OrderByCollectorExpression) expression);
            }
        }
    }


    private int batchSize() {
        return limit == null ? batchSizeHint : Math.min(batchSizeHint, limit - rowCount);
    }

    @Override
    public void doCollect() {
        for (LuceneCollectorExpression<?> expression : expressions) {
            expression.startCollect(collectorContext);
            expression.setScorer(scorer);
        }
        searchContext.searcher().inStage(ContextIndexSearcher.Stage.MAIN_QUERY);

        TopFieldDocs topFieldDocs;
        try {
            topFieldDocs = searchContext.searcher().search(searchContext.query(), batchSize(), sort);
        } catch (IOException e) {
            rowReceiver.fail(e);
            return;
        }
        innerCollect(topFieldDocs.scoreDocs, 0, null);
    }

    private void innerCollect(ScoreDoc[] scoreDocs, int position, @Nullable FieldDoc lastDoc) {
        boolean paused = false;
        try {
            if (emitRows(scoreDocs, position, lastDoc) == Result.FINISHED) {
                rowReceiver.finish();
            } else {
                paused = true;
            }
        } catch (Throwable t) {
            rowReceiver.fail(t);
        } finally {
            if (!paused) {
                searchContext.searcher().finishStage(ContextIndexSearcher.Stage.MAIN_QUERY);
                searchContext.clearReleasables(SearchContext.Lifetime.PHASE);
            }
        }
    }


    private Result emitRows(ScoreDoc[] scoreDocs, int scoreDocPos, @Nullable FieldDoc lastDoc) throws Throwable {
        if (scoreDocs.length == 0) {
            return Result.FINISHED;
        }

        int i = scoreDocPos;
        IndexReader indexReader = searchContext.searcher().getIndexReader();
        while (limit == null || rowCount < limit) {
            upstream.throwIfKilled();
            if (i == scoreDocs.length) {
                int batchSize = batchSize();
                if (scoreDocs.length < batchSize) {
                    // no more docs
                    break;
                }
                // there should be more docs.. do internal paging
                scoreDocs = searchContext.searcher().searchAfter(lastDoc, query(lastDoc), batchSize, sort).scoreDocs;
                if (scoreDocs.length == 0) {
                    break;
                }
                i = 0;
            }
            lastDoc = ((FieldDoc) scoreDocs[i]);
            i++;
            rowCount++;
            if (rowCount % KEEP_ALIVE_AFTER_ROWS == 0) {
                keepAliveListener.keepAlive();
            }
            dummyScorer.score(lastDoc.score);
            processNextDocId(lastDoc, indexReader);
            boolean wantMore = rowReceiver.setNextRow(inputRow);
            if (!wantMore) {
                break;
            }

            if (upstream.shouldPause()) {
                state.lastDoc = lastDoc;
                state.scoreDocPos = i;
                state.scoreDocs = scoreDocs;
                upstream.pauseProcessed();
                return Result.PAUSED;
            }
        }
        return Result.FINISHED;
    }

    private void processNextDocId(FieldDoc lastDoc, IndexReader indexReader) {
        for (OrderByCollectorExpression orderByCollectorExpression : orderByCollectorExpressions) {
            orderByCollectorExpression.setNextFieldDoc(lastDoc);
        }
        List<AtomicReaderContext> leaves = indexReader.leaves();
        int readerIndex = ReaderUtil.subIndex(lastDoc.doc, leaves);
        AtomicReaderContext subReaderContext = leaves.get(readerIndex);
        int subDoc = lastDoc.doc - subReaderContext.docBase;
        for (LuceneCollectorExpression<?> expression : expressions) {
            expression.setNextReader(subReaderContext);
            expression.setNextDocId(subDoc);
        }
    }

    private Query query(FieldDoc lastDoc) {
        Query query = nextPageQuery(lastDoc);
        if (query == null) {
            return searchContext.query();
        }
        BooleanQuery searchAfterQuery = new BooleanQuery();
        searchAfterQuery.add(searchContext.query(), BooleanClause.Occur.MUST);
        searchAfterQuery.add(query, BooleanClause.Occur.MUST_NOT);
        return searchAfterQuery;
    }

    @Nullable
    private Query nextPageQuery(FieldDoc lastCollected) {
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

    @Override
    public void kill(@Nullable Throwable throwable) {
        upstream.kill(throwable);
    }

    private static class State {
        ScoreDoc[] scoreDocs;
        int scoreDocPos;
        FieldDoc lastDoc;
    }
}
