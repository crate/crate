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
import io.crate.jobs.KeepAliveListener;
import io.crate.lucene.QueryBuilderHelper;
import io.crate.operation.Input;
import io.crate.operation.PageConsumeListener;
import io.crate.operation.collect.CollectInputSymbolVisitor;
import io.crate.operation.collect.CrateCollector;
import io.crate.operation.reference.doc.lucene.CollectorContext;
import io.crate.operation.reference.doc.lucene.LuceneCollectorExpression;
import io.crate.planner.node.dql.CollectPhase;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;
import org.apache.lucene.search.*;
import org.elasticsearch.search.internal.ContextIndexSearcher;
import org.elasticsearch.search.internal.SearchContext;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Executor;

public class ScoreDocCollector implements CrateCollector {

    private static final int KEEP_ALIVE_AFTER_ROWS = 1_000_000;

    private final CrateSearchContext searchContext;
    private final int batchSizeHint;
    private final Collection<? extends LuceneCollectorExpression<?>> expressions;
    private final CollectorContext collectorContext;
    private final Integer limit;
    private final Sort sort;
    private final Scorer scorer;
    private final OrderBy orderBy;
    private final KeepAliveListener keepAliveListener;
    private final ScoreDocMerger scoreDocMerger;
    private final int upstreamIdx;

    private boolean exhausted = false;
    private FieldDoc lastDoc;



    public ScoreDocCollector(final CrateSearchContext searchContext,
                             KeepAliveListener keepAliveListener,
                             Executor executor,
                             CollectPhase collectPhase,
                             final ScoreDocMerger scoreDocMerger,
                             List<Input<?>> inputs,
                             Collection<? extends LuceneCollectorExpression<?>> expressions,
                             CollectInputSymbolVisitor<?> inputSymbolVisitor,
                             int batchSizeHint) {
        this.keepAliveListener = keepAliveListener;
        this.scoreDocMerger = scoreDocMerger;
        this.searchContext = searchContext;
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
        DummyScorer dummyScorer = new DummyScorer();
        if (searchContext.minimumScore() == null) {
            scorer = dummyScorer;
        } else {
            scorer = new ScoreCachingWrappingScorer(dummyScorer);
        }
        sort = LuceneSortGenerator.generateLuceneSort(collectorContext, collectPhase.orderBy(), inputSymbolVisitor);

        upstreamIdx = scoreDocMerger.registerUpstream(
                inputs, expressions, searchContext.searcher().getIndexReader(), dummyScorer, new PageConsumeListener() {

            @Override
            public void needMore() {
                if (exhausted) {
                    ScoreDocCollector.this.finish();
                } else {
                    try {
                        int batchSize = batchSize();
                        ScoreDoc[] scoreDocs = searchContext.searcher().searchAfter(lastDoc, query(lastDoc), batchSize, sort).scoreDocs;
                        exhausted = scoreDocs.length < batchSize;
                        if (!exhausted) {
                            lastDoc = (FieldDoc) scoreDocs[scoreDocs.length - 1];
                        }
                        scoreDocMerger.setNextScoreDocs(upstreamIdx, scoreDocs);
                    } catch (IOException e) {
                        fail(e);
                    }
                }
            }

            @Override
            public void finish() {
                ScoreDocCollector.this.finish();
            }
        });
    }

    private int batchSize() {
        return limit == null ? batchSizeHint : Math.min(batchSizeHint, limit);
    }

    @Override
    public void doCollect() {
        for (LuceneCollectorExpression<?> expression : expressions) {
            expression.startCollect(collectorContext);
            expression.setScorer(scorer);
        }
        searchContext.searcher().inStage(ContextIndexSearcher.Stage.MAIN_QUERY);

        TopFieldDocs topFieldDocs;
        int batchSize = batchSize();
        try {
            topFieldDocs = searchContext.searcher().search(searchContext.query(), batchSize, sort);
        } catch (IOException e) {
            exhausted = true;
            scoreDocMerger.setNextScoreDocs(upstreamIdx, new ScoreDoc[0]);
            fail(e);
            return;
        }

        ScoreDoc[] scoreDocs = topFieldDocs.scoreDocs;
        if (scoreDocs.length == 0) {
            exhausted = true;
            scoreDocMerger.setNextScoreDocs(upstreamIdx, new ScoreDoc[0]);
            return;
        }

        lastDoc = (FieldDoc) scoreDocs[scoreDocs.length - 1];
        exhausted = scoreDocs.length < batchSize;

        scoreDocMerger.setNextScoreDocs(upstreamIdx, scoreDocs);
    }

    private void finish() {
        exhausted = true;
        searchContext.searcher().finishStage(ContextIndexSearcher.Stage.MAIN_QUERY);
        searchContext.clearReleasables(SearchContext.Lifetime.PHASE);
        scoreDocMerger.finish(upstreamIdx);
    }

    private void fail(IOException e) {
        exhausted = true;
        searchContext.searcher().finishStage(ContextIndexSearcher.Stage.MAIN_QUERY);
        searchContext.clearReleasables(SearchContext.Lifetime.PHASE);
        scoreDocMerger.fail(upstreamIdx, e);
    }

    @Override
    public void kill(@Nullable Throwable throwable) {

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
}
