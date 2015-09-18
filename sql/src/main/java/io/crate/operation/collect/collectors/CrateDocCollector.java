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
import io.crate.breaker.CrateCircuitBreakerService;
import io.crate.breaker.RamAccountingContext;
import io.crate.core.collections.Row;
import io.crate.jobs.KeepAliveListener;
import io.crate.operation.Input;
import io.crate.operation.InputRow;
import io.crate.operation.collect.CollectionFinishedEarlyException;
import io.crate.operation.collect.CollectionPauseException;
import io.crate.operation.collect.CrateCollector;
import io.crate.operation.collect.UnexpectedCollectionTerminatedException;
import io.crate.operation.projectors.RowReceiver;
import io.crate.operation.reference.doc.lucene.CollectorContext;
import io.crate.operation.reference.doc.lucene.LuceneCollectorExpression;
import io.crate.planner.node.dql.CollectPhase;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.elasticsearch.common.lucene.MinimumScoreCollector;
import org.elasticsearch.search.internal.ContextIndexSearcher;
import org.elasticsearch.search.internal.SearchContext;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Executor;

public class CrateDocCollector implements CrateCollector {

    private final CollectorContext collectorContext;
    private final CrateSearchContext searchContext;
    private final RowReceiver rowReceiver;
    private final Collection<? extends LuceneCollectorExpression<?>> expressions;
    private final Collector luceneCollector;
    private final TopRowUpstream upstreamState;
    private final State state = new State();

    public CrateDocCollector(CrateSearchContext searchContext,
                             Executor executor,
                             KeepAliveListener keepAliveListener,
                             CollectPhase collectPhase,
                             RamAccountingContext ramAccountingContext,
                             RowReceiver rowReceiver,
                             List<Input<?>> inputs,
                             Collection<? extends LuceneCollectorExpression<?>> expressions) {
        this.searchContext = searchContext;
        this.rowReceiver = rowReceiver;
        upstreamState = new TopRowUpstream(executor, new Runnable() {
            @Override
            public void run() {
                innerCollect(state.collector, state.weight, state.leaveIt, state.scorer);
            }
        });
        this.expressions = expressions;
        CollectorFieldsVisitor fieldsVisitor = new CollectorFieldsVisitor(expressions.size());
        collectorContext = new CollectorContext(
                searchContext.mapperService(),
                searchContext.fieldData(),
                fieldsVisitor,
                ((int) searchContext.id())
        );
        rowReceiver.setUpstream(upstreamState);
        Collector collector = new LuceneDocCollector(
                keepAliveListener,
                ramAccountingContext,
                upstreamState,
                collectPhase.limit(),
                rowReceiver,
                new InputRow(inputs),
                expressions
        );
        if (searchContext.minimumScore() != null) {
            collector = new MinimumScoreCollector(collector, searchContext.minimumScore());
        }
        luceneCollector = collector;
    }

    @Override
    public void doCollect() {
        for (LuceneCollectorExpression<?> expression : expressions) {
            expression.startCollect(collectorContext);
        }
        Collector collector = luceneCollector;
        if (collectorContext.visitor().required()) {
            collector = new FieldVisitorCollector(collector, collectorContext.visitor());
        }
        ContextIndexSearcher contextIndexSearcher = searchContext.searcher();
        contextIndexSearcher.inStage(ContextIndexSearcher.Stage.MAIN_QUERY);

        Weight weight;
        Iterator<AtomicReaderContext> leavesIt;
        try {
            weight = searchContext.engineSearcher().searcher().createNormalizedWeight(searchContext.query());
            leavesIt = contextIndexSearcher.getTopReaderContext().leaves().iterator();
        } catch (IOException e) {
            rowReceiver.fail(e);
            return;
        }
        innerCollect(collector, weight, leavesIt, null);
    }

    private void innerCollect(Collector collector, Weight weight, Iterator<AtomicReaderContext> leavesIt, @Nullable BulkScorer scorer) {
        boolean paused = false;
        try {
            if (collectLeaves(collector, weight, leavesIt, scorer) == Result.FINISHED) {
                rowReceiver.finish();
            } else {
                paused = true;
            }
        } catch (CollectionFinishedEarlyException e) {
            rowReceiver.finish();
        } catch (Throwable t) {
            rowReceiver.fail(t);
        } finally {
            if (!paused) {
                searchContext.searcher().finishStage(ContextIndexSearcher.Stage.MAIN_QUERY);
                searchContext.clearReleasables(SearchContext.Lifetime.PHASE);
            }
        }
    }

    private Result collectLeaves(Collector collector,
                                 Weight weight,
                                 Iterator<AtomicReaderContext> leaves,
                                 @Nullable BulkScorer scorer) throws IOException {
        if (scorer != null) {
            if (processScorer(collector, weight, leaves, scorer)) return Result.PAUSED;
        }
        try {
            while (leaves.hasNext()) {
                AtomicReaderContext leaf = leaves.next();
                collector.setNextReader(leaf);
                scorer = weight.bulkScorer(leaf, !collector.acceptsDocsOutOfOrder(), leaf.reader().getLiveDocs());
                if (scorer == null) {
                    continue;
                }
                if (processScorer(collector, weight, leaves, scorer)) return Result.PAUSED;
            }
        } finally {
            searchContext.clearReleasables(SearchContext.Lifetime.COLLECTION);
        }
        return Result.FINISHED;
    }

    private boolean processScorer(Collector collector, Weight weight, Iterator<AtomicReaderContext> leaves, BulkScorer scorer) throws IOException {
        try {
            scorer.score(collector);
        } catch (CollectionPauseException e) {
            state.collector = collector;
            state.leaveIt = leaves;
            state.scorer = scorer;
            state.weight = weight;
            upstreamState.pauseProcessed();
            return true;
        }
        return false;
    }

    @Override
    public void kill(@Nullable Throwable throwable) {
        upstreamState.kill(throwable);
    }

    static class State {
        BulkScorer scorer;
        Iterator<AtomicReaderContext> leaveIt;
        Collector collector;
        Weight weight;
    }

    static class LuceneDocCollector extends Collector {

        private static final int KEEP_ALIVE_AFTER_ROWS = 1_000_000;
        private final KeepAliveListener keepAliveListener;
        private final RamAccountingContext ramAccountingContext;
        private final TopRowUpstream topRowUpstream;
        private final RowReceiver rowReceiver;
        private final Row inputRow;
        private final Integer limit;
        private final Collection<? extends LuceneCollectorExpression<?>> expressions;

        private int rowCount;

        public LuceneDocCollector(KeepAliveListener keepAliveListener,
                                  RamAccountingContext ramAccountingContext,
                                  TopRowUpstream topRowUpstream,
                                  Integer limit,
                                  RowReceiver rowReceiver,
                                  Row inputRow,
                                  Collection<? extends LuceneCollectorExpression<?>> expressions) {
            this.keepAliveListener = keepAliveListener;
            this.ramAccountingContext = ramAccountingContext;
            this.topRowUpstream = topRowUpstream;
            this.rowReceiver = rowReceiver;
            this.inputRow = inputRow;
            this.limit = limit;
            this.expressions = expressions;
        }

        @Override
        public void setScorer(Scorer scorer) throws IOException {
            for (LuceneCollectorExpression<?> expression : expressions) {
                expression.setScorer(scorer);
            }
        }

        @Override
        public void collect(int doc) throws IOException {
            topRowUpstream.throwIfKilled();
            checkCircuitBreaker();

            rowCount++;
            if (rowCount % KEEP_ALIVE_AFTER_ROWS == 0) {
                keepAliveListener.keepAlive();
            }
            for (LuceneCollectorExpression<?> expression : expressions) {
                expression.setNextDocId(doc);
            }
            boolean wantMore = rowReceiver.setNextRow(inputRow);
            if (!wantMore || (limit != null && rowCount >= limit)) {
                throw CollectionFinishedEarlyException.INSTANCE;
            }
            if (topRowUpstream.shouldPause()) {
                throw CollectionPauseException.INSTANCE;
            }
        }

        private void checkCircuitBreaker() throws UnexpectedCollectionTerminatedException {
            if (ramAccountingContext != null && ramAccountingContext.trippedBreaker()) {
                // stop collecting because breaker limit was reached
                throw new UnexpectedCollectionTerminatedException(
                        CrateCircuitBreakerService.breakingExceptionMessage(ramAccountingContext.contextId(),
                                ramAccountingContext.limit()));
            }
        }

        @Override
        public void setNextReader(AtomicReaderContext context) throws IOException {
            // trigger keep-alive here as well
            // in case we have a long running query without actual matches
            keepAliveListener.keepAlive();
            for (LuceneCollectorExpression<?> expression : expressions) {
                expression.setNextReader(context);
            }
        }

        @Override
        public boolean acceptsDocsOutOfOrder() {
            return false;
        }
    }
}
