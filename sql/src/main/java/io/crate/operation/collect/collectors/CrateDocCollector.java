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
import io.crate.operation.Input;
import io.crate.operation.InputRow;
import io.crate.operation.collect.CollectionFinishedEarlyException;
import io.crate.operation.collect.CollectionPauseException;
import io.crate.operation.collect.CrateCollector;
import io.crate.operation.projectors.ExecutorResumeHandle;
import io.crate.operation.projectors.RepeatHandle;
import io.crate.operation.projectors.RowReceiver;
import io.crate.operation.reference.doc.lucene.CollectorContext;
import io.crate.operation.reference.doc.lucene.LuceneCollectorExpression;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.*;
import org.apache.lucene.util.Bits;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.lucene.MinimumScoreCollector;
import org.elasticsearch.search.internal.ContextIndexSearcher;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Executor;

public class CrateDocCollector implements CrateCollector, RepeatHandle {

    private static final ESLogger LOGGER = Loggers.getLogger(CrateDocCollector.class);

    private final CollectorContext collectorContext;
    private final CrateSearchContext searchContext;
    private final RowReceiver rowReceiver;
    private final Collection<? extends LuceneCollectorExpression<?>> expressions;
    private final SimpleCollector luceneCollector;
    private final State state = new State();
    private final ExecutorResumeHandle resumeable;
    private final boolean doScores;

    public static class Builder implements CrateCollector.Builder {

        private final CrateSearchContext searchContext;
        private final Executor executor;
        private final boolean doScores;
        private final RamAccountingContext ramAccountingContext;
        private final List<Input<?>> inputs;
        private final Collection<? extends LuceneCollectorExpression<?>> expressions;
        private final byte relationId;

        public Builder(CrateSearchContext searchContext,
                       Executor executor,
                       boolean doScores,
                       RamAccountingContext ramAccountingContext,
                       List<Input<?>> inputs,
                       Collection<? extends LuceneCollectorExpression<?>> expressions,
                       byte relationId) {
            this.searchContext = searchContext;
            this.executor = executor;
            this.doScores = doScores;
            this.ramAccountingContext = ramAccountingContext;
            this.inputs = inputs;
            this.expressions = expressions;
            this.relationId = relationId;
        }

        @Override
        public CrateCollector build(RowReceiver rowReceiver) {
            return new CrateDocCollector(
                searchContext,
                executor,
                doScores,
                ramAccountingContext,
                rowReceiver,
                inputs,
                expressions,
                relationId
            );
        }
    }

    CrateDocCollector(final CrateSearchContext searchContext,
                             Executor executor,
                             boolean doScores,
                             RamAccountingContext ramAccountingContext,
                             RowReceiver rowReceiver,
                             List<Input<?>> inputs,
                             Collection<? extends LuceneCollectorExpression<?>> expressions,
                             byte relationId) {
        this.searchContext = searchContext;
        this.rowReceiver = rowReceiver;
        this.expressions = expressions;
        CollectorFieldsVisitor fieldsVisitor = new CollectorFieldsVisitor(expressions.size());
        collectorContext = new CollectorContext(
            searchContext.mapperService(),
            searchContext.fieldData(),
            fieldsVisitor,
            (int) searchContext.id(),
            relationId
        );
        this.doScores = doScores || searchContext.minimumScore() != null;
        SimpleCollector collector = new LuceneDocCollector(
            ramAccountingContext,
            rowReceiver,
            this.doScores,
            new InputRow(inputs),
            expressions
        );
        if (searchContext.minimumScore() != null) {
            collector = new MinimumScoreCollector(collector, searchContext.minimumScore());
        }
        luceneCollector = collector;
        this.resumeable = new ExecutorResumeHandle(executor, new Runnable() {
            @Override
            public void run() {
                traceLog("resume collect");
                innerCollect(state.collector, state.weight, state.leaveIt, state.bulkScorer, state.leaf);
            }
        });
    }

    private void debugLog(String message) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("{} {} {}", Thread.currentThread().getName(), searchContext.indexShard().shardId(), message);
        }
    }

    private void traceLog(String message) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("{} {} {}", Thread.currentThread().getName(), searchContext.indexShard().shardId(), message);
        }
    }

    @Override
    public void doCollect() {
        debugLog("doCollect");
        for (LuceneCollectorExpression<?> expression : expressions) {
            expression.startCollect(collectorContext);
        }
        SimpleCollector collector = luceneCollector;
        if (collectorContext.visitor().required()) {
            collector = new FieldVisitorCollector(collector, collectorContext.visitor());
        }
        ContextIndexSearcher contextIndexSearcher = searchContext.searcher();

        Weight weight;
        Iterator<LeafReaderContext> leavesIt;
        try {
            weight = searchContext.engineSearcher().searcher().createNormalizedWeight(searchContext.query(), doScores);
            leavesIt = contextIndexSearcher.getTopReaderContext().leaves().iterator();
        } catch (Throwable e) {
            fail(e);
            return;
        }
        // these won't change anymore, so safe the state once in case there is a pause or resume
        state.collector = collector;
        state.weight = weight;
        state.leaveIt = leavesIt;

        innerCollect(collector, weight, leavesIt, null, null);
    }

    private void innerCollect(SimpleCollector collector, Weight weight, Iterator<LeafReaderContext> leavesIt,
                              @Nullable BulkScorer scorer, @Nullable LeafReaderContext leaf) {
        try {
            if (collectLeaves(collector, weight, leavesIt, scorer, leaf) == RowReceiver.Result.PAUSE) {
                traceLog("paused collect");
            } else {
                finishCollect();
            }
        } catch (CollectionFinishedEarlyException e) {
            finishCollect();
        } catch (Throwable t) {
            fail(t);
        }
    }

    private void fail(Throwable t) {
        debugLog("finished collect with failure");
        rowReceiver.fail(t);
    }

    private void finishCollect() {
        debugLog("finished collect");
        rowReceiver.finish(this);
    }

    private RowReceiver.Result collectLeaves(SimpleCollector collector,
                                             Weight weight,
                                             Iterator<LeafReaderContext> leaves,
                                             @Nullable BulkScorer bulkScorer,
                                             @Nullable LeafReaderContext leaf) throws IOException {
        if (bulkScorer != null) {
            assert leaf != null : "leaf must not be null if bulkScorer isn't null";
            if (processScorer(collector, leaf, bulkScorer)) return RowReceiver.Result.PAUSE;
        }
        while (leaves.hasNext()) {
            leaf = leaves.next();
            LeafCollector leafCollector = collector.getLeafCollector(leaf);
            Scorer scorer = weight.scorer(leaf);
            if (scorer == null) {
                continue;
            }
            bulkScorer = new DefaultBulkScorer(scorer);
            if (processScorer(leafCollector, leaf, bulkScorer)) return RowReceiver.Result.PAUSE;
        }
        return RowReceiver.Result.CONTINUE;
    }

    private boolean processScorer(LeafCollector leafCollector, LeafReaderContext leaf, BulkScorer scorer) throws IOException {
        try {
            scorer.score(leafCollector, leaf.reader().getLiveDocs());
        } catch (CollectionPauseException e) {
            state.leaf = leaf;
            state.bulkScorer = scorer;
            rowReceiver.pauseProcessed(resumeable);
            return true;
        }
        return false;
    }

    @Override
    public void kill(@Nullable Throwable throwable) {
        debugLog("kill searchContext=" + searchContext);
        rowReceiver.kill(throwable);
    }

    @Override
    public void repeat() {
        debugLog("repeat collect");
        ContextIndexSearcher indexSearcher = searchContext.searcher();
        Iterator<LeafReaderContext> iterator = indexSearcher.getTopReaderContext().leaves().iterator();
        innerCollect(state.collector, state.weight, iterator, null, null);
    }

    static class State {
        BulkScorer bulkScorer;
        Iterator<LeafReaderContext> leaveIt;
        SimpleCollector collector;
        Weight weight;
        LeafReaderContext leaf;
    }

    private static class LuceneDocCollector extends SimpleCollector {

        private final RamAccountingContext ramAccountingContext;
        private final RowReceiver rowReceiver;
        private final boolean doScores;
        private final Row inputRow;
        private final LuceneCollectorExpression[] expressions;

        LuceneDocCollector(RamAccountingContext ramAccountingContext,
                           RowReceiver rowReceiver,
                           boolean doScores,
                           Row inputRow,
                           Collection<? extends LuceneCollectorExpression<?>> expressions) {
            this.ramAccountingContext = ramAccountingContext;
            this.rowReceiver = rowReceiver;
            this.doScores = doScores;
            this.inputRow = inputRow;
            this.expressions = expressions.toArray(new LuceneCollectorExpression[0]);
        }


        @Override
        public boolean needsScores() {
            return doScores;
        }

        @Override
        public void setScorer(Scorer scorer) throws IOException {
            for (LuceneCollectorExpression<?> expression : expressions) {
                expression.setScorer(scorer);
            }
        }


        @Override
        public void collect(int doc) throws IOException {
            checkCircuitBreaker();

            for (LuceneCollectorExpression<?> expression : expressions) {
                expression.setNextDocId(doc);
            }
            RowReceiver.Result result = rowReceiver.setNextRow(inputRow);
            switch (result) {
                case CONTINUE:
                    return;
                case PAUSE:
                    throw CollectionPauseException.INSTANCE;
                case STOP:
                    throw CollectionFinishedEarlyException.INSTANCE;
            }
            throw new AssertionError("Unrecognized setNextRow result: " + result);
        }

        private void checkCircuitBreaker() throws CircuitBreakingException {
            if (ramAccountingContext != null && ramAccountingContext.trippedBreaker()) {
                // stop collecting because breaker limit was reached
                throw new CircuitBreakingException(
                    CrateCircuitBreakerService.breakingExceptionMessage(ramAccountingContext.contextId(),
                        ramAccountingContext.limit()));
            }
        }

        @Override
        protected void doSetNextReader(LeafReaderContext context) throws IOException {
            // trigger keep-alive here as well
            // in case we have a long running query without actual matches
            for (LuceneCollectorExpression<?> expression : expressions) {
                expression.setNextReader(context);
            }
        }
    }

    private static class DefaultBulkScorer extends BulkScorer {

        private final Scorer scorer;
        private final DocIdSetIterator iterator;

        DefaultBulkScorer(Scorer scorer) {
            this.scorer = scorer;
            this.iterator = scorer.iterator();
        }

        @Override
        public int score(LeafCollector collector, Bits acceptDocs, int min, int max) throws IOException {
            // TODO: figure out if min/max can be used to optimize this and still work correctly with pause/resume
            // and also check if twoPhaseIterator can be used
            collector.setScorer(scorer);
            for (int doc = iterator.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = iterator.nextDoc()) {
                if (acceptDocs == null || acceptDocs.get(doc)) {
                    collector.collect(doc);
                }
            }
            return DocIdSetIterator.NO_MORE_DOCS;
        }

        @Override
        public long cost() {
            return iterator.cost();
        }
    }
}
