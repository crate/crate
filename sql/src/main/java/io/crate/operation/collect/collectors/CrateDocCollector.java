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

import io.crate.breaker.CrateCircuitBreakerService;
import io.crate.breaker.RamAccountingContext;
import io.crate.data.Input;
import io.crate.operation.InputRow;
import io.crate.operation.collect.CrateCollector;
import io.crate.operation.projectors.ExecutorResumeHandle;
import io.crate.operation.projectors.RepeatHandle;
import io.crate.operation.projectors.RowReceiver;
import io.crate.operation.reference.doc.lucene.CollectorContext;
import io.crate.operation.reference.doc.lucene.LuceneCollectorExpression;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.*;
import org.apache.lucene.util.Bits;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.index.shard.ShardId;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.function.Predicate;

public class CrateDocCollector implements CrateCollector, RepeatHandle {

    private static final Logger LOGGER = Loggers.getLogger(CrateDocCollector.class);

    private final CollectorContext collectorContext;
    private final ShardId shardId;
    private final IndexSearcher indexSearcher;
    private final Query query;
    private final RowReceiver rowReceiver;
    private final LuceneCollectorExpression<?>[] expressions;
    private final State state = new State();
    private final ExecutorResumeHandle resumeable;
    private final boolean doScores;
    private final RamAccountingContext ramAccountingContext;
    private final InputRow inputRow;
    private final Predicate<Scorer> filter;
    private DocReaderConsumer docReaderConsumer;

    public static class Builder implements CrateCollector.Builder {

        private final ShardId shardId;
        private final IndexSearcher indexSearcher;
        private final Query query;
        private final Float minScore;
        private final Executor executor;
        private final boolean doScores;
        private final CollectorContext collectorContext;
        private final RamAccountingContext ramAccountingContext;
        private final List<Input<?>> inputs;
        private final Collection<? extends LuceneCollectorExpression<?>> expressions;

        public Builder(ShardId shardId,
                       IndexSearcher indexSearcher,
                       Query query,
                       Float minScore,
                       Executor executor,
                       boolean doScores,
                       CollectorContext collectorContext,
                       RamAccountingContext ramAccountingContext,
                       List<Input<?>> inputs,
                       Collection<? extends LuceneCollectorExpression<?>> expressions) {
            this.shardId = shardId;
            this.indexSearcher = indexSearcher;
            this.query = query;
            this.minScore = minScore;
            this.executor = executor;
            this.doScores = doScores;
            this.collectorContext = collectorContext;
            this.ramAccountingContext = ramAccountingContext;
            this.inputs = inputs;
            this.expressions = expressions;
        }

        @Override
        public CrateCollector build(RowReceiver rowReceiver) {
            return new CrateDocCollector(
                shardId,
                indexSearcher,
                query,
                minScore,
                executor,
                doScores,
                collectorContext,
                ramAccountingContext,
                rowReceiver,
                inputs,
                expressions
            );
        }
    }

    public CrateDocCollector(ShardId shardId,
                             IndexSearcher indexSearcher,
                             Query query,
                             Float minScore,
                             Executor executor,
                             boolean doScores,
                             CollectorContext collectorContext,
                             RamAccountingContext ramAccountingContext,
                             RowReceiver rowReceiver,
                             List<Input<?>> inputs,
                             Collection<? extends LuceneCollectorExpression<?>> expressions) {
        this.shardId = shardId;
        this.indexSearcher = indexSearcher;
        this.query = query;
        this.collectorContext = collectorContext;
        this.rowReceiver = rowReceiver;
        this.doScores = doScores || minScore != null;
        this.filter = createMinScorePredicate(minScore);
        this.ramAccountingContext = ramAccountingContext;
        this.inputRow = new InputRow(inputs);
        this.expressions = expressions.toArray(new LuceneCollectorExpression[0]);
        this.resumeable = new ExecutorResumeHandle(executor, new Runnable() {
            @Override
            public void run() {
                traceLog("resume collect");
                innerCollect(state.weight, state.leaveIt, state.scorer, state.it, state.leaf);
            }
        });
    }

    private static Predicate<Scorer> createMinScorePredicate(Float minScore) {
        if (minScore == null) {
            return s -> true;
        } else {
            return s -> {
                try {
                    return s.score() >= minScore;
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            };
        }
    }

    private void debugLog(String message) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("{} {} {}", Thread.currentThread().getName(), shardId, message);
        }
    }

    private void traceLog(String message) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("{} {} {}", Thread.currentThread().getName(), shardId, message);
        }
    }

    @Override
    public void doCollect() {
        debugLog("doCollect");
        for (LuceneCollectorExpression<?> expression : expressions) {
            expression.startCollect(collectorContext);
        }
        docReaderConsumer = createOnDocFunction(collectorContext.visitor());
        Weight weight;
        Iterator<LeafReaderContext> leavesIt;
        try {
            weight = indexSearcher.createNormalizedWeight(query, doScores);
            leavesIt = indexSearcher.getTopReaderContext().leaves().iterator();
        } catch (Throwable e) {
            fail(e);
            return;
        }
        // these won't change anymore, so safe the state once in case there is a pause or resume
        state.weight = weight;
        state.leaveIt = leavesIt;

        innerCollect(weight, leavesIt, null, null, null);
    }

    private DocReaderConsumer createOnDocFunction(CollectorFieldsVisitor visitor) {
        if (visitor.required()) {
            return (doc, reader) -> {
                checkCircuitBreaker();
                visitor.reset();
                reader.document(doc, visitor);
                for (LuceneCollectorExpression<?> expression : expressions) {
                    expression.setNextDocId(doc);
                }
            };
        }
        return (doc, reader) -> {
            checkCircuitBreaker();
            for (LuceneCollectorExpression<?> expression : expressions) {
                expression.setNextDocId(doc);
            }
        };
    }

    private void innerCollect(Weight weight,
                              Iterator<LeafReaderContext> leavesIt,
                              @Nullable Scorer scorer,
                              @Nullable DocIdSetIterator it,
                              @Nullable LeafReaderContext leaf) {
        try {
            RowReceiver.Result result;
            if (scorer == null) {
                result = consumeLeaves(weight, leavesIt);
            } else {
                result = consumeIterator(scorer, leaf, it);
                if (result == RowReceiver.Result.CONTINUE) {
                    result = consumeLeaves(weight, leavesIt);
                }
            }
            pauseOrFinish(result);
        } catch (Throwable t) {
            fail(t);
        }
    }

    private void pauseOrFinish(RowReceiver.Result result) {
        switch (result) {
            case PAUSE:
                traceLog("paused collect");
                rowReceiver.pauseProcessed(resumeable);
                break;

            case CONTINUE: // no more leaves to process
            case STOP:
                debugLog("finished collect");
                rowReceiver.finish(this);
        }
    }

    private void fail(Throwable t) {
        debugLog("finished collect with failure");
        rowReceiver.fail(t);
    }

    private RowReceiver.Result consumeLeaves(Weight weight, Iterator<LeafReaderContext> leaves) throws IOException {
        LeafReaderContext leaf;
        Scorer scorer;
        while (leaves.hasNext()) {
            leaf = leaves.next();
            scorer = weight.scorer(leaf);
            if (scorer == null) {
                continue;
            }
            RowReceiver.Result result = consumeIterator(scorer, leaf, scorer.iterator());
            if (result != RowReceiver.Result.CONTINUE) {
                return result;
            }
        }
        return RowReceiver.Result.CONTINUE;
    }

    private RowReceiver.Result consumeIterator(Scorer scorer, LeafReaderContext leaf, DocIdSetIterator it) throws IOException {
        LeafReader reader = leaf.reader();
        Bits liveDocs = reader.getLiveDocs();
        for (LuceneCollectorExpression<?> expression : expressions) {
            expression.setScorer(scorer);
            expression.setNextReader(leaf);
        }
        for (int doc = it.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = it.nextDoc()) {
            if (docDeleted(liveDocs, doc)) {
                continue;
            }
            docReaderConsumer.onDoc(doc, reader);
            if (filter.test(scorer) == false) {
                continue;
            }
            RowReceiver.Result result = rowReceiver.setNextRow(inputRow);
            switch (result) {
                case CONTINUE:
                    continue;
                case PAUSE:
                    state.it = it;
                    state.leaf = leaf;
                    state.scorer = scorer;
                    return RowReceiver.Result.PAUSE;
                case STOP:
                    return RowReceiver.Result.STOP;
            }
            throw new AssertionError("Unrecognized setNextRow result: " + result);
        }
        return RowReceiver.Result.CONTINUE;
    }

    private void checkCircuitBreaker() throws CircuitBreakingException {
        if (ramAccountingContext != null && ramAccountingContext.trippedBreaker()) {
            // stop collecting because breaker limit was reached
            throw new CircuitBreakingException(
                CrateCircuitBreakerService.breakingExceptionMessage(ramAccountingContext.contextId(),
                    ramAccountingContext.limit()));
        }
    }

    private static boolean docDeleted(@Nullable Bits liveDocs, int doc) {
        if (liveDocs == null) {
            return false;
        }
        return liveDocs.get(doc) == false;
    }

    @Override
    public void kill(@Nullable Throwable throwable) {
        debugLog("kill CrateDocCollector");
        rowReceiver.kill(throwable);
    }

    @Override
    public void repeat() {
        debugLog("repeat collect");
        Iterator<LeafReaderContext> iterator = indexSearcher.getTopReaderContext().leaves().iterator();
        innerCollect(state.weight, iterator, null, null, null);
    }

    interface DocReaderConsumer {
        void onDoc(int doc, LeafReader reader) throws IOException;
    }

    static class State {
        Scorer scorer;
        DocIdSetIterator it;
        Iterator<LeafReaderContext> leaveIt;
        Weight weight;
        LeafReaderContext leaf;
    }
}
