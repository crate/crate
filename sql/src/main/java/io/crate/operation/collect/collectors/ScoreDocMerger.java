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

import com.carrotsearch.hppc.IntObjectOpenHashMap;
import com.carrotsearch.hppc.IntOpenHashSet;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;
import com.google.common.util.concurrent.MoreExecutors;
import io.crate.core.collections.Row;
import io.crate.jobs.ExecutionState;
import io.crate.operation.Input;
import io.crate.operation.InputRow;
import io.crate.operation.PageConsumeListener;
import io.crate.operation.collect.collectors.TopRowUpstream;
import io.crate.operation.merge.SortedPagingIterator;
import io.crate.operation.projectors.IterableRowEmitter;
import io.crate.operation.projectors.RowReceiver;
import io.crate.operation.reference.doc.lucene.LuceneCollectorExpression;
import io.crate.operation.reference.doc.lucene.OrderByCollectorExpression;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Scorer;
import sun.reflect.generics.scope.DummyScope;

import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class ScoreDocMerger implements ExecutionState {

    private final SortedPagingIterator<Row> pagingIterator;
    private final RowReceiver rowReceiver;
    private final TopRowUpstream topRowUpstream;

    private final IntObjectOpenHashMap<ScoreDocRowFunction> upstreams = new IntObjectOpenHashMap<>();
    private final AtomicReference<Throwable> failure = new AtomicReference<>(null);
    private final List<Iterable<Row>> rowsList = new ArrayList<>();
    private final List<PageConsumeListener> pageConsumeListeners = new ArrayList<>();

    private final IntOpenHashSet activeUpstreams = new IntOpenHashSet();
    private final AtomicInteger registeredUpstreams = new AtomicInteger();
    private int rowCount = 0;

    public ScoreDocMerger(RowReceiver rowReceiver,
                          Ordering<Row> rowOrdering) {
        topRowUpstream = new TopRowUpstream(MoreExecutors.directExecutor(), new Runnable() {
            @Override
            public void run() {
                processIterator();
            }
        });
        rowReceiver.setUpstream(topRowUpstream);
        this.rowReceiver = rowReceiver;
        pagingIterator = new SortedPagingIterator<>(rowOrdering);
    }


    public int registerUpstream(List<Input<?>> inputs,
                                Collection<? extends LuceneCollectorExpression<?>> expressions,
                                IndexReader indexReader,
                                DummyScorer scorer,
                                PageConsumeListener pageConsumeListener) {

        pageConsumeListeners.add(pageConsumeListener);
        ScoreDocRowFunction rowFunction = new ScoreDocRowFunction(indexReader, inputs, expressions, scorer);

        int i = registeredUpstreams.getAndIncrement();
        synchronized (upstreams) {
            activeUpstreams.add(i);
            upstreams.put(i, rowFunction);
        }
        return i;
    }

    public void setNextScoreDocs(int upstreamIdx, ScoreDoc[] scoreDocs) {
        Iterable<Row> rows = Iterables.transform(Arrays.asList(scoreDocs), upstreams.get(upstreamIdx));

        int i;
        synchronized (activeUpstreams) {
            rowsList.add(rows);
            activeUpstreams.remove(upstreamIdx);
            i = activeUpstreams.size();
        }
        if (i == 0) {
            pagingIterator.merge(rowsList);
            rowsList.clear();

            processIterator();
        }
    }

    private void processIterator() {
        while (pagingIterator.hasNext()) {
            Row row = pagingIterator.next();
            rowCount++;

            boolean wantMore = rowReceiver.setNextRow(row);
            if (topRowUpstream.shouldPause()) {
                topRowUpstream.pauseProcessed();
                return;
            }
            if (!wantMore) {
                for (PageConsumeListener pageConsumeListener : pageConsumeListeners) {
                    pageConsumeListener.finish();
                }
                return;
            }
        }

        // TODO: could be improved by keeping track in the PagingIterator on which Iterator was exhausted so that
        // needMore is only called on one collector / upstream
        for (int i = 0; i < pageConsumeListeners.size(); i++) {
            activeUpstreams.add(i);
        }
        for (PageConsumeListener pageConsumeListener : pageConsumeListeners) {
            pageConsumeListener.needMore();
        }
    }

    public void finish(int upstreamIdx) {
        countdown();
    }

    public void fail(int upstreamIdx, Throwable t) {
        failure.set(t);
        countdown();
    }

    private void countdown() {
        if (registeredUpstreams.decrementAndGet() == 0) {
            Throwable throwable = failure.get();
            if (throwable == null) {
                pagingIterator.finish();
                if (pagingIterator.hasNext()) {
                    IterableRowEmitter rowEmitter = new IterableRowEmitter(rowReceiver, this, pagingIterator);
                    rowEmitter.run();
                } else {
                    rowReceiver.finish();
                }
            } else {
                rowReceiver.fail(throwable);
            }
        }
    }

    @Override
    public boolean isKilled() {
        // TODO: implement
        return false;
    }

    private static class ScoreDocRowFunction implements Function<ScoreDoc, Row> {

        private final List<OrderByCollectorExpression> orderByCollectorExpressions = new ArrayList<>();
        private final IndexReader indexReader;
        private final Collection<? extends LuceneCollectorExpression<?>> expressions;
        private final DummyScorer scorer;
        private final InputRow inputRow;

        public ScoreDocRowFunction(IndexReader indexReader,
                                   List<Input<?>> inputs,
                                   Collection<? extends LuceneCollectorExpression<?>> expressions,
                                   DummyScorer scorer) {
            this.indexReader = indexReader;
            this.expressions = expressions;
            this.scorer = scorer;
            this.inputRow = new InputRow(inputs);
            addOrderByExpressions(expressions);
        }

        private void addOrderByExpressions(Collection<? extends LuceneCollectorExpression<?>> expressions) {
            for (LuceneCollectorExpression<?> expression : expressions) {
                if (expression instanceof OrderByCollectorExpression) {
                    orderByCollectorExpressions.add((OrderByCollectorExpression) expression);
                }
            }
        }

        @Nullable
        @Override
        public Row apply(@Nullable ScoreDoc input) {
            if (input == null) {
                return null;
            }
            FieldDoc fieldDoc = (FieldDoc) input;
            scorer.score(fieldDoc.score);
            for (OrderByCollectorExpression orderByCollectorExpression : orderByCollectorExpressions) {
                orderByCollectorExpression.setNextFieldDoc(fieldDoc);
            }
            List<AtomicReaderContext> leaves = indexReader.leaves();
            int readerIndex = ReaderUtil.subIndex(fieldDoc.doc, leaves);
            AtomicReaderContext subReaderContext = leaves.get(readerIndex);
            int subDoc = fieldDoc.doc - subReaderContext.docBase;
            for (LuceneCollectorExpression<?> expression : expressions) {
                expression.setNextReader(subReaderContext);
                expression.setNextDocId(subDoc);
            }
            return inputRow;
        }
    }
}
