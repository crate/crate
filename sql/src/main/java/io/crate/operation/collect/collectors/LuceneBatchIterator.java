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
import io.crate.concurrent.CompletableFutures;
import io.crate.data.BatchIterator;
import io.crate.data.Columns;
import io.crate.data.Input;
import io.crate.operation.reference.doc.lucene.CollectorContext;
import io.crate.operation.reference.doc.lucene.LuceneCollectorExpression;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.*;
import org.apache.lucene.util.Bits;
import org.elasticsearch.common.breaker.CircuitBreakingException;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletionStage;

/**
 * BatchIterator implementation which exposes the data stored in a lucene index.
 * It supports filtering the data using a lucene {@link Query} or via {@code minScore}.
 *
 * Row data depends on {@code inputs} and {@code expressions}. The data is unordered.
 */
public class LuceneBatchIterator implements BatchIterator {

    private final IndexSearcher indexSearcher;
    private final Query query;
    private final CollectorContext collectorContext;
    private final RamAccountingContext ramAccountingContext;
    private final boolean doScores;
    private final LuceneCollectorExpression[] expressions;
    private final List<LeafReaderContext> leaves;
    private final Weight weight;
    private final Columns inputs;
    private final CollectorFieldsVisitor visitor;
    private final Float minScore;

    private Iterator<LeafReaderContext> leavesIt;
    private LeafReaderContext currentLeaf;
    private Scorer currentScorer;
    private DocIdSetIterator currentDocIdSetIt;
    private boolean closed = false;

    LuceneBatchIterator(IndexSearcher indexSearcher,
                        Query query,
                        @Nullable Float minScore,
                        boolean doScores,
                        CollectorContext collectorContext,
                        RamAccountingContext ramAccountingContext,
                        List<? extends Input<?>> inputs,
                        Collection<? extends LuceneCollectorExpression<?>> expressions) {
        this.indexSearcher = indexSearcher;
        this.query = query;
        this.doScores = doScores || minScore != null;
        this.minScore = minScore;
        this.collectorContext = collectorContext;
        this.visitor = collectorContext.visitor();
        this.ramAccountingContext = ramAccountingContext;
        this.inputs = Columns.wrap(inputs);
        this.expressions = expressions.toArray(new LuceneCollectorExpression[0]);
        leaves = indexSearcher.getTopReaderContext().leaves();
        leavesIt = leaves.iterator();
        try {
            weight = createWeight();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Columns rowData() {
        return inputs;
    }

    @Override
    public void moveToStart() {
        checkClosed();
        leavesIt = leaves.iterator();
    }

    @Override
    public boolean moveNext() {
        checkClosed();
        try {
            return innerMoveNext();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private boolean innerMoveNext() throws IOException {
        while (tryAdvanceDocIdSetIterator()) {
            LeafReader reader = currentLeaf.reader();
            Bits liveDocs = reader.getLiveDocs();
            int doc;
            while ((doc = currentDocIdSetIt.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                if (docDeleted(liveDocs, doc) || belowMinScore(currentScorer)) {
                    continue;
                }
                onDoc(doc, reader);
                return true;
            }
            currentDocIdSetIt = null;
        }
        clearState();
        return false;
    }

    private boolean belowMinScore(Scorer currentScorer) throws IOException {
        return minScore != null && currentScorer.score() < minScore;
    }

    private boolean tryAdvanceDocIdSetIterator() throws IOException {
        if (currentDocIdSetIt != null) {
            return true;
        }
        while (leavesIt.hasNext()) {
            LeafReaderContext leaf = leavesIt.next();
            Scorer scorer = weight.scorer(leaf);
            if (scorer == null) {
                continue;
            }
            currentScorer = scorer;
            currentLeaf = leaf;
            currentDocIdSetIt = scorer.iterator();
            for (LuceneCollectorExpression expression : expressions) {
                expression.setScorer(currentScorer);
                expression.setNextReader(currentLeaf);
            }
            return true;
        }
        return false;
    }

    private void clearState() {
        currentDocIdSetIt = null;
        currentScorer = null;
        currentLeaf = null;
    }

    @Override
    public void close() {
        closed = true;
        clearState();
    }

    @Override
    public CompletionStage<?> loadNextBatch() {
        if (closed) {
            return CompletableFutures.failedFuture(new IllegalStateException("BatchIterator is closed"));
        }
        return CompletableFutures.failedFuture(new IllegalStateException("BatchIterator already fully loaded"));
    }

    private Weight createWeight() throws IOException {
        for (LuceneCollectorExpression expression : expressions) {
            expression.startCollect(collectorContext);
        }
        return indexSearcher.createNormalizedWeight(query, doScores);
    }

    @Override
    public boolean allLoaded() {
        checkClosed();
        return true;
    }

    private static boolean docDeleted(@Nullable Bits liveDocs, int doc) {
        if (liveDocs == null) {
            return false;
        }
        return liveDocs.get(doc) == false;
    }

    private void checkCircuitBreaker() throws CircuitBreakingException {
        if (ramAccountingContext != null && ramAccountingContext.trippedBreaker()) {
            // stop collecting because breaker limit was reached
            throw new CircuitBreakingException(
                CrateCircuitBreakerService.breakingExceptionMessage(ramAccountingContext.contextId(),
                    ramAccountingContext.limit()));
        }
    }

    private void onDoc(int doc, LeafReader reader) throws IOException {
        checkCircuitBreaker();
        if (visitor.required()) {
            visitor.reset();
            reader.document(doc, visitor);
        }
        for (LuceneCollectorExpression expression : expressions) {
            expression.setNextDocId(doc);
        }
    }

    private void checkClosed() {
        if (closed) {
            throw new IllegalStateException("BatchIterator is closed");
        }
    }
}
