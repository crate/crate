/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.execution.engine.collect.collectors;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletionStage;

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocAndFloatFeatureBuffer;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.jspecify.annotations.Nullable;

import io.crate.common.exceptions.Exceptions;
import io.crate.data.BatchIterator;
import io.crate.data.Input;
import io.crate.data.Row;
import io.crate.execution.engine.fetch.ReaderContext;
import io.crate.expression.InputRow;
import io.crate.expression.reference.doc.lucene.CollectorContext;
import io.crate.expression.reference.doc.lucene.LuceneCollectorExpression;

/**
 * BatchIterator implementation which exposes the data stored in a lucene index.
 * It supports filtering the data using a lucene {@link Query} or via {@code minScore}.
 * <p>
 * Row data depends on {@code inputs} and {@code expressions}. The data is unordered.
 */
public class LuceneBatchIterator implements BatchIterator<Row> {

    private final IndexSearcher indexSearcher;
    private final Query query;
    private final CollectorContext collectorContext;
    private final boolean doScores;
    private final LuceneCollectorExpression<?>[] expressions;
    private final List<LeafReaderContext> leaves;
    private final InputRow row;
    private Weight weight;
    private final Float minScore;
    private final DocAndFloatFeatureBuffer buffer = new DocAndFloatFeatureBuffer();

    private Iterator<LeafReaderContext> leavesIt;
    private LeafReaderContext currentLeaf;
    private Scorer currentScorer;
    private int buffIdx = Integer.MAX_VALUE;
    private volatile Throwable killed;

    public LuceneBatchIterator(IndexSearcher indexSearcher,
                               Query query,
                               @Nullable Float minScore,
                               boolean doScores,
                               CollectorContext collectorContext,
                               List<? extends Input<?>> inputs,
                               Collection<? extends LuceneCollectorExpression<?>> expressions) {
        this.indexSearcher = indexSearcher;
        this.query = query;
        this.doScores = doScores || minScore != null;
        this.minScore = minScore;
        this.collectorContext = collectorContext;
        this.row = new InputRow(inputs);
        this.expressions = expressions.toArray(new LuceneCollectorExpression[0]);
        leaves = indexSearcher.getTopReaderContext().leaves();
        leavesIt = leaves.iterator();
    }

    @Override
    public Row currentElement() {
        return row;
    }

    @Override
    public void moveToStart() {
        raiseIfKilled();
        clearState();
        leavesIt = leaves.iterator();
    }

    @Override
    public boolean moveNext() {
        raiseIfKilled();
        if (weight == null) {
            try {
                weight = createWeight();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        try {
            return innerMoveNext();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private boolean innerMoveNext() throws IOException {
        while (tryAdvanceLeaf()) {
            while (getNextDocs()) {
                while (buffIdx < buffer.size) {
                    int doc = buffer.docs[buffIdx];
                    if (minScore != null && buffer.features[buffIdx] < minScore) {
                        buffIdx++;
                        continue;
                    }
                    for (LuceneCollectorExpression<?> expression : expressions) {
                        expression.setNextDocId(doc);
                    }
                    buffIdx++;
                    return true;
                }
            }
            buffIdx = Integer.MAX_VALUE;
            currentLeaf = null;
        }
        clearState();
        return false;
    }

    private boolean getNextDocs() throws IOException {
        if (buffIdx < buffer.size) {
            return true;
        }
        buffIdx = 0;
        LeafReader reader = currentLeaf.reader();
        currentScorer.nextDocsAndScores(reader.maxDoc(), reader.getLiveDocs(), buffer);
        return buffer.size > 0;
    }

    private boolean tryAdvanceLeaf() throws IOException {
        if (currentLeaf != null) {
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
            var readerContext = new ReaderContext(currentLeaf);
            for (LuceneCollectorExpression<?> expression : expressions) {
                expression.setScorer(currentScorer);
                expression.setNextReader(readerContext);
            }
            scorer.iterator().advance(0);
            return true;
        }
        return false;
    }

    private void clearState() {
        buffIdx = Integer.MAX_VALUE;
        currentScorer = null;
        currentLeaf = null;
    }

    @Override
    public void close() {
        clearState();
        killed = BatchIterator.CLOSED;
    }

    @Override
    public CompletionStage<?> loadNextBatch() throws Exception {
        throw new IllegalStateException("BatchIterator already fully loaded");
    }

    private Weight createWeight() throws IOException {
        for (LuceneCollectorExpression<?> expression : expressions) {
            expression.startCollect(collectorContext);
        }
        ScoreMode scoreMode = doScores ? ScoreMode.COMPLETE : ScoreMode.COMPLETE_NO_SCORES;
        return indexSearcher.createWeight(indexSearcher.rewrite(query), scoreMode, 1f);
    }

    @Override
    public boolean allLoaded() {
        return true;
    }

    @Override
    public boolean hasLazyResultSet() {
        return true;
    }

    private void raiseIfKilled() {
        if (killed != null) {
            Exceptions.rethrowUnchecked(killed);
        }
    }

    @Override
    public void kill(Throwable throwable) {
        killed = throwable;
    }
}
