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

package io.crate.execution.engine.collect.collectors;

import com.carrotsearch.hppc.IntArrayList;
import io.crate.data.BatchIterator;
import io.crate.exceptions.Exceptions;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.util.Bits;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.IntFunction;

public final class SegmentBatchIterator<T> implements BatchIterator<T> {

    private final Scorer scorer;
    private final Bits liveDocs;
    private final IntFunction<T> rowFromDocId;
    private final Float minScore;
    private final IntArrayList matchedDocs;

    private int idx = -1;
    private boolean movedToStart = false;
    private boolean closed = false;

    private DocIdSetIterator iterator;
    private volatile Throwable killed;

    public SegmentBatchIterator(Scorer scorer,
                                LeafReaderContext leafReader,
                                IntFunction<T> rowFromDocId,
                                @Nullable Float minScore) {
        this.rowFromDocId = rowFromDocId;
        this.minScore = minScore;
        this.scorer = scorer;
        this.iterator = scorer.iterator();
        this.liveDocs = leafReader.reader().getLiveDocs();
        this.matchedDocs = new IntArrayList(leafReader.reader().numDocs());
    }

    @Override
    public T currentElement() {
        if (movedToStart) {
            return rowFromDocId.apply(matchedDocs.get(idx));
        } else {
            return rowFromDocId.apply(iterator.docID());
        }
    }

    @Override
    public void moveToStart() {
        raiseIfClosedOrKilled();
        idx = -1;
        movedToStart = true;
    }

    @Override
    public boolean moveNext() {
        raiseIfClosedOrKilled();
        if (movedToStart) {
            idx++;
            return idx < matchedDocs.size();
        }
        if (iterator.docID() == DocIdSetIterator.NO_MORE_DOCS) {
            return false;
        }
        try {
            int doc;
            while ((doc = iterator.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                if (docDeleted(doc) || belowMinScore(scorer)) {
                    continue;
                }
                matchedDocs.add(doc);
                return true;
            }
            return false;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private boolean docDeleted(int doc) {
        if (liveDocs == null) {
            return false;
        }
        return liveDocs.get(doc) == false;
    }

    private boolean belowMinScore(Scorer currentScorer) throws IOException {
        return minScore != null && currentScorer.score() < minScore;
    }

    @Override
    public void close() {
        closed = true;
    }

    @Override
    public CompletionStage<?> loadNextBatch() {
        return CompletableFuture.failedFuture(new IllegalStateException("BatchIterator is already fully loaded"));
    }

    @Override
    public boolean allLoaded() {
        return true;
    }

    @Override
    public boolean involvesIO() {
        return true;
    }

    @Override
    public void kill(@Nonnull Throwable throwable) {
        killed = throwable;
    }

    private void raiseIfClosedOrKilled() {
        if (killed != null) {
            Exceptions.rethrowUnchecked(killed);
        }
        if (closed) {
            throw new IllegalStateException("BatchIterator is closed");
        }
    }
}
