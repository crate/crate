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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.CompletionStage;

import com.carrotsearch.hppc.IntArrayList;

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.util.Bits;

import io.crate.common.CheckedFunction;
import io.crate.data.BatchIterator;
import io.crate.exceptions.Exceptions;

public class SegmentBatchIterator implements BatchIterator<IntArrayList> {

    private static final int BLOCK_SIZE = 1000;

    private final CheckedFunction<LeafReaderContext, Scorer, IOException> getScorer;
    private final IntArrayList docIds;
    private final LeafReaderContext leaf;
    private final Bits liveDocs;

    private DocIdSetIterator iterator;
    private volatile Throwable killed;


    public SegmentBatchIterator(CheckedFunction<LeafReaderContext, Scorer, IOException> getScorer,
                                LeafReaderContext leaf) throws IOException {
        final Scorer scorer = getScorer.apply(leaf);
        final LeafReader reader = leaf.reader();

        this.leaf = leaf;
        this.getScorer = getScorer;
        this.liveDocs = reader.getLiveDocs();
        this.iterator = scorer == null ? DocIdSetIterator.empty() : scorer.iterator();
        this.docIds = new IntArrayList(BLOCK_SIZE);
    }

    @Override
    public void kill(Throwable throwable) {
        this.killed = throwable;
    }

    @Override
    public IntArrayList currentElement() {
        return docIds;
    }

    @Override
    public void moveToStart() {
        Scorer scorer;
        try {
            scorer = getScorer.apply(leaf);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        iterator = scorer == null ? DocIdSetIterator.empty() : scorer.iterator();
        docIds.clear();
    }

    @Override
    public boolean moveNext() {
        docIds.clear();
        if (killed != null) {
            Exceptions.rethrowUnchecked(killed);
        }
        if (iterator.docID() == DocIdSetIterator.NO_MORE_DOCS) {
            return false;
        }
        int doc;
        try {
            while (docIds.size() < BLOCK_SIZE && (doc = iterator.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                if (liveDocs != null && liveDocs.get(doc) == false) {
                    continue;
                }
                docIds.add(doc);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return !docIds.isEmpty();
    }

    @Override
    public void close() {
        killed = BatchIterator.CLOSED;
    }

    @Override
    public CompletionStage<?> loadNextBatch() throws Exception {
        throw new UnsupportedOperationException("SegmentBatchIterator is already fully loaded");
    }

    @Override
    public boolean allLoaded() {
        return true;
    }

    @Override
    public boolean hasLazyResultSet() {
        return true;
    }
}
