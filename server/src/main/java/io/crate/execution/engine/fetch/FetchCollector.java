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

package io.crate.execution.engine.fetch;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;

import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.elasticsearch.common.lucene.index.SequentialStoredFieldsLeafReader;

import com.carrotsearch.hppc.IntArrayList;

import io.crate.Streamer;
import io.crate.data.breaker.RamAccounting;
import io.crate.execution.engine.distribution.StreamBucket;
import io.crate.expression.InputRow;
import io.crate.expression.reference.doc.lucene.CollectorContext;
import io.crate.expression.reference.doc.lucene.LuceneCollectorExpression;
import io.netty.util.collection.IntObjectHashMap;

class FetchCollector {

    private final LuceneCollectorExpression[] collectorExpressions;
    private final InputRow row;
    private final Streamer<?>[] streamers;
    private final RamAccounting ramAccounting;
    private final int readerId;
    private final FetchTask fetchTask;

    FetchCollector(List<LuceneCollectorExpression<?>> collectorExpressions,
                   Streamer<?>[] streamers,
                   FetchTask fetchTask,
                   RamAccounting ramAccounting,
                   int readerId) {
        this.fetchTask = fetchTask;
        // use toArray to avoid iterator allocations in docIds loop
        this.collectorExpressions = collectorExpressions.toArray(new LuceneCollectorExpression[0]);
        this.streamers = streamers;
        this.ramAccounting = ramAccounting;
        this.readerId = readerId;
        var table = fetchTask.table(readerId);
        CollectorContext collectorContext = new CollectorContext(readerId, table.droppedColumns(), table.lookupNameBySourceKey());
        for (LuceneCollectorExpression<?> collectorExpression : this.collectorExpressions) {
            collectorExpression.startCollect(collectorContext);
        }
        this.row = new InputRow(collectorExpressions);

    }

    private void setNextDocId(ReaderContext readerContext, int doc) throws IOException {
        for (LuceneCollectorExpression<?> e : collectorExpressions) {
            e.setNextReader(readerContext);
            e.setNextDocId(doc);
        }
    }

    public StreamBucket collect(IntArrayList docIds) {
        boolean collectSequential = isSequential(docIds);
        StreamBucket.Builder builder = new StreamBucket.Builder(streamers, ramAccounting);
        try (var borrowed = fetchTask.searcher(readerId)) {
            var searcher = borrowed.item();
            List<LeafReaderContext> leaves = searcher.getTopReaderContext().leaves();
            var readerContexts = new IntObjectHashMap<ReaderContext>(leaves.size());
            for (var cursor : docIds) {
                int docId = cursor.value;
                int readerIndex = readerIndex(docId, leaves);
                LeafReaderContext subReaderContext = leaves.get(readerIndex);
                try {
                    var readerContext = readerContexts.get(readerIndex);
                    if (readerContext == null) {
                        // If the document access is sequential, the field reader from the merge instance can be used
                        // to provide a significant speed up. However, accessing the merge CompressingStoredFieldsReader is expensive
                        // because the underlying inputData is cloned.
                        if (collectSequential && subReaderContext.reader() instanceof SequentialStoredFieldsLeafReader storedFieldsLeafReader) {
                            StoredFieldsReader sequentialStoredFieldsReader = storedFieldsLeafReader.getSequentialStoredFieldsReader();
                            readerContext = new ReaderContext(subReaderContext, sequentialStoredFieldsReader::document);
                        } else {
                            readerContext = new ReaderContext(subReaderContext);
                        }
                        readerContexts.put(readerIndex, readerContext);
                    }
                    setNextDocId(readerContext, docId - subReaderContext.docBase);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
                builder.add(row);
            }
        }
        return builder.build();
    }

    private int readerIndex(int docId, List<LeafReaderContext> leaves) {
        int readerIndex = ReaderUtil.subIndex(docId, leaves);
        if (readerIndex == -1) {
            throw new IllegalStateException("jobId=" + fetchTask.jobId() + " docId " + docId + " doesn't fit to leaves of searcher " + readerId + " fetchTask=" + fetchTask);
        }
        return readerIndex;
    }

    static boolean isSequential(IntArrayList docIds) {
        if (docIds.size() < 2) {
            return false;
        }
        // checks if doc ids are in sequential order using the following conditions:
        // (last element - first element) = (number of elements in between first and last)
        // [3,4,5,6,7] -> 7 - 3 == 4
        int last = docIds.get(docIds.size() - 1);
        int first = docIds.get(0);
        return last - first == docIds.size() - 1;
    }
}
