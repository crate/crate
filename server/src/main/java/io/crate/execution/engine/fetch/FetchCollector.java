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

package io.crate.execution.engine.fetch;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import com.carrotsearch.hppc.IntContainer;

import com.carrotsearch.hppc.cursors.IntCursor;
import io.netty.util.collection.IntObjectHashMap;
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;

import io.crate.Streamer;
import io.crate.breaker.RamAccounting;
import io.crate.exceptions.Exceptions;
import io.crate.execution.engine.distribution.StreamBucket;
import io.crate.expression.InputRow;
import io.crate.expression.reference.doc.lucene.CollectorContext;
import io.crate.expression.reference.doc.lucene.LuceneCollectorExpression;
import org.elasticsearch.common.lucene.index.SequentialStoredFieldsLeafReader;

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
        CollectorContext collectorContext = new CollectorContext(readerId);
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

    public StreamBucket collect(IntContainer docIds) {
        if (docIds.size() >= 10) {
            // If the doc ids are in sequential order we can leverage
            // the merge instances of the stored fields readers that
            // are optimized for sequential access.
            int[] ids = docIds.toArray();
            Arrays.sort(ids);
            if (isSequential(ids)) {
                return collectSequential(ids);
            }
        }
        StreamBucket.Builder builder = new StreamBucket.Builder(streamers, ramAccounting);
        try (var borrowed = fetchTask.searcher(readerId)) {
            var searcher = borrowed.item();
            List<LeafReaderContext> leaves = searcher.getTopReaderContext().leaves();
            for (IntCursor cursor : docIds) {
                int docId = cursor.value;
                int readerIndex = readerIndex(docId, leaves);
                LeafReaderContext subReaderContext = leaves.get(readerIndex);
                try {
                    setNextDocId(new ReaderContext(subReaderContext), docId - subReaderContext.docBase);
                } catch (IOException e) {
                    Exceptions.rethrowRuntimeException(e);
                }
                builder.add(row);
            }
        }
        return builder.build();
    }

    private StreamBucket collectSequential(int[] docIds) {
        StreamBucket.Builder builder = new StreamBucket.Builder(streamers, ramAccounting);
        try (var borrowed = fetchTask.searcher(readerId)) {
            var searcher = borrowed.item();
            List<LeafReaderContext> leaves = searcher.getTopReaderContext().leaves();
            var storedFieldsReaders = new IntObjectHashMap<StoredFieldsReader>(leaves.size());
            for (int docId : docIds) {
                int readerIndex = readerIndex(docId, leaves);
                LeafReaderContext subReaderContext = leaves.get(readerIndex);
                try {
                    var storedFieldReader = storedFieldsReaders.get(readerIndex);
                    if (storedFieldReader == null) {
                        storedFieldReader = sequentialStoredFieldReader(subReaderContext);
                        storedFieldsReaders.put(readerIndex, storedFieldReader);
                    }
                    setNextDocId(new ReaderContext(subReaderContext, storedFieldReader), docId - subReaderContext.docBase);
                } catch (IOException e) {
                    Exceptions.rethrowRuntimeException(e);
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

    static boolean isSequential(int[] docIds) {
        // checks if doc ids are in sequential order using the following conditions:
        // (last element - first element) = (number of elements in between first and last)
        // [3,4,5,6,7] -> 7 - 3 == 4
        if (docIds.length == 0) {
            return false;
        }
        int last = docIds[docIds.length - 1];
        int first = docIds[0];
        return last - first == docIds.length - 1;
    }

    static StoredFieldsReader sequentialStoredFieldReader(LeafReaderContext context) {
        // If the document access is sequential, the field reader from the merge instance can be used
        // to provide a significant speed up. However, accessing the merge CompressingStoredFieldsReader is expensive
        // because the underlying inputData is cloned.
        if (context.reader() instanceof SequentialStoredFieldsLeafReader) {
            try {
                SequentialStoredFieldsLeafReader reader = (SequentialStoredFieldsLeafReader) context.reader();
                return reader.getSequentialStoredFieldsReader();
            } catch (IOException e) {
                throw Exceptions.toRuntimeException(e);
            }
        }
        throw new RuntimeException("Sequential StoredFieldReader not available");
    }
}
