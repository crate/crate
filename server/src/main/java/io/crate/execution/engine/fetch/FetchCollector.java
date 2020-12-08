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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

import com.carrotsearch.hppc.IntContainer;

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
import org.apache.lucene.index.StoredFieldVisitor;
import org.elasticsearch.common.CheckedBiConsumer;
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

    private void setNextDocId(LeafReaderContext readerContext, int doc, CheckedBiConsumer<Integer, StoredFieldVisitor, IOException> fieldReader) throws IOException {
        for (LuceneCollectorExpression<?> e : collectorExpressions) {
            e.setNextReader(new ReaderContext(readerContext, fieldReader));
            e.setNextDocId(doc, true);
        }
    }

    public StreamBucket collect(IntContainer docIds) {
        StreamBucket.Builder builder = new StreamBucket.Builder(streamers, ramAccounting);
        int[] ids = docIds.toArray();
        Arrays.sort(ids);
        boolean isSequental = hasSequentialDocs(ids) && ids.length >= 10;
        HashMap<Integer, CheckedBiConsumer<Integer, StoredFieldVisitor, IOException>> fieldReaders = new HashMap<>();
        try (var borrowed = fetchTask.searcher(readerId)) {
            var searcher = borrowed.item();
            List<LeafReaderContext> leaves = searcher.getTopReaderContext().leaves();
            for (int docId : ids) {
                int readerIndex = ReaderUtil.subIndex(docId, leaves);
                LeafReaderContext subReaderContext = leaves.get(readerIndex);
                var fieldReader = fieldReaders.get(
                    readerIndex);
                if (fieldReader == null) {
                    if (isSequental) {
                        fieldReader = FieldReader.getSequentialFieldReader(subReaderContext);
                        fieldReaders.put(readerIndex, fieldReader);
                    }
                }
                if (readerIndex == -1) {
                    throw new IllegalStateException(
                        "jobId=" + fetchTask.jobId() + " docId " + docId + " doesn't fit to leaves of searcher " +
                        readerId + " fetchTask=" + fetchTask);
                }

                try {
                    setNextDocId(subReaderContext, docId - subReaderContext.docBase, fieldReader);
                } catch (IOException e) {
                    Exceptions.rethrowRuntimeException(e);
                }
                builder.add(row);
            }
        }
        return builder.build();
    }

    private static boolean hasSequentialDocs(int[] docIds) {
        // checks if doc ids are in sequential order
        // it is sequential if last element - first element = distance of elements in between
        // e.g. [1,2,3,4,5] -> 5 - 1 = 4
        if (docIds.length <= 0) {
            return false;
        }
        int last = docIds[docIds.length - 1];
        int first = docIds[0];
        return last - first == docIds.length - 1;
    }
}
