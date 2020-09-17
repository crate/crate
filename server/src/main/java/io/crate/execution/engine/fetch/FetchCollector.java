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
import java.util.List;

import com.carrotsearch.hppc.IntContainer;
import com.carrotsearch.hppc.cursors.IntCursor;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;

import io.crate.Streamer;
import io.crate.breaker.RamAccounting;
import io.crate.exceptions.Exceptions;
import io.crate.execution.engine.distribution.StreamBucket;
import io.crate.expression.InputRow;
import io.crate.expression.reference.doc.lucene.CollectorContext;
import io.crate.expression.reference.doc.lucene.LuceneCollectorExpression;

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

    private void setNextDocId(LeafReaderContext readerContext, int doc) throws IOException {
        for (LuceneCollectorExpression<?> e : collectorExpressions) {
            e.setNextReader(readerContext);
            e.setNextDocId(doc);
        }
    }

    public StreamBucket collect(IntContainer docIds) {
        StreamBucket.Builder builder = new StreamBucket.Builder(streamers, ramAccounting);
        try (var borrowed = fetchTask.searcher(readerId)) {
            var searcher = borrowed.item();
            List<LeafReaderContext> leaves = searcher.getTopReaderContext().leaves();
            for (IntCursor cursor : docIds) {
                int docId = cursor.value;
                int readerIndex = ReaderUtil.subIndex(docId, leaves);
                if (readerIndex == -1) {
                    throw new IllegalStateException("jobId=" + fetchTask.jobId() + " docId " + docId + " doesn't fit to leaves of searcher " + readerId + " fetchTask=" + fetchTask);
                }
                LeafReaderContext subReaderContext = leaves.get(readerIndex);
                try {
                    setNextDocId(subReaderContext, docId - subReaderContext.docBase);
                } catch (IOException e) {
                    Exceptions.rethrowRuntimeException(e);
                }
                builder.add(row);
            }
        }
        return builder.build();
    }
}
