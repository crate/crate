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

package io.crate.operation.fetch;

import com.carrotsearch.hppc.IntContainer;
import com.carrotsearch.hppc.cursors.IntCursor;
import io.crate.Streamer;
import io.crate.executor.transport.StreamBucket;
import io.crate.operation.InputRow;
import io.crate.operation.collect.collectors.CollectorFieldsVisitor;
import io.crate.operation.reference.doc.lucene.CollectorContext;
import io.crate.operation.reference.doc.lucene.LuceneCollectorExpression;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.mapper.MapperService;

import java.io.IOException;
import java.util.List;

class FetchCollector {

    private final CollectorFieldsVisitor fieldsVisitor;
    private final boolean visitorEnabled;
    private final LuceneCollectorExpression[] collectorExpressions;
    private final InputRow row;
    private final Streamer<?>[] streamers;
    private final List<LeafReaderContext> readerContexts;

    FetchCollector(List<LuceneCollectorExpression<?>> collectorExpressions,
                   Streamer<?>[] streamers,
                   MapperService mapperService,
                   Engine.Searcher searcher,
                   IndexFieldDataService indexFieldDataService) {
        // use toArray to avoid iterator allocations in docIds loop
        this.collectorExpressions = collectorExpressions.toArray(new LuceneCollectorExpression[0]);
        this.streamers = streamers;
        this.readerContexts = searcher.searcher().getIndexReader().leaves();
        this.fieldsVisitor = new CollectorFieldsVisitor(this.collectorExpressions.length);
        Context context = new Context(mapperService, indexFieldDataService, fieldsVisitor);
        for (LuceneCollectorExpression<?> collectorExpression : this.collectorExpressions) {
            collectorExpression.startCollect(context);
        }
        visitorEnabled = fieldsVisitor.required();
        this.row = new InputRow(collectorExpressions);

    }

    private void setNextDocId(LeafReaderContext readerContext, int doc) throws IOException {
        if (visitorEnabled) {
            fieldsVisitor.reset();
            readerContext.reader().document(doc, fieldsVisitor);
        }
        for (LuceneCollectorExpression e : collectorExpressions) {
            e.setNextReader(readerContext);
            e.setNextDocId(doc);
        }
    }

    public StreamBucket collect(IntContainer docIds) throws IOException {
        StreamBucket.Builder builder = new StreamBucket.Builder(streamers);
        for (IntCursor cursor : docIds) {
            int docId = cursor.value;
            int readerIndex = ReaderUtil.subIndex(docId, readerContexts);
            LeafReaderContext subReaderContext = readerContexts.get(readerIndex);
            setNextDocId(subReaderContext, docId - subReaderContext.docBase);
            builder.add(row);
        }
        return builder.build();
    }

    public static final class Context extends CollectorContext {

        public Context(MapperService mapperService,
                       IndexFieldDataService fieldData,
                       CollectorFieldsVisitor visitor) {
            super(mapperService, fieldData, visitor, -1, (byte) 0);
        }

        @Override
        public int readerId() {
            throw new UnsupportedOperationException("readerId() not supported");
        }

        @Override
        public byte relationId() {
            throw new UnsupportedOperationException("relationId() not supported");
        }
    }
}
