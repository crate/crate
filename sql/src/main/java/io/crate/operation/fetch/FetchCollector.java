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
import io.crate.executor.transport.StreamBucket;
import io.crate.operation.InputRow;
import io.crate.operation.collect.collectors.CollectorFieldsVisitor;
import io.crate.operation.reference.doc.lucene.CollectorContext;
import io.crate.operation.reference.doc.lucene.LuceneCollectorExpression;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.mapper.MapperService;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

public class FetchCollector {

    private final CollectorFieldsVisitor fieldsVisitor;
    private final boolean visitorEnabled;
    private final Collection<LuceneCollectorExpression<?>> collectorExpressions;
    private final InputRow row;
    private AtomicReader currentReader;
    private final List<AtomicReaderContext> readerContexts;

    public FetchCollector(List<LuceneCollectorExpression<?>> collectorExpressions,
                          MapperService mapperService,
                          Engine.Searcher searcher,
                          IndexFieldDataService indexFieldDataService,
                          int readerId) {
        this.collectorExpressions = collectorExpressions;
        this.readerContexts = searcher.searcher().getIndexReader().leaves();
        this.fieldsVisitor = new CollectorFieldsVisitor(this.collectorExpressions.size());
        CollectorContext collectorContext = new CollectorContext(mapperService, indexFieldDataService, fieldsVisitor, readerId);
        for (LuceneCollectorExpression<?> collectorExpression : this.collectorExpressions) {
            collectorExpression.startCollect(collectorContext);
        }
        visitorEnabled = fieldsVisitor.required();
        this.row = new InputRow(collectorExpressions);

    }

    public void setNextReader(AtomicReaderContext context) throws IOException {
        currentReader = context.reader();
        for (LuceneCollectorExpression expr : collectorExpressions) {
            expr.setNextReader(context);
        }
    }

    public void setNextDocId(int doc) throws IOException {
        if (visitorEnabled) {
            fieldsVisitor.reset();
            currentReader.document(doc, fieldsVisitor);
        }
        for (LuceneCollectorExpression e : collectorExpressions) {
            e.setNextDocId(doc);
        }
    }

    public void collect(IntContainer docIds, StreamBucket.Builder builder) throws IOException {
        for (IntCursor cursor : docIds) {
            final int docId = cursor.value;
            int readerIndex = ReaderUtil.subIndex(docId, readerContexts);
            AtomicReaderContext subReaderContext = readerContexts.get(readerIndex);
            setNextReader(subReaderContext);
            setNextDocId(docId - subReaderContext.docBase);
            builder.add(row);
        }
    }
}
