/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.operation.fetch;

import io.crate.breaker.CrateCircuitBreakerService;
import io.crate.breaker.RamAccountingContext;
import io.crate.jobs.ExecutionState;
import io.crate.operation.Input;
import io.crate.operation.InputRow;
import io.crate.operation.RowDownstream;
import io.crate.operation.RowUpstream;
import io.crate.operation.collect.collectors.CollectorFieldsVisitor;
import io.crate.operation.projectors.RowReceiver;
import io.crate.operation.reference.doc.lucene.CollectorContext;
import io.crate.operation.reference.doc.lucene.LuceneCollectorExpression;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.mapper.MapperService;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CancellationException;

public class LuceneDocFetcher implements RowUpstream {

    private final MapperService mapperService;
    private final IndexFieldDataService fieldDataService;
    private final Engine.Searcher searcher;
    private final ExecutionState executionState;
    private RamAccountingContext ramAccountingContext;

    private final InputRow inputRow;
    private final List<LuceneCollectorExpression<?>> collectorExpressions;
    private final RowReceiver downstream;
    private final NodeFetchOperation.ShardDocIdsBucket shardDocIdsBucket;
    private final CollectorFieldsVisitor fieldsVisitor;
    private boolean visitorEnabled = false;
    private AtomicReader currentReader;

    public LuceneDocFetcher(List<Input<?>> inputs,
                            List<LuceneCollectorExpression<?>> collectorExpressions,
                            RowDownstream downstream,
                            NodeFetchOperation.ShardDocIdsBucket shardDocIdsBucket,
                            MapperService mapperService,
                            IndexFieldDataService fieldDataService,
                            Engine.Searcher searcher,
                            ExecutionState executionState) {
        this.mapperService = mapperService;
        this.fieldDataService = fieldDataService;
        this.searcher = searcher;
        this.executionState = executionState;
        inputRow = new InputRow(inputs);
        this.collectorExpressions = collectorExpressions;
        this.downstream = downstream.newRowReceiver();
        this.downstream.setUpstream(this);
        this.shardDocIdsBucket = shardDocIdsBucket;
        this.fieldsVisitor = new CollectorFieldsVisitor(collectorExpressions.size());
    }

    public void setNextReader(AtomicReaderContext context) throws IOException {
        currentReader = context.reader();
        for (LuceneCollectorExpression expr : collectorExpressions) {
            expr.setNextReader(context);
        }
    }

    private boolean fetch(int position, int doc) throws Exception {
        if (ramAccountingContext != null && ramAccountingContext.trippedBreaker()) {
            // stop fetching because breaker limit was reached
            throw new UnexpectedFetchTerminatedException(
                    CrateCircuitBreakerService.breakingExceptionMessage(ramAccountingContext.contextId(),
                            ramAccountingContext.limit()));
        }
        if (visitorEnabled) {
            fieldsVisitor.reset();
            currentReader.document(doc, fieldsVisitor);
        }
        for (LuceneCollectorExpression e : collectorExpressions) {
            e.setNextDocId(doc);
        }
        return downstream.setNextRow(new PositionalRowDelegate(inputRow, position));
    }

    public void doFetch(RamAccountingContext ramAccountingContext) {
        this.ramAccountingContext = ramAccountingContext;
        CollectorContext collectorContext = new CollectorContext(
                mapperService,
                fieldDataService,
                fieldsVisitor
        );
        for (LuceneCollectorExpression<?> collectorExpression : collectorExpressions) {
            collectorExpression.startCollect(collectorContext);
        }
        visitorEnabled = fieldsVisitor.required();

        try {
            for (int index = 0; index < shardDocIdsBucket.size(); index++) {
                if (executionState.isKilled()) {
                    throw new CancellationException();
                }

                int docId = shardDocIdsBucket.docId(index);
                int readerIndex = ReaderUtil.subIndex(docId, searcher.searcher().getIndexReader().leaves());
                AtomicReaderContext subReaderContext = searcher.searcher().getIndexReader().leaves().get(readerIndex);
                int subDoc = docId - subReaderContext.docBase;
                setNextReader(subReaderContext);
                boolean needMoreRows = fetch(shardDocIdsBucket.position(index), subDoc);
                if (!needMoreRows) {
                    break;
                }
            }
            downstream.finish();
        } catch (Exception e) {
            downstream.fail(e);
        }
    }

    @Override
    public void pause() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void resume(boolean async) {
        throw new UnsupportedOperationException();
    }
}
