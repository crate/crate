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

import io.crate.action.sql.query.CrateSearchContext;
import io.crate.breaker.CrateCircuitBreakerService;
import io.crate.breaker.RamAccountingContext;
import io.crate.operation.*;
import io.crate.operation.collect.JobCollectContext;
import io.crate.operation.collect.LuceneDocCollector;
import io.crate.operation.reference.doc.lucene.CollectorContext;
import io.crate.operation.reference.doc.lucene.LuceneCollectorExpression;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.ReaderUtil;

import java.io.IOException;
import java.util.List;

public class LuceneDocFetcher implements RowUpstream {

    private RamAccountingContext ramAccountingContext;

    private final InputRow inputRow;
    private final List<LuceneCollectorExpression<?>> collectorExpressions;
    private final RowDownstreamHandle downstream;
    private final NodeFetchOperation.ShardDocIdsBucket shardDocIdsBucket;
    private final JobCollectContext jobCollectContext;
    private final CrateSearchContext searchContext;
    private final int jobSearchContextId;
    private final boolean closeContext;
    private final LuceneDocCollector.CollectorFieldsVisitor fieldsVisitor;
    private boolean visitorEnabled = false;
    private AtomicReader currentReader;

    public LuceneDocFetcher(List<Input<?>> inputs,
                            List<LuceneCollectorExpression<?>> collectorExpressions,
                            RowDownstream downstream,
                            NodeFetchOperation.ShardDocIdsBucket shardDocIdsBucket,
                            JobCollectContext jobCollectContext,
                            CrateSearchContext searchContext,
                            int jobSearchContextId,
                            boolean closeContext) {
        inputRow = new InputRow(inputs);
        this.collectorExpressions = collectorExpressions;
        this.downstream = downstream.registerUpstream(this);
        this.shardDocIdsBucket = shardDocIdsBucket;
        this.jobCollectContext = jobCollectContext;
        this.searchContext = searchContext;
        this.jobSearchContextId = jobSearchContextId;
        this.closeContext = closeContext;
        this.fieldsVisitor = new LuceneDocCollector.CollectorFieldsVisitor(collectorExpressions.size());
    }

    public void setNextReader(AtomicReaderContext context) throws IOException {
        currentReader = context.reader();
        for (LuceneCollectorExpression expr : collectorExpressions) {
            expr.setNextReader(context);
        }
    }

    private void fetch(int position, int doc) throws Exception {
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
        if (!downstream.setNextRow(new PositionalRowDelegate(inputRow, position))) {
            // no more rows required, we can stop here
            throw new FetchAbortedException();
        }

    }

    public void doFetch(RamAccountingContext ramAccountingContext) {
        this.ramAccountingContext = ramAccountingContext;

        jobCollectContext.acquireContext(searchContext);

        CollectorContext collectorContext = new CollectorContext()
                .visitor(fieldsVisitor)
                .searchContext(searchContext)
                .searchLookup(searchContext.lookup(false));
        for (LuceneCollectorExpression<?> collectorExpression : collectorExpressions) {
            collectorExpression.startCollect(collectorContext);
        }
        visitorEnabled = fieldsVisitor.required();

        try {
            for (int index = 0; index < shardDocIdsBucket.size(); index++) {
                int docId = shardDocIdsBucket.docId(index);
                int readerIndex = ReaderUtil.subIndex(docId, searchContext.searcher().getIndexReader().leaves());
                AtomicReaderContext subReaderContext = searchContext.searcher().getIndexReader().leaves().get(readerIndex);
                int subDoc = docId - subReaderContext.docBase;
                setNextReader(subReaderContext);
                fetch(shardDocIdsBucket.position(index), subDoc);
            }
            downstream.finish();
        } catch (FetchAbortedException e) {
            // yeah, that's ok! :)
            downstream.finish();
        } catch (Exception e) {
            downstream.fail(e);
        } finally {
            jobCollectContext.releaseContext(searchContext);
            if (closeContext) {
                jobCollectContext.closeContext(jobSearchContextId);
            }
        }
    }
}
