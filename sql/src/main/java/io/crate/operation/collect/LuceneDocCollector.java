/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.operation.collect;

import io.crate.action.sql.query.CrateSearchContext;
import io.crate.breaker.CrateCircuitBreakerService;
import io.crate.breaker.RamAccountingContext;
import io.crate.operation.*;
import io.crate.operation.reference.doc.lucene.CollectorContext;
import io.crate.operation.reference.doc.lucene.LuceneCollectorExpression;
import io.crate.planner.node.dql.CollectPhase;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.search.*;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.index.fieldvisitor.FieldsVisitor;
import org.elasticsearch.index.mapper.internal.SourceFieldMapper;
import org.elasticsearch.search.internal.ContextIndexSearcher;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * collect documents from ES shard, a lucene index
 */
public class LuceneDocCollector extends Collector implements CrateCollector, RowUpstream {

    private static final ESLogger LOGGER = Loggers.getLogger(LuceneDocCollector.class);

    public static class CollectorFieldsVisitor extends FieldsVisitor {

        final HashSet<String> requiredFields;
        private boolean required = false;

        public CollectorFieldsVisitor(int size) {
            requiredFields = new HashSet<>(size);
        }

        public boolean addField(String name) {
            required = true;
            return requiredFields.add(name);
        }

        public boolean required() {
            return required;
        }

        @Override
        public Status needsField(FieldInfo fieldInfo) throws IOException {
            if (SourceFieldMapper.NAME.equals(fieldInfo.name)) {
                return Status.YES;
            }
            return requiredFields.contains(fieldInfo.name) ? Status.YES : Status.NO;
        }

        public void required(boolean required) {
            this.required = required;
        }
    }

    private final RowDownstreamHandle downstream;
    private final CollectorFieldsVisitor fieldsVisitor;
    private final InputRow inputRow;
    private final List<LuceneCollectorExpression<?>> collectorExpressions;
    protected final Integer limit;
    protected final CrateSearchContext searchContext;
    private final RamAccountingContext ramAccountingContext;
    private final AtomicBoolean paused = new AtomicBoolean(false);

    protected volatile boolean killed = false;
    private boolean visitorEnabled = false;
    private AtomicReader currentReader;
    protected int rowCount = 0;
    private volatile boolean pendingPause = false;
    private InternalCollectContext internalCollectContext;
    private int currentDocBase = 0;

    public LuceneDocCollector(CrateSearchContext searchContext,
                              List<Input<?>> inputs,
                              List<LuceneCollectorExpression<?>> collectorExpressions,
                              CollectPhase collectNode,
                              RowDownstream downStreamProjector,
                              RamAccountingContext ramAccountingContext) throws Exception {
        this.searchContext = searchContext;
        this.ramAccountingContext = ramAccountingContext;
        this.limit = collectNode.limit();
        this.downstream = downStreamProjector.registerUpstream(this);
        this.inputRow = new InputRow(inputs);
        this.collectorExpressions = collectorExpressions;
        this.fieldsVisitor = new CollectorFieldsVisitor(collectorExpressions.size());
    }

    @Override
    public void setScorer(Scorer scorer) throws IOException {
        for (LuceneCollectorExpression expr : collectorExpressions) {
            expr.setScorer(scorer);
        }
    }

    @Override
    public void collect(int doc) throws IOException {
        if (shouldPause()) {
            throw new CollectionPauseException();
        }

        if (killed) {
            throw new CancellationException();
        }
        if (ramAccountingContext != null && ramAccountingContext.trippedBreaker()) {
            // stop collecting because breaker limit was reached
            throw new UnexpectedCollectionTerminatedException(
                    CrateCircuitBreakerService.breakingExceptionMessage(ramAccountingContext.contextId(),
                            ramAccountingContext.limit()));
        }

        if (skipDoc(doc)) {
            return;
        }

        rowCount++;
        if (visitorEnabled) {
            fieldsVisitor.reset();
            currentReader.document(doc, fieldsVisitor);
        }
        for (LuceneCollectorExpression e : collectorExpressions) {
            e.setNextDocId(doc);
        }
        boolean wantMore = downstream.setNextRow(inputRow);

        postCollectDoc(doc);

        if (!wantMore || (limit != null && rowCount >= limit)) {
            // no more rows required, we can stop here
            throw new CollectionFinishedEarlyException();
        }
    }

    protected boolean skipDoc(int doc) {
        ScoreDoc after = internalCollectContext.lastCollected;
        if (after != null) {
            if (currentDocBase == internalCollectContext.lastDocBase && doc <= after.doc) {
                // doc was already collected, skip
                LOGGER.trace("skipping doc {} <= {} of docBase {}", doc, after.doc, currentDocBase);
                return true;
            }
        }
        return false;
    }

    protected void postCollectDoc(int doc) {
        if (internalCollectContext.lastCollected == null) {
            internalCollectContext.lastCollected = new ScoreDoc(doc, Float.NaN);
        }
        internalCollectContext.lastCollected.doc = doc;
        internalCollectContext.lastDocBase = currentDocBase;
    }


    @Override
    public void setNextReader(AtomicReaderContext context) throws IOException {
        currentReader = context.reader();
        currentDocBase = context.docBase;
        skipSegmentReader();
        for (LuceneCollectorExpression expr : collectorExpressions) {
            expr.setNextReader(context);
        }
    }

    protected void skipSegmentReader() {
        if (currentDocBase < internalCollectContext.lastDocBase) {
            // skip this segment reader, all docs of this segment are already collected
            LOGGER.trace("skipping segment reader with docBase {} < {}",
                    currentDocBase, internalCollectContext.lastDocBase);
            throw new CollectionTerminatedException();
        }
    }


    @Override
    public boolean acceptsDocsOutOfOrder() {
        return false;
    }

    @Override
    public void doCollect() {
        // start collect
        CollectorContext collectorContext = new CollectorContext(
                searchContext.mapperService(),
                searchContext.fieldData(),
                fieldsVisitor
        ).jobSearchContextId((int) searchContext.id());
        for (LuceneCollectorExpression<?> collectorExpression : collectorExpressions) {
            collectorExpression.startCollect(collectorContext);
        }
        visitorEnabled = fieldsVisitor.required();
        SearchContext.setCurrent(searchContext);
        searchContext.searcher().inStage(ContextIndexSearcher.Stage.MAIN_QUERY);

        Query query = searchContext.query();
        LOGGER.trace("query {}", query);
        assert query != null : "query must not be null";

        buildContext(query);

        innerCollect();
    }

    protected void buildContext(Query query) {
        if (internalCollectContext == null) {
            internalCollectContext = new InternalCollectContext(query);
        }
    }

    protected void searchAndCollect() throws IOException {
        searchContext.searcher().search(internalCollectContext.query, this);
    }

    private void innerCollect() {
        try {
            searchAndCollect();

            if (!paused.get()) {
                downstream.finish();
            }
        } catch (CollectionFinishedEarlyException e) {
            paused.set(false);
            downstream.finish();
        } catch (CollectionPauseException e) {
            // paused - do nothing
        } catch (Throwable e) {
            paused.set(false);
            searchContext.close();
            downstream.fail(e);
        } finally {
            finishCollect();
        }
    }

    private void finishCollect() {
        if (!paused.get()) {
            searchContext.searcher().finishStage(ContextIndexSearcher.Stage.MAIN_QUERY);
            assert SearchContext.current() == searchContext;
            searchContext.clearReleasables(SearchContext.Lifetime.PHASE);
            SearchContext.removeCurrent();
        }
    }

    @Override
    public void kill() {
        killed = true;
        if (paused.get()) {
            downstream.fail(new CancellationException());
            finishCollect();
        }
    }

    @Override
    public void pause() {
        pendingPause = true;
    }

    @Override
    public void resume(boolean async) {
        if (paused.compareAndSet(true, false)) {
            innerCollect();
        } else {
            LOGGER.trace("Collector was not paused and so will not resume");
            pendingPause = false;
        }
    }

    public boolean shouldPause() {
        if (pendingPause) {
            paused.set(true);
            pendingPause = false;
            return true;
        }
        return false;
    }

    protected static class InternalCollectContext {

        protected final Query query;
        protected int lastDocBase = 0;
        @Nullable protected ScoreDoc lastCollected;

        public InternalCollectContext(Query query) {
            this.query = query;
        }
    }
}
