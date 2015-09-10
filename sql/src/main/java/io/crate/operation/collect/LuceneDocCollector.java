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
import io.crate.jobs.KeepAliveListener;
import io.crate.operation.Input;
import io.crate.operation.InputRow;
import io.crate.operation.RowUpstream;
import io.crate.operation.projectors.RowReceiver;
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
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.index.fieldvisitor.FieldsVisitor;
import org.elasticsearch.index.mapper.internal.SourceFieldMapper;
import org.elasticsearch.search.internal.ContextIndexSearcher;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * collect documents from ES shard, a lucene index
 */
public class LuceneDocCollector extends Collector implements CrateCollector, RowUpstream {

    private static final ESLogger LOGGER = Loggers.getLogger(LuceneDocCollector.class);
    private static final int KEEP_ALIVE_AFTER_ROWS = 1_000_000;

     // cache exception to avoid expensive stacktrace generation
    @SuppressWarnings("ThrowableInstanceNeverThrown")
    private static final CollectionTerminatedException COLLECTION_TERMINATED_EXCEPTION = new CollectionTerminatedException();
    private final KeepAliveListener keepAliveListener;

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

    protected final ThreadPoolExecutor executor;
    private final RowReceiver downstream;
    private final CollectorFieldsVisitor fieldsVisitor;
    private final InputRow inputRow;
    private final List<LuceneCollectorExpression<?>> collectorExpressions;
    protected final Integer limit;
    protected final CrateSearchContext searchContext;
    private final RamAccountingContext ramAccountingContext;
    protected final AtomicBoolean paused = new AtomicBoolean(false);
    private volatile boolean finished = false;

    protected volatile boolean killed = false;
    private boolean visitorEnabled = false;
    private AtomicReader currentReader;
    protected int rowCount = 0;
    private volatile boolean pendingPause = false;
    private InternalCollectContext internalCollectContext;
    private int currentDocBase = 0;

    public LuceneDocCollector(ThreadPool threadPool,
                              CrateSearchContext searchContext,
                              KeepAliveListener keepAliveListener,
                              List<Input<?>> inputs,
                              List<LuceneCollectorExpression<?>> collectorExpressions,
                              CollectPhase collectPhase,
                              RowReceiver downStreamProjector,
                              RamAccountingContext ramAccountingContext) throws Exception {
        this.keepAliveListener = keepAliveListener;
        this.executor = (ThreadPoolExecutor)threadPool.executor(ThreadPool.Names.SEARCH);
        this.searchContext = searchContext;
        this.ramAccountingContext = ramAccountingContext;
        this.limit = collectPhase.limit();
        this.downstream = downStreamProjector;
        downStreamProjector.setUpstream(this);
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
        doCollect(doc);
    }

    protected void doCollect(int doc) throws IOException {
        if (killed) {
            throw new CancellationException();
        }

        if (ramAccountingContext != null && ramAccountingContext.trippedBreaker()) {
            // stop collecting because breaker limit was reached
            throw new UnexpectedCollectionTerminatedException(
                    CrateCircuitBreakerService.breakingExceptionMessage(ramAccountingContext.contextId(),
                            ramAccountingContext.limit()));
        }

        if (rowCount % KEEP_ALIVE_AFTER_ROWS == 0) {
            // trigger keep-alive only every KEEP_ALIVE_AFTER_ROWS
            // as it uses too much volatile stuff to do it more often
            keepAliveListener.keepAlive();
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
            throw CollectionFinishedEarlyException.INSTANCE;
        }
    }

    protected boolean skipDoc(int doc) {
        ScoreDoc after = internalCollectContext.lastCollected;
        if (after != null) {
            return currentDocBase == internalCollectContext.lastDocBase && doc <= after.doc;
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

        // trigger keep-alive here as well
        // in case we have a long running query without actual matches
        keepAliveListener.keepAlive();

        for (LuceneCollectorExpression expr : collectorExpressions) {
            expr.setNextReader(context);
        }
    }

    protected void skipSegmentReader() {
        if (currentDocBase < internalCollectContext.lastDocBase) {
            // skip this segment reader, all docs of this segment are already collected
            throw COLLECTION_TERMINATED_EXCEPTION;
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

    protected boolean searchAndCollect() throws IOException {
        searchContext.searcher().search(internalCollectContext.query, this);
        return !shouldPause();
    }

    private void innerCollect() {
        try {
            if (searchAndCollect()) {
                finished = true;
                downstream.finish();
            } /// else = pause
        } catch (CollectionFinishedEarlyException e) {
            paused.set(false);
            finished = true;
            downstream.finish();
        } catch (CollectionPauseException e) {
            // paused - do nothing
        } catch (Throwable e) {
            paused.set(false);
            searchContext.close();
            finished = true;
            downstream.fail(e);
        } finally {
            finishCollect();
        }
    }

    private void finishCollect() {
        if (finished) {
            searchContext.searcher().finishStage(ContextIndexSearcher.Stage.MAIN_QUERY);
            searchContext.clearReleasables(SearchContext.Lifetime.PHASE);
        }
    }

    @Override
    public void kill(@Nullable Throwable throwable) {
        killed = true;
        if (paused.get()) {
            if (throwable == null) {
                throwable = new CancellationException();
            }
            downstream.fail(throwable);
            finishCollect();
        }
    }

    @Override
    public void pause() {
        pendingPause = true;
    }

    @Override
    public void resume(boolean async) {
        pendingPause = false;
        if (paused.compareAndSet(true, false)) {
            if (finished) {
                downstream.finish();
                return;
            }
            if (!async) {
                innerCollect();
            } else {
                try {
                    executor.execute(new Runnable() {
                        @Override
                        public void run() {
                            SearchContext.setCurrent(searchContext);
                            searchContext.searcher().inStage(ContextIndexSearcher.Stage.MAIN_QUERY);
                            innerCollect();
                        }
                    });
                } catch (EsRejectedExecutionException e) {
                    // fallback to synchronous
                    LOGGER.trace("Got rejected exception, will resume synchronous");
                    innerCollect();
                }
            }
        }
    }

    /**
     * tells the RowUpstream that it should push all rows again
     */
    @Override
    public void repeat() {
        throw new UnsupportedOperationException();
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
