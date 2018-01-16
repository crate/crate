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

package io.crate.execution.engine.fetch;

import com.carrotsearch.hppc.IntContainer;
import com.carrotsearch.hppc.IntObjectHashMap;
import com.carrotsearch.hppc.IntObjectMap;
import com.carrotsearch.hppc.cursors.IntObjectCursor;
import io.crate.Streamer;
import io.crate.analyze.symbol.Symbols;
import io.crate.breaker.RamAccountingContext;
import io.crate.exceptions.SQLExceptions;
import io.crate.execution.engine.distribution.StreamBucket;
import io.crate.execution.jobs.JobContextService;
import io.crate.execution.jobs.JobExecutionContext;
import io.crate.metadata.Reference;
import io.crate.metadata.TableIdent;
import io.crate.execution.engine.collect.stats.JobsLogs;
import io.crate.execution.expression.reference.doc.lucene.LuceneCollectorExpression;
import io.crate.execution.expression.reference.doc.lucene.LuceneReferenceResolver;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.index.IndexService;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class NodeFetchOperation {

    private final Executor executor;
    private final JobsLogs jobsLogs;
    private final JobContextService jobContextService;
    private final CircuitBreaker circuitBreaker;

    private static class TableFetchInfo {

        private final Streamer<?>[] streamers;
        private final Collection<Reference> refs;
        private final FetchContext fetchContext;

        TableFetchInfo(Collection<Reference> refs, FetchContext fetchContext) {
            this.refs = refs;
            this.fetchContext = fetchContext;
            this.streamers = Symbols.streamerArray(refs);
        }

        FetchCollector createCollector(int readerId, RamAccountingContext ramAccountingContext) {
            IndexService indexService = fetchContext.indexService(readerId);
            LuceneReferenceResolver resolver = new LuceneReferenceResolver(
                indexService.mapperService()::fullName, indexService.getIndexSettings());
            ArrayList<LuceneCollectorExpression<?>> exprs = new ArrayList<>(refs.size());
            for (Reference reference : refs) {
                exprs.add(resolver.getImplementation(reference));
            }
            return new FetchCollector(
                exprs,
                streamers,
                fetchContext.searcher(readerId),
                indexService.fieldData(),
                ramAccountingContext,
                readerId
            );
        }
    }

    public NodeFetchOperation(Executor executor,
                              JobsLogs jobsLogs,
                              JobContextService jobContextService,
                              CircuitBreaker circuitBreaker) {
        this.executor = executor;
        this.jobsLogs = jobsLogs;
        this.jobContextService = jobContextService;
        this.circuitBreaker = circuitBreaker;
    }

    public CompletableFuture<IntObjectMap<StreamBucket>> fetch(UUID jobId,
                                                               int phaseId,
                                                               @Nullable IntObjectMap<? extends IntContainer> docIdsToFetch,
                                                               boolean closeContextOnFinish) {
        CompletableFuture<IntObjectMap<StreamBucket>> resultFuture = new CompletableFuture<>();
        logStartAndSetupLogFinished(jobId, phaseId, resultFuture);

        if (docIdsToFetch == null) {
            if (closeContextOnFinish) {
                tryCloseContext(jobId, phaseId);
            }
            resultFuture.complete(new IntObjectHashMap<>(0));
            return resultFuture;
        }

        JobExecutionContext context = jobContextService.getContext(jobId);
        FetchContext fetchContext = context.getSubContext(phaseId);
        try {
            doFetch(fetchContext, resultFuture, docIdsToFetch);
        } catch (Throwable t) {
            resultFuture.completeExceptionally(t);
        }
        if (closeContextOnFinish) {
            return resultFuture.whenComplete(new CloseContextCallback(fetchContext));
        }
        return resultFuture;
    }

    private void logStartAndSetupLogFinished(final UUID jobId, final int phaseId, CompletableFuture<?> resultFuture) {
        jobsLogs.operationStarted(phaseId, jobId, "fetch");
        resultFuture.whenComplete((r, t) -> {
            if (t == null) {
                jobsLogs.operationFinished(phaseId, jobId, null, 0);
            } else {
                jobsLogs.operationFinished(phaseId, jobId, SQLExceptions.messageOf(t), 0);
            }
        });
    }

    private void tryCloseContext(UUID jobId, int phaseId) {
        JobExecutionContext ctx = jobContextService.getContextOrNull(jobId);
        if (ctx != null) {
            FetchContext fetchContext = ctx.getSubContextOrNull(phaseId);
            if (fetchContext != null) {
                fetchContext.close();
            }
        }
    }

    private HashMap<TableIdent, TableFetchInfo> getTableFetchInfos(FetchContext fetchContext) {
        HashMap<TableIdent, TableFetchInfo> result = new HashMap<>(fetchContext.toFetch().size());
        for (Map.Entry<TableIdent, Collection<Reference>> entry : fetchContext.toFetch().entrySet()) {
            TableFetchInfo tableFetchInfo = new TableFetchInfo(entry.getValue(), fetchContext);
            result.put(entry.getKey(), tableFetchInfo);
        }
        return result;
    }

    private void doFetch(FetchContext fetchContext,
                         CompletableFuture<IntObjectMap<StreamBucket>> resultFuture,
                         IntObjectMap<? extends IntContainer> toFetch) throws Exception {

        final IntObjectHashMap<StreamBucket> fetched = new IntObjectHashMap<>(toFetch.size());
        HashMap<TableIdent, TableFetchInfo> tableFetchInfos = getTableFetchInfos(fetchContext);
        final AtomicReference<Throwable> lastThrowable = new AtomicReference<>(null);
        final AtomicInteger threadLatch = new AtomicInteger(toFetch.size());

        // RamAccountingContext is per doFetch call instead of per FetchContext/fetchPhase
        // To be able to free up the memory count when the operation is complete
        RamAccountingContext ramAccountingContext = new RamAccountingContext("fetch-" + fetchContext.id(), circuitBreaker);
        resultFuture.whenComplete((r, f) -> ramAccountingContext.close());

        for (IntObjectCursor<? extends IntContainer> toFetchCursor : toFetch) {
            final int readerId = toFetchCursor.key;
            final IntContainer docIds = toFetchCursor.value;

            TableIdent ident = fetchContext.tableIdent(readerId);
            final TableFetchInfo tfi = tableFetchInfos.get(ident);
            assert tfi != null : "tfi must not be null";

            CollectRunnable runnable = new CollectRunnable(
                tfi.createCollector(readerId, ramAccountingContext),
                docIds,
                fetched,
                readerId,
                lastThrowable,
                threadLatch,
                resultFuture,
                fetchContext.isKilled()
            );
            try {
                executor.execute(runnable);
            } catch (EsRejectedExecutionException | RejectedExecutionException e) {
                runnable.run();
            }
        }
    }

    private static class CollectRunnable implements Runnable {
        private final FetchCollector collector;
        private final IntContainer docIds;
        private final IntObjectHashMap<StreamBucket> fetched;
        private final int readerId;
        private final AtomicReference<Throwable> lastThrowable;
        private final AtomicInteger threadLatch;
        private final CompletableFuture<IntObjectMap<StreamBucket>> resultFuture;
        private final AtomicBoolean contextKilledRef;

        CollectRunnable(FetchCollector collector,
                        IntContainer docIds,
                        IntObjectHashMap<StreamBucket> fetched,
                        int readerId,
                        AtomicReference<Throwable> lastThrowable,
                        AtomicInteger threadLatch,
                        CompletableFuture<IntObjectMap<StreamBucket>> resultFuture,
                        AtomicBoolean contextKilledRef) {
            this.collector = collector;
            this.docIds = docIds;
            this.fetched = fetched;
            this.readerId = readerId;
            this.lastThrowable = lastThrowable;
            this.threadLatch = threadLatch;
            this.resultFuture = resultFuture;
            this.contextKilledRef = contextKilledRef;
        }

        @Override
        public void run() {
            try {
                StreamBucket bucket = collector.collect(docIds);
                synchronized (fetched) {
                    fetched.put(readerId, bucket);
                }
            } catch (Exception e) {
                lastThrowable.set(e);
            } finally {
                if (threadLatch.decrementAndGet() == 0) {
                    Throwable throwable = lastThrowable.get();
                    if (throwable == null) {
                        resultFuture.complete(fetched);
                    } else {
                        /* If the context gets killed the operation might fail due to the release of the underlying searchers.
                         * Only a InterruptedException is sent to the fetch-client.
                         * Otherwise the original exception which caused the kill could be overwritten by some
                         * side-effect-exception that happened because of the kill.
                         */
                        if (contextKilledRef.get()) {
                            resultFuture.completeExceptionally(new InterruptedException());
                        } else {
                            resultFuture.completeExceptionally(throwable);
                        }
                    }
                }
            }
        }
    }

}
