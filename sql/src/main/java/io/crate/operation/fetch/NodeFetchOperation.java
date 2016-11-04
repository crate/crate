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

import com.carrotsearch.hppc.IntContainer;
import com.carrotsearch.hppc.IntObjectHashMap;
import com.carrotsearch.hppc.IntObjectMap;
import com.carrotsearch.hppc.cursors.IntObjectCursor;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.Streamer;
import io.crate.analyze.symbol.Symbols;
import io.crate.exceptions.SQLExceptions;
import io.crate.executor.transport.StreamBucket;
import io.crate.jobs.JobContextService;
import io.crate.jobs.JobExecutionContext;
import io.crate.metadata.Reference;
import io.crate.metadata.TableIdent;
import io.crate.operation.collect.stats.JobsLogs;
import io.crate.operation.reference.doc.lucene.LuceneCollectorExpression;
import io.crate.operation.reference.doc.lucene.LuceneReferenceResolver;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.threadpool.ThreadPool;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class NodeFetchOperation {

    private final Executor executor;
    private final JobsLogs jobsLogs;
    private final JobContextService jobContextService;

    private static class TableFetchInfo {

        private final Streamer<?>[] streamers;
        private final Collection<Reference> refs;
        private final FetchContext fetchContext;

        TableFetchInfo(Collection<Reference> refs, FetchContext fetchContext) {
            this.refs = refs;
            this.fetchContext = fetchContext;
            this.streamers = Symbols.streamerArray(refs);
        }

        FetchCollector createCollector(int readerId) {
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
                readerId
            );
        }
    }

    public NodeFetchOperation(ThreadPool threadPool, JobsLogs jobsLogs, JobContextService jobContextService) {
        executor = threadPool.executor(ThreadPool.Names.SEARCH);
        this.jobsLogs = jobsLogs;
        this.jobContextService = jobContextService;
    }

    public ListenableFuture<IntObjectMap<StreamBucket>> fetch(UUID jobId,
                                                              int phaseId,
                                                              @Nullable IntObjectMap<? extends IntContainer> docIdsToFetch,
                                                              boolean closeContextOnFinish) {
        SettableFuture<IntObjectMap<StreamBucket>> resultFuture = SettableFuture.create();
        logStartAndSetupLogFinished(jobId, phaseId, resultFuture);

        if (docIdsToFetch == null) {
            if (closeContextOnFinish) {
                tryCloseContext(jobId, phaseId);
            }
            return Futures.immediateFuture(new IntObjectHashMap<>(0));
        }

        JobExecutionContext context = jobContextService.getContext(jobId);
        FetchContext fetchContext = context.getSubContext(phaseId);
        if (closeContextOnFinish) {
            Futures.addCallback(resultFuture, new CloseContextCallback(fetchContext));
        }
        try {
            doFetch(fetchContext, resultFuture, docIdsToFetch);
        } catch (Throwable t) {
            resultFuture.setException(t);
        }
        return resultFuture;
    }

    private void logStartAndSetupLogFinished(final UUID jobId, final int phaseId, ListenableFuture<?> resultFuture) {
        jobsLogs.operationStarted(phaseId, jobId, "fetch");
        Futures.addCallback(resultFuture, new FutureCallback<Object>() {
        @Override
            public void onSuccess(@Nullable Object result) {
                jobsLogs.operationFinished(phaseId, jobId, null, 0);
            }

            @Override
            public void onFailure(@Nonnull Throwable t) {
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
                         SettableFuture<IntObjectMap<StreamBucket>> resultFuture,
                         IntObjectMap<? extends IntContainer> toFetch) throws Exception {

        final IntObjectHashMap<StreamBucket> fetched = new IntObjectHashMap<>(toFetch.size());
        HashMap<TableIdent, TableFetchInfo> tableFetchInfos = getTableFetchInfos(fetchContext);
        final AtomicReference<Throwable> lastThrowable = new AtomicReference<>(null);
        final AtomicInteger threadLatch = new AtomicInteger(toFetch.size());

        for (IntObjectCursor<? extends IntContainer> toFetchCursor : toFetch) {
            final int readerId = toFetchCursor.key;
            final IntContainer docIds = toFetchCursor.value;

            TableIdent ident = fetchContext.tableIdent(readerId);
            final TableFetchInfo tfi = tableFetchInfos.get(ident);
            assert tfi != null : "tfi must not be null";

            CollectRunnable runnable = new CollectRunnable(
                tfi.createCollector(readerId),
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
        private final SettableFuture<IntObjectMap<StreamBucket>> resultFuture;
        private final AtomicBoolean contextKilledRef;

        CollectRunnable(FetchCollector collector,
                        IntContainer docIds,
                        IntObjectHashMap<StreamBucket> fetched,
                        int readerId,
                        AtomicReference<Throwable> lastThrowable,
                        AtomicInteger threadLatch,
                        SettableFuture<IntObjectMap<StreamBucket>> resultFuture,
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
                        resultFuture.set(fetched);
                    } else {
                        /* If the context gets killed the operation might fail due to the release of the underlying searchers.
                         * Only a InterruptedException is sent to the fetch-client.
                         * Otherwise the original exception which caused the kill could be overwritten by some
                         * side-effect-exception that happened because of the kill.
                         */
                        if (contextKilledRef.get()) {
                            resultFuture.setException(new InterruptedException());
                        } else {
                            resultFuture.setException(throwable);
                        }
                    }
                }
            }
        }
    }

}
