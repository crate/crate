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
import io.crate.breaker.RamAccountingContext;
import io.crate.exceptions.SQLExceptions;
import io.crate.execution.engine.collect.stats.JobsLogs;
import io.crate.execution.engine.distribution.StreamBucket;
import io.crate.execution.jobs.RootTask;
import io.crate.execution.jobs.TasksService;
import io.crate.execution.support.ThreadPools;
import io.crate.expression.reference.doc.lucene.LuceneCollectorExpression;
import io.crate.expression.reference.doc.lucene.LuceneReferenceResolver;
import io.crate.expression.symbol.Symbols;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.index.IndexService;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

public class NodeFetchOperation {

    private final ThreadPoolExecutor executor;
    private final int numProcessors;
    private final JobsLogs jobsLogs;
    private final TasksService tasksService;
    private final CircuitBreaker circuitBreaker;

    private static class TableFetchInfo {

        private final Streamer<?>[] streamers;
        private final Collection<Reference> refs;
        private final FetchTask fetchTask;

        TableFetchInfo(Collection<Reference> refs, FetchTask fetchTask) {
            this.refs = refs;
            this.fetchTask = fetchTask;
            this.streamers = Symbols.streamerArray(refs);
        }

        FetchCollector createCollector(int readerId, RamAccountingContext ramAccountingContext) {
            IndexService indexService = fetchTask.indexService(readerId);
            LuceneReferenceResolver resolver = new LuceneReferenceResolver(indexService.mapperService()::fullName);
            ArrayList<LuceneCollectorExpression<?>> exprs = new ArrayList<>(refs.size());
            for (Reference reference : refs) {
                exprs.add(resolver.getImplementation(reference));
            }
            return new FetchCollector(
                exprs,
                streamers,
                fetchTask.searcher(readerId),
                indexService.fieldData(),
                ramAccountingContext,
                readerId
            );
        }
    }

    public NodeFetchOperation(ThreadPoolExecutor executor,
                              int numProcessors,
                              JobsLogs jobsLogs,
                              TasksService tasksService,
                              CircuitBreaker circuitBreaker) {
        this.executor = executor;
        this.numProcessors = numProcessors;
        this.jobsLogs = jobsLogs;
        this.tasksService = tasksService;
        this.circuitBreaker = circuitBreaker;
    }

    public CompletableFuture<IntObjectMap<StreamBucket>> fetch(UUID jobId,
                                                               int phaseId,
                                                               @Nullable IntObjectMap<? extends IntContainer> docIdsToFetch,
                                                               boolean closeTaskOnFinish) {
        CompletableFuture<IntObjectMap<StreamBucket>> resultFuture = new CompletableFuture<>();
        logStartAndSetupLogFinished(jobId, phaseId, resultFuture);

        if (docIdsToFetch == null) {
            if (closeTaskOnFinish) {
                tryCloseTask(jobId, phaseId);
            }
            resultFuture.complete(new IntObjectHashMap<>(0));
            return resultFuture;
        }

        RootTask context = tasksService.getTask(jobId);
        FetchTask fetchTask = context.getTask(phaseId);
        try {
            doFetch(fetchTask, resultFuture, docIdsToFetch);
        } catch (Throwable t) {
            resultFuture.completeExceptionally(t);
        }
        if (closeTaskOnFinish) {
            return resultFuture.whenComplete(new CloseTaskCallback(fetchTask));
        }
        return resultFuture;
    }

    private void logStartAndSetupLogFinished(final UUID jobId, final int phaseId, CompletableFuture<?> resultFuture) {
        jobsLogs.operationStarted(phaseId, jobId, "fetch", () -> -1);
        resultFuture.whenComplete((r, t) -> {
            if (t == null) {
                jobsLogs.operationFinished(phaseId, jobId, null);
            } else {
                jobsLogs.operationFinished(phaseId, jobId, SQLExceptions.messageOf(t));
            }
        });
    }

    private void tryCloseTask(UUID jobId, int phaseId) {
        RootTask rootTask = tasksService.getTaskOrNull(jobId);
        if (rootTask != null) {
            FetchTask fetchTask = rootTask.getTaskOrNull(phaseId);
            if (fetchTask != null) {
                fetchTask.close();
            }
        }
    }

    private HashMap<RelationName, TableFetchInfo> getTableFetchInfos(FetchTask fetchTask) {
        HashMap<RelationName, TableFetchInfo> result = new HashMap<>(fetchTask.toFetch().size());
        for (Map.Entry<RelationName, Collection<Reference>> entry : fetchTask.toFetch().entrySet()) {
            TableFetchInfo tableFetchInfo = new TableFetchInfo(entry.getValue(), fetchTask);
            result.put(entry.getKey(), tableFetchInfo);
        }
        return result;
    }

    private void doFetch(FetchTask fetchTask,
                         CompletableFuture<IntObjectMap<StreamBucket>> resultFuture,
                         IntObjectMap<? extends IntContainer> toFetch) throws Exception {

        final IntObjectHashMap<StreamBucket> fetched = new IntObjectHashMap<>(toFetch.size());
        HashMap<RelationName, TableFetchInfo> tableFetchInfos = getTableFetchInfos(fetchTask);
        final AtomicReference<Throwable> lastThrowable = new AtomicReference<>(null);
        final AtomicInteger threadLatch = new AtomicInteger(toFetch.size());

        // RamAccountingContext is per doFetch call instead of per FetchTask/fetchPhase
        // To be able to free up the memory count when the operation is complete
        RamAccountingContext ramAccountingContext = new RamAccountingContext("fetch-" + fetchTask.id(), circuitBreaker);
        resultFuture.whenComplete((r, f) -> ramAccountingContext.close());

        ArrayList<Supplier<Void>> collectors = new ArrayList<>();
        for (IntObjectCursor<? extends IntContainer> toFetchCursor : toFetch) {
            final int readerId = toFetchCursor.key;
            final IntContainer docIds = toFetchCursor.value;

            RelationName ident = fetchTask.tableIdent(readerId);
            final TableFetchInfo tfi = tableFetchInfos.get(ident);
            assert tfi != null : "tfi must not be null";

            CollectRunnable collectRunnable = new CollectRunnable(
                tfi.createCollector(readerId, ramAccountingContext),
                docIds,
                fetched,
                readerId,
                lastThrowable,
                threadLatch,
                resultFuture,
                fetchTask.isKilled()
            );
            collectors.add(() -> {
                collectRunnable.run();
                return null;
            });
        }

        // We're only forwarding the failure case to the resultFuture here (eg. thread pools rejecting the collectors)
        // When all collectors complete successfully, the last one will complete the resultFuture with the result.
        ThreadPools.runWithAvailableThreads(
            executor,
            ThreadPools.numIdleThreads(executor, numProcessors),
            collectors
        ).whenComplete((r, t) -> {
            if (t != null) {
                resultFuture.completeExceptionally(t);
            }
        });
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
