/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.execution.engine.fetch;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.index.IndexService;
import org.jetbrains.annotations.Nullable;

import com.carrotsearch.hppc.IntArrayList;
import com.carrotsearch.hppc.IntObjectHashMap;
import com.carrotsearch.hppc.IntObjectMap;
import com.carrotsearch.hppc.cursors.IntObjectCursor;

import io.crate.Streamer;
import io.crate.breaker.ConcurrentRamAccounting;
import io.crate.data.breaker.BlockBasedRamAccounting;
import io.crate.data.breaker.RamAccounting;
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

        FetchCollector createCollector(int readerId, RamAccounting ramAccounting) {
            IndexService indexService = fetchTask.indexService(readerId);
            var mapperService = indexService.mapperService();
            LuceneReferenceResolver resolver = new LuceneReferenceResolver(
                indexService.index().getName(),
                mapperService::fieldType,
                fetchTask.table(readerId).partitionedByColumns()
            );
            ArrayList<LuceneCollectorExpression<?>> exprs = new ArrayList<>(refs.size());
            for (Reference reference : refs) {
                exprs.add(resolver.getImplementation(reference));
            }
            return new FetchCollector(
                exprs,
                streamers,
                fetchTask,
                ramAccounting,
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

    public CompletableFuture<? extends IntObjectMap<StreamBucket>> fetch(UUID jobId,
                                                                         int phaseId,
                                                                         @Nullable IntObjectMap<IntArrayList> docIdsToFetch,
                                                                         boolean closeTaskOnFinish) {
        if (docIdsToFetch == null) {
            if (closeTaskOnFinish) {
                tryCloseTask(jobId, phaseId);
            }
            jobsLogs.operationStarted(phaseId, jobId, "fetch", () -> -1);
            jobsLogs.operationFinished(phaseId, jobId, null);
            return CompletableFuture.completedFuture(new IntObjectHashMap<>(0));
        }

        RootTask context = tasksService.getTask(jobId);
        FetchTask fetchTask = context.getTask(phaseId);
        jobsLogs.operationStarted(phaseId, jobId, "fetch", () -> -1);
        BiConsumer<? super IntObjectMap<StreamBucket>, ? super Throwable> whenComplete = (res, err) -> {
            if (closeTaskOnFinish) {
                if (err == null) {
                    fetchTask.close();
                } else {
                    fetchTask.kill(err);
                }
            }
            if (err == null) {
                jobsLogs.operationFinished(phaseId, jobId, null);
            } else {
                jobsLogs.operationFinished(phaseId, jobId, SQLExceptions.messageOf(err));
            }
        };
        try {
            return doFetch(fetchTask, docIdsToFetch).whenComplete(whenComplete);
        } catch (Throwable t) {
            whenComplete.accept(null, t);
            return CompletableFuture.failedFuture(t);
        }
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

    private static HashMap<RelationName, TableFetchInfo> getTableFetchInfos(FetchTask fetchTask) {
        HashMap<RelationName, TableFetchInfo> result = new HashMap<>(fetchTask.toFetch().size());
        for (Map.Entry<RelationName, Collection<Reference>> entry : fetchTask.toFetch().entrySet()) {
            TableFetchInfo tableFetchInfo = new TableFetchInfo(entry.getValue(), fetchTask);
            result.put(entry.getKey(), tableFetchInfo);
        }
        return result;
    }

    private CompletableFuture<? extends IntObjectMap<StreamBucket>> doFetch(FetchTask fetchTask, IntObjectMap<IntArrayList> toFetch) throws Exception {
        HashMap<RelationName, TableFetchInfo> tableFetchInfos = getTableFetchInfos(fetchTask);

        // RamAccounting is per doFetch call instead of per FetchTask/fetchPhase
        // To be able to free up the memory count when the operation is complete
        final var ramAccounting = ConcurrentRamAccounting.forCircuitBreaker(
            "fetch-" + fetchTask.id(),
            circuitBreaker,
            fetchTask.memoryLimitInBytes()
        );
        ArrayList<Supplier<StreamBucket>> collectors = new ArrayList<>(toFetch.size());
        for (IntObjectCursor<IntArrayList> toFetchCursor : toFetch) {
            final int readerId = toFetchCursor.key;
            final IntArrayList docIds = toFetchCursor.value;

            RelationName ident = fetchTask.tableIdent(readerId);
            final TableFetchInfo tfi = tableFetchInfos.get(ident);
            assert tfi != null : "tfi must not be null";

            var collector = tfi.createCollector(
                readerId,
                new BlockBasedRamAccounting(
                    ramAccounting::addBytes,
                    BlockBasedRamAccounting.MAX_BLOCK_SIZE_IN_BYTES
                )
            );
            collectors.add(() -> collector.collect(docIds));
        }
        return ThreadPools.runWithAvailableThreads(
            executor,
            ThreadPools.numIdleThreads(executor, numProcessors),
            collectors
        ).thenApply(buckets -> {
            var toFetchIt = toFetch.iterator();
            assert toFetch.size() == buckets.size()
                : "Must have a bucket per reader and they must be in the same order";
            IntObjectHashMap<StreamBucket> bucketByReader = new IntObjectHashMap<>(toFetch.size());
            for (var bucket : buckets) {
                assert toFetchIt.hasNext() : "toFetchIt must have an element if there is one in buckets";
                int readerId = toFetchIt.next().key;
                bucketByReader.put(readerId, bucket);
            }
            return bucketByReader;
        }).whenComplete((result, err) -> ramAccounting.close());
    }
}
