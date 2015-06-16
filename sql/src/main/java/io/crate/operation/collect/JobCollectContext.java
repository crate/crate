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

package io.crate.operation.collect;

import com.carrotsearch.hppc.IntObjectOpenHashMap;
import com.carrotsearch.hppc.cursors.IntObjectCursor;
import io.crate.breaker.RamAccountingContext;
import io.crate.jobs.ContextCallback;
import io.crate.jobs.ExecutionState;
import io.crate.jobs.ExecutionSubContext;
import io.crate.operation.RowDownstream;
import io.crate.operation.RowDownstreamHandle;
import io.crate.operation.RowUpstream;
import io.crate.planner.node.dql.CollectNode;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Locale;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class JobCollectContext implements ExecutionSubContext, RowUpstream, ExecutionState {

    private final UUID id;
    private final CollectNode collectNode;
    private final CollectOperation collectOperation;
    private final RamAccountingContext ramAccountingContext;
    private final RowDownstream downstream;

    private final IntObjectOpenHashMap<JobQueryShardContext> queryContexts = new IntObjectOpenHashMap<>();
    private final IntObjectOpenHashMap<JobFetchShardContext> fetchContexts = new IntObjectOpenHashMap<>();
    private final ConcurrentMap<ShardId, EngineSearcherDelegate> shardsSearcherMap = new ConcurrentHashMap<>();
    private final AtomicInteger activeQueryContexts = new AtomicInteger(0);
    private final AtomicInteger activeFetchContexts = new AtomicInteger(0);
    private final Object subContextLock = new Object();

    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final ArrayList<ContextCallback> contextCallbacks = new ArrayList<>(1);

    private volatile boolean isKilled = false;
    private long usedBytesOfQueryPhase = 0L;

    private static final ESLogger LOGGER = Loggers.getLogger(JobCollectContext.class);

    public JobCollectContext(UUID jobId,
                             CollectNode collectNode,
                             CollectOperation collectOperation,
                             RamAccountingContext ramAccountingContext,
                             RowDownstream downstream) {
        id = jobId;
        this.collectNode = collectNode;
        this.collectOperation = collectOperation;
        this.ramAccountingContext = ramAccountingContext;
        this.downstream = downstream;
    }

    @Override
    public void addCallback(ContextCallback contextCallback) {
        assert !closed.get() : "may not add a callback on a closed context";
        contextCallbacks.add(contextCallback);
    }

    public void addContext(int jobSearchContextId, JobQueryShardContext shardQueryContext) {
        interruptIfKilled();
        if (closed.get()) {
            throw new IllegalStateException("context already closed");
        }
        synchronized (subContextLock) {
            if (queryContexts.put(jobSearchContextId, shardQueryContext) != null) {
                throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                        "ExecutionSubContext for %d already added", jobSearchContextId));
            }
        }
        int numActive = activeQueryContexts.incrementAndGet();
        LOGGER.trace("adding query subContext {}, now there are {} query subContexts", jobSearchContextId, numActive);

        shardQueryContext.addCallback(new RemoveQueryContextCallback(jobSearchContextId));
        EngineSearcherDelegate searcherDelegate;
        try {
            searcherDelegate = acquireSearcher(shardQueryContext.indexShard());
            shardQueryContext.searcher(searcherDelegate);
        } catch (Exception e) {
            // all resources (specially engine searcher) must be closed
            shardQueryContext.close();
            throw e;
        }

        if (collectNode.keepContextForFetcher()) {
            JobFetchShardContext shardFetchContext = new JobFetchShardContext(
                    searcherDelegate,
                    shardQueryContext.searchContext());
            synchronized (subContextLock) {
                fetchContexts.put(jobSearchContextId, shardFetchContext);
            }
            shardFetchContext.addCallback(new RemoveFetchContextCallback(jobSearchContextId));

            int numActiveFetch = activeFetchContexts.incrementAndGet();
            LOGGER.trace("adding fetch subContext {}, now there are {} fetch subContexts", jobSearchContextId, numActiveFetch);
        }
    }


    @Nullable
    public JobFetchShardContext getFetchContext(int jobSearchContextId) {
        synchronized (subContextLock) {
            return fetchContexts.get(jobSearchContextId);
        }
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) { // prevent double release
            synchronized (subContextLock) {
                if (queryContexts.size() != 0 || fetchContexts.size() != 0) {
                    LOGGER.trace("closing query subContexts {}", id);
                    Iterator<IntObjectCursor<JobQueryShardContext>> queryIterator = queryContexts.iterator();
                    while (queryIterator.hasNext()) {
                        queryIterator.next().value.close();
                    }
                    LOGGER.trace("closing fetch subContexts {}", id);
                    Iterator<IntObjectCursor<JobFetchShardContext>> fetchIterator = fetchContexts.iterator();
                    while (fetchIterator.hasNext()) {
                        fetchIterator.next().value.close();
                    }
                } else {
                    callContextCallback();
                }
            }
            ramAccountingContext.close();
        } else {
            LOGGER.trace("close called on an already closed JobCollectContext: {}", id);
        }
    }

    @Override
    public void kill() {
        isKilled = true;
        if (closed.compareAndSet(false, true)) { // prevent double release
            synchronized (subContextLock) {
                if (queryContexts.size() != 0 || fetchContexts.size() != 0) {
                    LOGGER.trace("killing query subContexts {}", id);
                    Iterator<IntObjectCursor<JobQueryShardContext>> queryIterator = queryContexts.iterator();
                    while (queryIterator.hasNext()) {
                        IntObjectCursor<JobQueryShardContext> cursor = queryIterator.next();
                        cursor.value.kill();
                    }
                    LOGGER.trace("killing fetch subContexts {}", id);
                    Iterator<IntObjectCursor<JobFetchShardContext>> fetchIterator = fetchContexts.iterator();
                    while (fetchIterator.hasNext()) {
                        fetchIterator.next().value.kill();
                    }
                } else {
                    callContextCallback();
                }
            }
            ramAccountingContext.close();
        } else {
            LOGGER.trace("killed called on an already closed JobCollectContext: {}", id);
        }
    }

    @Override
    public String name() {
        return collectNode.name();
    }

    @Override
    public void start() {
        startQueryPhase();
    }

    protected void startQueryPhase() {
        try {
            collectOperation.collect(collectNode, downstream, this);
        } catch (Throwable t) {
            RowDownstreamHandle rowDownstreamHandle = downstream.registerUpstream(this);
            rowDownstreamHandle.fail(t);
            close();
        }
    }

    @Override
    public boolean isKilled() {
        return isKilled;
    }

    public void interruptIfKilled() {
        if (isKilled) {
            throw new CancellationException();
        }
    }

    public RamAccountingContext ramAccountingContext() {
        return ramAccountingContext;
    }

    private void callContextCallback() {
        if (contextCallbacks.isEmpty()) {
            return;
        }
        if (activeQueryContexts.get() == 0 && activeFetchContexts.get() == 0) {
            for (ContextCallback contextCallback : contextCallbacks) {
                contextCallback.onClose(null, usedBytesOfQueryPhase);
            }
        }
    }

    /**
     * Try to find a {@link EngineSearcherDelegate} for the same shard.
     * If none is found, create new one with new acquired {@link Engine.Searcher}.
     */
    protected EngineSearcherDelegate acquireSearcher(IndexShard indexShard) {
        EngineSearcherDelegate engineSearcherDelegate;
        for (;;) {
            engineSearcherDelegate = shardsSearcherMap.get(indexShard.shardId());
            if (engineSearcherDelegate == null) {
                engineSearcherDelegate = new EngineSearcherDelegate(acquireNewSearcher(indexShard));
                if (shardsSearcherMap.putIfAbsent(indexShard.shardId(), engineSearcherDelegate) == null) {
                    return engineSearcherDelegate;
                }
            } else {
                return engineSearcherDelegate;
            }
        }
    }

    /**
     * Acquire a new searcher, wrapper method needed for simplified testing
     */
    protected Engine.Searcher acquireNewSearcher(IndexShard indexShard) {
        return EngineSearcher.getSearcherWithRetry(indexShard, "search", null);
    }


    class RemoveQueryContextCallback implements ContextCallback {

        private final int jobSearchContextId;

        public RemoveQueryContextCallback(int jobSearchContextId) {
            this.jobSearchContextId = jobSearchContextId;
        }

        @Override
        public void onClose(@Nullable Throwable error, long bytesUsed) {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("[{}] Closing query subContext {}",
                        System.identityHashCode(queryContexts), jobSearchContextId);
            }

            JobQueryShardContext remove;
            synchronized (subContextLock) {
                remove = queryContexts.remove(jobSearchContextId);
            }
            int remaining;
            if (remove == null) {
                LOGGER.trace("Closed query context {} which was already closed.", jobSearchContextId);
                remaining = activeQueryContexts.get();
            } else {
                remaining = activeQueryContexts.decrementAndGet();

                if (collectNode.keepContextForFetcher()
                        && (remove.collector() == null || !remove.collector().producedRows() || remove.collector().failed())) {
                    // close fetch context on error or if query produced no rows

                    JobFetchShardContext fetchShardContext;
                    synchronized (subContextLock) {
                        fetchShardContext = fetchContexts.get(jobSearchContextId);
                    }
                    if (fetchShardContext != null) {
                        fetchShardContext.close();
                    }
                }
            }


            if (remaining == 0) {
                usedBytesOfQueryPhase = ramAccountingContext.totalBytes();
                ramAccountingContext.close();
                callContextCallback();
            }
        }
    }

    class RemoveFetchContextCallback implements ContextCallback {

        private final int jobSearchContextId;

        public RemoveFetchContextCallback(int jobSearchContextId) {
            this.jobSearchContextId = jobSearchContextId;
        }

        @Override
        public void onClose(@Nullable Throwable error, long bytesUsed) {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("[{}] Closing fetch subContext {}",
                        System.identityHashCode(fetchContexts), jobSearchContextId);
            }

            JobFetchShardContext remove;
            synchronized (subContextLock) {
                remove = fetchContexts.remove(jobSearchContextId);
            }
            int remaining;
            if (remove == null) {
                LOGGER.trace("Closed fetch context {} which was already closed.", jobSearchContextId);
                remaining = activeFetchContexts.get();
            } else {
                remaining = activeFetchContexts.decrementAndGet();
            }
            if (remaining == 0) {
                callContextCallback();
            }
        }
    }

}
