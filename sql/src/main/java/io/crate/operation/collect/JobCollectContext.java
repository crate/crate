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
import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.action.sql.query.CrateSearchContext;
import io.crate.breaker.RamAccountingContext;
import io.crate.core.collections.Row;
import io.crate.jobs.*;
import io.crate.operation.RowDownstream;
import io.crate.operation.RowDownstreamHandle;
import io.crate.operation.RowUpstream;
import io.crate.planner.node.dql.CollectPhase;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Locale;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class JobCollectContext implements ExecutionSubContext, RowUpstream, ExecutionState {

    private final UUID id;
    private final CollectPhase collectNode;
    private final CollectOperation collectOperation;
    private final RamAccountingContext queryPhaseRamAccountingContext;
    private final RowDownstream downstream;

    private final IntObjectOpenHashMap<CrateSearchContext> searchContexts = new IntObjectOpenHashMap<>();
    private final Object subContextLock = new Object();

    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final SettableFuture<Void> closeFuture = SettableFuture.create();
    private final Collection<CrateCollector> collectors = new ArrayList<>();

    private ContextCallback contextCallback = ContextCallback.NO_OP;
    private volatile boolean isKilled = false;

    private static final ESLogger LOGGER = Loggers.getLogger(JobCollectContext.class);

    public JobCollectContext(UUID jobId,
                             final CollectPhase collectPhase,
                             CollectOperation collectOperation,
                             RamAccountingContext queryPhaseRamAccountingContext,
                             final RowDownstream rowDownstream) {
        id = jobId;
        this.collectNode = collectPhase;
        this.collectOperation = collectOperation;
        this.queryPhaseRamAccountingContext = queryPhaseRamAccountingContext;
        this.downstream = new RowDownstream() {

            final AtomicInteger numUpstreams = new AtomicInteger();

            @Override
            public RowDownstreamHandle registerUpstream(RowUpstream upstream) {
                numUpstreams.incrementAndGet();
                final RowDownstreamHandle rowDownstreamHandle = rowDownstream.registerUpstream(upstream);
                return new RowDownstreamHandle() {

                    @Override
                    public boolean setNextRow(Row row) {
                        return rowDownstreamHandle.setNextRow(row);
                    }

                    @Override
                    public void finish() {
                        rowDownstreamHandle.finish();
                        if (numUpstreams.decrementAndGet() == 0) {
                            if (!collectPhase.keepContextForFetcher()) {
                                close();
                            }
                        }
                    }

                    @Override
                    public void fail(Throwable throwable) {
                        rowDownstreamHandle.fail(throwable);
                        closeDueToFailure(throwable);
                    }
                };
            }
        };
    }

    @Override
    public void addCallback(ContextCallback contextCallback) {
        assert !closed.get() : "may not add a callback on a closed context";
        this.contextCallback = MultiContextCallback.merge(this.contextCallback, contextCallback);
    }

    public void addContext(int jobSearchContextId, CrateSearchContext searchContext) {
        if (closed.get()) {
            // if this is closed and addContext is called this means the context got killed.
            searchContext.close();
            return;
        }

        synchronized (subContextLock) {
            CrateSearchContext replacedContext = searchContexts.put(jobSearchContextId, searchContext);
            if (replacedContext != null) {
                replacedContext.close();
                searchContext.close();
                throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                        "ShardCollectContext for %d already added", jobSearchContextId));
            }
        }
    }

    @Nullable
    public CrateSearchContext getContext(int jobSearchContextId) {
        synchronized (subContextLock) {
            return searchContexts.get(jobSearchContextId);
        }
    }

    public void closeDueToFailure(Throwable throwable) {
        close(throwable);
    }

    @Override
    public void close() {
        close(null);
    }

    private void close(@Nullable Throwable throwable) {
        if (closed.compareAndSet(false, true)) { // prevent double release
            LOGGER.trace("closing JobCollectContext: {}", id);
            synchronized (subContextLock) {
                for (ObjectCursor<CrateSearchContext> cursor : searchContexts.values()) {
                    cursor.value.close();
                }
                searchContexts.clear();
            }
            long bytesUsed = queryPhaseRamAccountingContext.totalBytes();
            contextCallback.onClose(throwable, bytesUsed);
            queryPhaseRamAccountingContext.close();
            closeFuture.set(null);
        } else {
            LOGGER.trace("close called on an already closed JobCollectContext: {}", id);
            try {
                closeFuture.get();
            } catch (Throwable e) {
                LOGGER.warn("Error while waiting for already running close {}", e);
            }
        }
    }

    @Override
    public void kill() {
        isKilled = true;
        close(new CancellationException());
        for (CrateCollector collector : collectors) {
            collector.kill();
        }
    }

    @Override
    public String name() {
        return collectNode.name();
    }


    @Override
    public String toString() {
        return "JobCollectContext{" +
                "searchContexts=" + searchContexts +
                ", closed=" + closed +
                ", id=" + id +
                '}';
    }

    @Override
    public void start() {
        try {
            collectors.addAll(collectOperation.collect(collectNode, downstream, this));
        } catch (Throwable t) {
            RowDownstreamHandle rowDownstreamHandle = downstream.registerUpstream(this);
            rowDownstreamHandle.fail(t);
        }
    }

    @Override
    public boolean isKilled() {
        return isKilled;
    }

    public RamAccountingContext queryPhaseRamAccountingContext() {
        return queryPhaseRamAccountingContext;
    }

    @Override
    public void pause() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void resume(boolean async) {
        throw new UnsupportedOperationException();
    }

    public KeepAliveListener keepAliveListener() {
        return contextCallback;
    }

    @Override
    public void repeat() {
        throw new UnsupportedOperationException();
    }
}
