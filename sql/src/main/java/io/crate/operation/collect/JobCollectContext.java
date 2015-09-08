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
import io.crate.jobs.*;
import io.crate.operation.projectors.ForwardingRowReceiver;
import io.crate.operation.projectors.RowReceiver;
import io.crate.planner.node.dql.CollectPhase;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Locale;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicBoolean;

public class JobCollectContext implements ExecutionSubContext, ExecutionState {

    private final UUID id;
    private final CollectPhase collectNode;
    private final MapSideDataCollectOperation collectOperation;
    private final RamAccountingContext queryPhaseRamAccountingContext;
    private final RowReceiver rowReceiver;

    private final IntObjectOpenHashMap<CrateSearchContext> searchContexts = new IntObjectOpenHashMap<>();
    private final Object subContextLock = new Object();

    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AtomicBoolean killed = new AtomicBoolean(false);
    private final SettableFuture<Void> closeFuture = SettableFuture.create();
    private final SettableFuture<Void> killFuture = SettableFuture.create();
    private Collection<CrateCollector> collectors;

    private ContextCallback contextCallback = ContextCallback.NO_OP;

    private static final ESLogger LOGGER = Loggers.getLogger(JobCollectContext.class);

    public JobCollectContext(UUID jobId,
                             final CollectPhase collectPhase,
                             MapSideDataCollectOperation collectOperation,
                             RamAccountingContext queryPhaseRamAccountingContext,
                             final RowReceiver rowReceiver) {
        id = jobId;
        this.collectNode = collectPhase;
        this.collectOperation = collectOperation;
        this.queryPhaseRamAccountingContext = queryPhaseRamAccountingContext;
        this.rowReceiver = new ForwardingRowReceiver(rowReceiver) {

            @Override
            public void finish() {
                super.finish();
                if (!collectPhase.keepContextForFetcher()) {
                    close();
                }
            }

            @Override
            public void fail(Throwable throwable) {
                super.fail(throwable);
                closeDueToFailure(throwable);
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
        if (closed.compareAndSet(false, true)) { // prevent double close
            LOGGER.trace("closing JobCollectContext: {}", id);
            closeSearchContexts();
            long bytesUsed = queryPhaseRamAccountingContext.totalBytes();
            contextCallback.onClose(throwable, bytesUsed);
            queryPhaseRamAccountingContext.close();
            closeFuture.set(null);
        } else {
            LOGGER.trace("close called on an already closed JobCollectContext: {}", id);
            try {
                closeFuture.get();
            } catch (Throwable e) {
                LOGGER.warn("Error while waiting for already running close {}", e, id);
            }
        }
    }

    private void closeSearchContexts() {
        synchronized (subContextLock) {
            for (ObjectCursor<CrateSearchContext> cursor : searchContexts.values()) {
                cursor.value.close();
            }
            searchContexts.clear();
        }
    }

    @Override
    public void kill(@Nullable Throwable throwable) {
        if (killed.compareAndSet(false, true)) { // prevent double kill
            LOGGER.trace("killing JobCollectContext: {}", id);
            if (throwable == null) {
                throwable = new CancellationException();
            }
            for (CrateCollector collector : collectors) {
                collector.kill(throwable);
            }
            closeSearchContexts();
            contextCallback.onKill();
            queryPhaseRamAccountingContext.close();
            killFuture.set(null);
        } else {
            LOGGER.trace("kill called on an already killed JobCollectContext: {}", id);
            try {
                killFuture.get();
            } catch (Throwable e) {
                LOGGER.warn("Error while waiting for already running kill {}", e);
            }

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

    private void propagateFailure(Throwable t){
        assert contextCallback != null;
        rowReceiver.fail(t);
        if (contextCallback != null){
            contextCallback.onClose(t, -1);
        }
    }

    @Override
    public void prepare() {
        try {
            collectors = collectOperation.createCollectors(collectNode, rowReceiver, this);
        } catch (Throwable t) {
            propagateFailure(t);
        }
    }

    @Override
    public void start() {
        try {
            collectOperation.launchCollectors(collectNode, collectors);
        } catch (Throwable t) {
            propagateFailure(t);
        }
    }

    @Override
    public boolean isKilled() {
        return killed.get();
    }

    public RamAccountingContext queryPhaseRamAccountingContext() {
        return queryPhaseRamAccountingContext;
    }

    public KeepAliveListener keepAliveListener() {
        return contextCallback;
    }
}
