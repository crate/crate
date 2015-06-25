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
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import io.crate.action.sql.query.CrateSearchContext;
import io.crate.breaker.RamAccountingContext;
import io.crate.core.collections.Bucket;
import io.crate.jobs.ContextCallback;
import io.crate.jobs.ExecutionState;
import io.crate.jobs.ExecutionSubContext;
import io.crate.operation.RowDownstreamHandle;
import io.crate.operation.RowUpstream;
import io.crate.operation.projectors.ResultProvider;
import io.crate.planner.node.dql.CollectNode;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Locale;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicBoolean;

public class JobCollectContext implements ExecutionSubContext, RowUpstream, ExecutionState {

    private final UUID id;
    private final CollectNode collectNode;
    private final CollectOperation collectOperation;
    private final RamAccountingContext ramAccountingContext;
    private final ResultProvider downstream;

    private final IntObjectOpenHashMap<CrateSearchContext> searchContexts = new IntObjectOpenHashMap<>();
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
                             ResultProvider downstream) {
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

    public void addContext(int jobSearchContextId, CrateSearchContext shardCollectContext) {
        if (closed.get()) {
            // if this is closed and addContext is called this means the context got killed.
            shardCollectContext.close();
            return;
        }

        synchronized (subContextLock) {
            CrateSearchContext replacedContext = searchContexts.put(jobSearchContextId, shardCollectContext);
            if (replacedContext != null) {
                replacedContext.close();
                shardCollectContext.close();
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
            LOGGER.error("closing JobCollectContext: {}", id);
            synchronized (subContextLock) {
                for (ObjectCursor<CrateSearchContext> cursor : searchContexts.values()) {
                    cursor.value.close();
                }
            }
            for (ContextCallback contextCallback : contextCallbacks) {
                contextCallback.onClose(throwable, ramAccountingContext.totalBytes());
            }
            ramAccountingContext.close();
        } else {
            LOGGER.trace("close called on an already closed JobCollectContext: {}", id);
        }
    }

    @Override
    public void kill() {
        isKilled = true;
        close(new CancellationException());
    }

    @Override
    public String name() {
        return collectNode.name();
    }

    @Override
    public void start() {
        try {
            Futures.addCallback(downstream.result(), new FutureCallback<Bucket>() {
                @Override
                public void onSuccess(@Nullable Bucket result) {
                    // if the result is pushed to a downstream, the Bucket might be null here.
                    if ((result != null && result.size() == 0) || !collectNode.keepContextForFetcher()) {
                        close();
                    }
                }

                @Override
                public void onFailure(@Nonnull Throwable t) {
                    closeDueToFailure(t);
                }
            });
            collectOperation.collect(collectNode, downstream, this);
        } catch (Throwable t) {
            RowDownstreamHandle rowDownstreamHandle = downstream.registerUpstream(this);
            rowDownstreamHandle.fail(t);
            closeDueToFailure(t);
        }
    }

    @Override
    public boolean isKilled() {
        return isKilled;
    }

    public RamAccountingContext ramAccountingContext() {
        return ramAccountingContext;
    }
}
