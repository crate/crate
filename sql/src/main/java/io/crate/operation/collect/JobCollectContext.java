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
import com.google.common.util.concurrent.MoreExecutors;
import io.crate.action.job.SharedShardContexts;
import io.crate.action.sql.query.CrateSearchContext;
import io.crate.breaker.RamAccountingContext;
import io.crate.jobs.AbstractExecutionSubContext;
import io.crate.jobs.ExecutionState;
import io.crate.jobs.KeepAliveListener;
import io.crate.operation.projectors.ListenableRowReceiver;
import io.crate.operation.projectors.RowReceiver;
import io.crate.operation.projectors.RowReceivers;
import io.crate.planner.node.dql.CollectPhase;
import org.elasticsearch.common.StopWatch;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Locale;

public class JobCollectContext extends AbstractExecutionSubContext implements ExecutionState {

    private final CollectPhase collectPhase;
    private final MapSideDataCollectOperation collectOperation;
    private final RamAccountingContext queryPhaseRamAccountingContext;
    private final RowReceiver rowReceiver;
    private final SharedShardContexts sharedShardContexts;

    private final IntObjectOpenHashMap<CrateSearchContext> searchContexts = new IntObjectOpenHashMap<>();
    private final Object subContextLock = new Object();
    private final ListenableRowReceiver listenableRowReceiver;

    private Collection<CrateCollector> collectors;

    public JobCollectContext(final CollectPhase collectPhase,
                             MapSideDataCollectOperation collectOperation,
                             RamAccountingContext queryPhaseRamAccountingContext,
                             final RowReceiver rowReceiver,
                             SharedShardContexts sharedShardContexts) {
        super(collectPhase.executionPhaseId());
        this.collectPhase = collectPhase;
        this.collectOperation = collectOperation;
        this.queryPhaseRamAccountingContext = queryPhaseRamAccountingContext;
        this.sharedShardContexts = sharedShardContexts;

        listenableRowReceiver = RowReceivers.listenableRowReceiver(rowReceiver);
        Futures.addCallback(listenableRowReceiver.finishFuture(), new FutureCallback<Void>() {
            @Override
            public void onSuccess(@Nullable Void result) {
                close();
            }

            @Override
            public void onFailure(@Nonnull Throwable t) {
                closeDueToFailure(t);
            }
        });
        this.rowReceiver = listenableRowReceiver;
    }

    public void addSearchContext(int jobSearchContextId, CrateSearchContext searchContext) {
        if (future.closed()) {
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
    protected void cleanup() {
        closeSearchContexts();
        queryPhaseRamAccountingContext.close();
    }

    @Override
    protected void innerClose(@Nullable Throwable throwable) {
        future.bytesUsed(queryPhaseRamAccountingContext.totalBytes());
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
    public void innerKill(@Nonnull Throwable throwable) {
        if (collectors != null) {
            for (CrateCollector collector : collectors) {
                collector.kill(throwable);
            }
        }
        future.bytesUsed(queryPhaseRamAccountingContext.totalBytes());
    }

    @Override
    public String name() {
        return collectPhase.name();
    }


    @Override
    public String toString() {
        return "JobCollectContext{" +
                "searchContexts=" + searchContexts +
                ", closed=" + future.closed() +
                '}';
    }

    @Override
    protected void innerPrepare() {
        collectors = collectOperation.createCollectors(collectPhase, rowReceiver, this);

    }

    @Override
    protected void innerStart() {
        if (collectors.isEmpty()) {
            rowReceiver.finish();
        } else {
            if (LOGGER.isTraceEnabled()) {
                measureCollectTime();
            }
            collectOperation.launchCollectors(collectPhase, collectors);
        }
    }

    private void measureCollectTime() {
        final StopWatch stopWatch = new StopWatch(collectPhase.executionPhaseId() + ": " + collectPhase.name());
        stopWatch.start("starting collectors");
        listenableRowReceiver.finishFuture().addListener(new Runnable() {
            @Override
            public void run() {
                stopWatch.stop();
                LOGGER.trace("Collectors finished: {}", stopWatch.shortSummary());
            }
        }, MoreExecutors.directExecutor());
    }

    public RamAccountingContext queryPhaseRamAccountingContext() {
        return queryPhaseRamAccountingContext;
    }

    public KeepAliveListener keepAliveListener() {
        return keepAliveListener;
    }

    public SharedShardContexts sharedShardContexts() {
        return sharedShardContexts;
    }


    /**
     * active by default, because as long its operation is running
     * it is keeping the local execution context alive itself
     * and does not need external keep alives.
     */
    @Override
    public SubContextMode subContextMode() {
        return SubContextMode.ACTIVE;
    }
}
