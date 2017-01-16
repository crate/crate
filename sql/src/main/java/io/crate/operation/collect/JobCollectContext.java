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

import com.carrotsearch.hppc.IntObjectHashMap;
import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import io.crate.action.job.SharedShardContexts;
import io.crate.breaker.RamAccountingContext;
import io.crate.concurrent.CompletionListenable;
import io.crate.jobs.AbstractExecutionSubContext;
import io.crate.metadata.RowGranularity;
import io.crate.operation.data.BatchConsumer;
import io.crate.operation.data.EmptyBatchCursor;
import io.crate.operation.data.ListenableBatchConsumer;
import io.crate.planner.node.dql.CollectPhase;
import io.crate.planner.node.dql.RoutedCollectPhase;
import org.elasticsearch.common.StopWatch;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.threadpool.ThreadPool;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Locale;

public class JobCollectContext extends AbstractExecutionSubContext {

    private static final ESLogger LOGGER = Loggers.getLogger(JobCollectContext.class);

    private final CollectPhase collectPhase;
    private final MapSideDataCollectOperation collectOperation;
    private final RamAccountingContext queryPhaseRamAccountingContext;
    private final BatchConsumer rowReceiver;
    private final SharedShardContexts sharedShardContexts;

    private final IntObjectHashMap<Engine.Searcher> searchers = new IntObjectHashMap<>();
    private final Object subContextLock = new Object();
    private final String threadPoolName;

    private Collection<CrateCollector> collectors;
    private ListenableFuture<?> completionFuture;

    public JobCollectContext(final CollectPhase collectPhase,
                             MapSideDataCollectOperation collectOperation,
                             String localNodeId,
                             RamAccountingContext queryPhaseRamAccountingContext,
                             final BatchConsumer rowReceiver,
                             SharedShardContexts sharedShardContexts) {
        super(collectPhase.phaseId(), LOGGER);
        this.collectPhase = collectPhase;
        this.collectOperation = collectOperation;
        this.queryPhaseRamAccountingContext = queryPhaseRamAccountingContext;
        this.sharedShardContexts = sharedShardContexts;

        // XDOBE: we should make the collectors CompletionListenable instead of wrapping the cursor
        if (rowReceiver instanceof CompletionListenable){
            this.rowReceiver = rowReceiver;
            completionFuture = ((CompletionListenable) rowReceiver).completionFuture();
        } else {
            ListenableBatchConsumer lbc = new ListenableBatchConsumer(rowReceiver);
            this.rowReceiver = lbc;
            completionFuture = lbc.completionFuture();
        }
        Futures.addCallback(completionFuture, new FutureCallback<Object>() {
            @Override
            public void onSuccess(@Nullable Object result) {
                close();
            }

            @Override
            public void onFailure(Throwable t) {
                closeDueToFailure(t);
            }
        });
        this.threadPoolName = threadPoolName(collectPhase, localNodeId);
    }

    public void addSearcher(int searcherId, Engine.Searcher searcher) {
        if (future.closed()) {
            // if this is closed and addContext is called this means the context got killed.
            searcher.close();
            return;
        }

        synchronized (subContextLock) {
            Engine.Searcher replacedSearcher = searchers.put(searcherId, searcher);
            if (replacedSearcher != null) {
                replacedSearcher.close();
                searcher.close();
                throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                    "ShardCollectContext for %d already added", searcherId));
            }
        }
    }

    public void closeDueToFailure(Throwable throwable) {
        close(throwable);
    }

    @Override
    public void cleanup() {
        closeSearchContexts();
        queryPhaseRamAccountingContext.close();
    }

    @Override
    protected void innerClose(@Nullable Throwable throwable) {
        future.bytesUsed(queryPhaseRamAccountingContext.totalBytes());
    }

    private void closeSearchContexts() {
        synchronized (subContextLock) {
            for (ObjectCursor<Engine.Searcher> cursor : searchers.values()) {
                cursor.value.close();
            }
            searchers.clear();
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
               "id=" + id +
               ", sharedContexts=" + sharedShardContexts +
               ", rowReceiver=" + rowReceiver +
               ", searchContexts=" + Arrays.toString(searchers.keys) +
               ", closed=" + future.closed() +
               '}';
    }

    @Override
    public void innerPrepare() throws Exception {
        collectors = collectOperation.createCollectors(collectPhase, rowReceiver, this);
    }

    @Override
    protected void innerStart() {
        if (collectors.isEmpty()) {
            rowReceiver.accept(new EmptyBatchCursor());
        } else {
            if (logger.isTraceEnabled()) {
                measureCollectTime();
            }
            collectOperation.launchCollectors(collectors, threadPoolName);
        }
    }

    private void measureCollectTime() {
        final StopWatch stopWatch = new StopWatch(collectPhase.phaseId() + ": " + collectPhase.name());
        stopWatch.start("starting collectors");
        completionFuture.addListener(() -> {
            stopWatch.stop();
            logger.trace("Collectors finished: {}", stopWatch.shortSummary());
        }, MoreExecutors.directExecutor());
    }

    public RamAccountingContext queryPhaseRamAccountingContext() {
        return queryPhaseRamAccountingContext;
    }

    public SharedShardContexts sharedShardContexts() {
        return sharedShardContexts;
    }

    @VisibleForTesting
    static String threadPoolName(CollectPhase phase, String localNodeId) {
        if (phase instanceof RoutedCollectPhase) {
            RoutedCollectPhase collectPhase = (RoutedCollectPhase) phase;
            if (collectPhase.maxRowGranularity() == RowGranularity.DOC
                && collectPhase.routing().containsShards(localNodeId)) {
                // DOC table collectors
                return ThreadPool.Names.SEARCH;
            } else if (collectPhase.maxRowGranularity() == RowGranularity.NODE
                       || collectPhase.maxRowGranularity() == RowGranularity.SHARD) {
                // Node or Shard system table collector
                return ThreadPool.Names.MANAGEMENT;
            }
        }

        // Anything else like INFORMATION_SCHEMA tables or sys.cluster table collector
        return ThreadPool.Names.PERCOLATE;
    }

    @VisibleForTesting
    public Collection<CrateCollector> collectors() {
        return collectors;
    }


}
