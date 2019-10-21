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

package io.crate.execution.engine.collect;

import com.carrotsearch.hppc.IntObjectHashMap;
import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.google.common.annotations.VisibleForTesting;
import io.crate.breaker.RamAccountingContext;
import io.crate.data.BatchIterator;
import io.crate.data.Row;
import io.crate.data.RowConsumer;
import io.crate.execution.dsl.phases.CollectPhase;
import io.crate.execution.dsl.phases.RoutedCollectPhase;
import io.crate.execution.jobs.AbstractTask;
import io.crate.execution.jobs.SharedShardContexts;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.TransactionContext;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.threadpool.ThreadPool;

import javax.annotation.Nonnull;
import java.util.Locale;

public class CollectTask extends AbstractTask {

    private final CollectPhase collectPhase;
    private final TransactionContext txnCtx;
    private final MapSideDataCollectOperation collectOperation;
    private final RamAccountingContext queryPhaseRamAccountingContext;
    private final SharedShardContexts sharedShardContexts;

    private final IntObjectHashMap<Engine.Searcher> searchers = new IntObjectHashMap<>();
    private final Object subContextLock = new Object();
    private final RowConsumer consumer;

    private BatchIterator<Row> batchIterator = null;
    private long totalBytes = -1;

    public CollectTask(final CollectPhase collectPhase,
                       TransactionContext txnCtx,
                       MapSideDataCollectOperation collectOperation,
                       RamAccountingContext queryPhaseRamAccountingContext,
                       RowConsumer consumer,
                       SharedShardContexts sharedShardContexts) {
        super(collectPhase.phaseId());
        this.collectPhase = collectPhase;
        this.txnCtx = txnCtx;
        this.collectOperation = collectOperation;
        this.queryPhaseRamAccountingContext = queryPhaseRamAccountingContext;
        this.sharedShardContexts = sharedShardContexts;
        this.consumer = consumer;
        this.consumer.completionFuture().whenComplete(closeOrKill(this));
    }

    public void addSearcher(int searcherId, Engine.Searcher searcher) {
        if (isClosed()) {
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

    @Override
    protected void innerClose() {
        totalBytes = queryPhaseRamAccountingContext.totalBytes();
        closeSearchContexts();
        queryPhaseRamAccountingContext.close();
    }

    @Override
    public long bytesUsed() {
        if (totalBytes == -1) {
            return queryPhaseRamAccountingContext.totalBytes();
        } else {
            return totalBytes;
        }
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
        if (batchIterator != null) {
            batchIterator.kill(throwable);
        }
        innerClose();
    }

    @Override
    public String name() {
        return collectPhase.name();
    }


    @Override
    public String toString() {
        return "CollectTask{" +
               "id=" + id +
               ", sharedContexts=" + sharedShardContexts +
               ", consumer=" + consumer +
               ", searchContexts=" + searchers.keys() +
               ", closed=" + isClosed() +
               '}';
    }

    @Override
    public void innerPrepare() throws Exception {
        batchIterator = collectOperation.createIterator(txnCtx, collectPhase, consumer.requiresScroll(), this);
    }

    @Override
    protected void innerStart() {
        String threadPoolName = threadPoolName(collectPhase, batchIterator.involvesIO());
        collectOperation.launch(() -> consumer.accept(batchIterator, null), threadPoolName);
    }

    public TransactionContext txnCtx() {
        return txnCtx;
    }

    public RamAccountingContext queryPhaseRamAccountingContext() {
        return queryPhaseRamAccountingContext;
    }

    public SharedShardContexts sharedShardContexts() {
        return sharedShardContexts;
    }

    @VisibleForTesting
    static String threadPoolName(CollectPhase phase, boolean involvedIO) {
        if (phase instanceof RoutedCollectPhase) {
            RoutedCollectPhase collectPhase = (RoutedCollectPhase) phase;
            if (collectPhase.maxRowGranularity() == RowGranularity.NODE
                       || collectPhase.maxRowGranularity() == RowGranularity.SHARD) {
                // Node or Shard system table collector
                return ThreadPool.Names.GET;
            }
        }
        // If there is no IO involved it is a in-memory system tables. These are usually fast and the overhead
        // of a context switch would be bigger than running this directly.
        return involvedIO ? ThreadPool.Names.SEARCH : ThreadPool.Names.SAME;
    }
}
