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

package io.crate.execution.engine.collect;

import java.util.ArrayList;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import org.apache.lucene.search.IndexSearcher;
import org.elasticsearch.Version;
import org.elasticsearch.threadpool.ThreadPool;

import com.carrotsearch.hppc.IntObjectHashMap;

import io.crate.common.annotations.GuardedBy;
import org.jetbrains.annotations.VisibleForTesting;
import io.crate.common.collections.RefCountedItem;
import io.crate.common.exceptions.Exceptions;
import io.crate.data.BatchIterator;
import io.crate.data.Row;
import io.crate.data.RowConsumer;
import io.crate.data.breaker.BlockBasedRamAccounting;
import io.crate.data.breaker.RamAccounting;
import io.crate.execution.dsl.phases.CollectPhase;
import io.crate.execution.dsl.phases.RoutedCollectPhase;
import io.crate.execution.jobs.SharedShardContexts;
import io.crate.execution.jobs.Task;
import io.crate.memory.MemoryManager;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.TransactionContext;


public class CollectTask implements Task {


    private final CollectPhase collectPhase;
    private final TransactionContext txnCtx;
    private final MapSideDataCollectOperation collectOperation;
    private final RamAccounting ramAccounting;
    private final Function<RamAccounting, MemoryManager> memoryManagerFactory;
    private final SharedShardContexts sharedShardContexts;

    private final IntObjectHashMap<RefCountedItem<? extends IndexSearcher>> searchers = new IntObjectHashMap<>();
    private final RowConsumer consumer;
    private final int ramAccountingBlockSizeInBytes;

    @GuardedBy("searchers")
    private final ArrayList<MemoryManager> memoryManagers = new ArrayList<>();
    private final Version minNodeVersion;
    private final CompletableFuture<Void> consumerCompleted;
    private final CompletableFuture<BatchIterator<Row>> batchIterator = new CompletableFuture<>();
    private final AtomicBoolean started = new AtomicBoolean(false);

    @GuardedBy("searchers")
    private boolean releasedResources = false;

    private long totalBytes = -1;

    public CollectTask(CollectPhase collectPhase,
                       TransactionContext txnCtx,
                       MapSideDataCollectOperation collectOperation,
                       RamAccounting ramAccounting,
                       Function<RamAccounting, MemoryManager> memoryManagerFactory,
                       RowConsumer consumer,
                       SharedShardContexts sharedShardContexts,
                       Version minNodeVersion,
                       int ramAccountingBlockSizeInBytes) {
        this.collectPhase = collectPhase;
        this.txnCtx = txnCtx;
        this.collectOperation = collectOperation;
        this.ramAccounting = ramAccounting;
        this.memoryManagerFactory = memoryManagerFactory;
        this.sharedShardContexts = sharedShardContexts;
        this.consumer = consumer;
        this.ramAccountingBlockSizeInBytes = ramAccountingBlockSizeInBytes;
        this.minNodeVersion = minNodeVersion;
        this.batchIterator.whenComplete((it, err) -> {
            if (err == null) {
                try {
                    String threadPoolName = threadPoolName(collectPhase, it.hasLazyResultSet());
                    collectOperation.launch(() -> consumer.accept(it, null), threadPoolName);
                } catch (Throwable t) {
                    consumer.accept(null, t);
                }
            } else {
                consumer.accept(null, err);
            }
        });
        this.consumerCompleted = consumer.completionFuture().handle((res, err) -> {
            totalBytes = ramAccounting.totalBytes();
            releaseResources();
            if (err != null) {
                Exceptions.rethrowUnchecked(err);
            }
            return null;
        });
    }

    private void releaseResources() {
        synchronized (searchers) {
            if (releasedResources == false) {
                releasedResources = true;
                for (var cursor : searchers.values()) {
                    cursor.value.close();
                }
                searchers.clear();
                for (var memoryManager : memoryManagers) {
                    memoryManager.close();
                }
                memoryManagers.clear();
            } else {
                throw new AssertionError("Double release must not happen");
            }
        }
    }

    @Override
    public CompletableFuture<Void> completionFuture() {
        return consumerCompleted;
    }

    @Override
    public void kill(Throwable throwable) {
        if (started.compareAndSet(false, true)) {
            consumer.accept(null, throwable);
        } else {
            batchIterator.whenComplete((it, err) -> {
                if (err == null) {
                    it.kill(throwable);
                } // else: Consumer must have received a failure already
            });
        }
    }

    @Override
    public CompletableFuture<Void> start() {
        if (started.compareAndSet(false, true)) {
            try {
                var futureIt = collectOperation.createIterator(
                    txnCtx,
                    collectPhase,
                    consumer.requiresScroll(),
                    this
                );
                futureIt.whenComplete((it, err) -> {
                    if (err == null) {
                        batchIterator.complete(it);
                    } else {
                        batchIterator.completeExceptionally(err);
                    }
                });
            } catch (Throwable t) {
                batchIterator.completeExceptionally(t);
            }
        }
        return null;
    }

    @Override
    public int id() {
        return collectPhase.phaseId();
    }

    public void addSearcher(int searcherId, RefCountedItem<? extends IndexSearcher> searcher) {
        synchronized (searchers) {
            if (releasedResources == false) {
                var replacedSearcher = searchers.put(searcherId, searcher);
                if (replacedSearcher != null) {
                    replacedSearcher.close();
                    throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                        "ShardCollectContext for %d already added", searcherId));
                }
            } else {
                searcher.close();
                // addSearcher call after resource-release should only happen in error case
                // the join call should trigger the original failure
                try {
                    consumerCompleted.join();
                } catch (CompletionException e) {
                    throw Exceptions.toRuntimeException(e.getCause());
                }
                throw new AssertionError("addSearcher call after resources have already been released once");
            }
        }
    }

    @Override
    public long bytesUsed() {
        if (totalBytes == -1) {
            return ramAccounting.totalBytes();
        } else {
            return totalBytes;
        }
    }

    @Override
    public String name() {
        return collectPhase.name();
    }

    @Override
    public String toString() {
        synchronized (searchers) {
            return "CollectTask{" +
                "id=" + collectPhase.phaseId() +
                ", sharedContexts=" + sharedShardContexts +
                ", consumer=" + consumer +
                ", searchContexts=" + searchers.keys() +
                ", batchIterator=" + batchIterator +
                ", finished=" + consumerCompleted.isDone() +
                '}';
        }
    }

    public TransactionContext txnCtx() {
        return txnCtx;
    }

    public RamAccounting getRamAccounting() {
        // No tracking/close of BlockBasedRamAccounting
        // to avoid double-release of bytes when the parent instance (`ramAccounting`) is closed.
        return new BlockBasedRamAccounting(ramAccounting::addBytes, ramAccountingBlockSizeInBytes);
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

    public MemoryManager memoryManager() {
        MemoryManager memoryManager = memoryManagerFactory.apply(ramAccounting);
        // an atomicBoolean call would not be enough, because without syncronization
        // the `memoryManagers.add` could be called just right *after* another thread triggered `releaseResources`
        synchronized (searchers) {
            if (releasedResources == false) {
                memoryManagers.add(memoryManager);
                return memoryManager;
            } else {
                memoryManager.close();
                // memoryManager acess after resource-release should only happen in error case
                // the join call should trigger the original failure
                try {
                    consumerCompleted.join();
                } catch (CompletionException e) {
                    throw Exceptions.toRuntimeException(e.getCause());
                }
                throw new AssertionError("memoryManager access after resources have already been released once");
            }
        }
    }

    public Version minNodeVersion() {
        return minNodeVersion;
    }
}
