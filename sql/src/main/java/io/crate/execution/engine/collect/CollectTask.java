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

import com.google.common.annotations.VisibleForTesting;
import io.crate.breaker.RamAccountingContext;
import io.crate.data.BatchIterator;
import io.crate.data.ListenableRowConsumer;
import io.crate.data.Row;
import io.crate.data.RowConsumer;
import io.crate.execution.dsl.phases.CollectPhase;
import io.crate.execution.dsl.phases.RoutedCollectPhase;
import io.crate.execution.jobs.CompletionState;
import io.crate.execution.jobs.SharedShardContexts;
import io.crate.execution.jobs.Task;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.TransactionContext;
import org.elasticsearch.threadpool.ThreadPool;

import javax.annotation.Nonnull;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

public class CollectTask implements Task {

    private final CollectPhase collectPhase;
    private final TransactionContext txnCtx;
    private final MapSideDataCollectOperation collectOperation;
    private final RamAccountingContext queryPhaseRamAccountingContext;
    private final ListenableRowConsumer consumer;
    private final SharedShardContexts sharedShardContexts;

    private final String threadPoolName;
    private final AtomicBoolean started = new AtomicBoolean(false);

    private volatile BatchIterator<Row> batchIterator;
    private long bytesUsed = -1;

    public CollectTask(final CollectPhase collectPhase,
                       TransactionContext txnCtx,
                       MapSideDataCollectOperation collectOperation,
                       RamAccountingContext queryPhaseRamAccountingContext,
                       RowConsumer consumer,
                       SharedShardContexts sharedShardContexts) {
        this.collectPhase = collectPhase;
        this.txnCtx = txnCtx;
        this.collectOperation = collectOperation;
        this.queryPhaseRamAccountingContext = queryPhaseRamAccountingContext;
        this.sharedShardContexts = sharedShardContexts;
        this.consumer = new ListenableRowConsumer(consumer);
        this.consumer.completionFuture().whenComplete((result, err) -> {
            CollectTask.this.bytesUsed = queryPhaseRamAccountingContext.totalBytes();
            queryPhaseRamAccountingContext.close();
        });
        this.threadPoolName = threadPoolName(collectPhase);
    }

    @Override
    public String name() {
        return collectPhase.name();
    }

    @Override
    public int id() {
        return collectPhase.phaseId();
    }


    @Override
    public String toString() {
        return "CollectTask{" +
               "id=" + collectPhase.phaseId() +
               ", sharedContexts=" + sharedShardContexts +
               ", consumer=" + consumer +
               '}';
    }

    @Override
    public void start() {
        var iterator = collectOperation.createIterator(txnCtx, collectPhase, consumer.requiresScroll(), this);
        this.batchIterator = iterator;
        if (started.compareAndSet(false, true)) {
            try {
                collectOperation.launch(() -> consumer.accept(iterator, null), threadPoolName);
            } catch (RejectedExecutionException e) {
                consumer.accept(null, e);
            }
        } else {
            iterator.close();
        }
    }

    @Override
    public void kill(@Nonnull Throwable throwable) {
        if (started.getAndSet(true)) {
            batchIterator.kill(throwable);
        } else {
            consumer.accept(null, throwable);
        }
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
    static String threadPoolName(CollectPhase phase) {
        if (phase instanceof RoutedCollectPhase) {
            RoutedCollectPhase collectPhase = (RoutedCollectPhase) phase;
            if (collectPhase.maxRowGranularity() == RowGranularity.NODE
                       || collectPhase.maxRowGranularity() == RowGranularity.SHARD) {
                // Node or Shard system table collector
                return ThreadPool.Names.GET;
            }
        }

        // Anything else like doc tables, INFORMATION_SCHEMA tables or sys.cluster table collector, partition collector
        return ThreadPool.Names.SEARCH;
    }

    @Override
    public CompletableFuture<CompletionState> completionFuture() {
        return consumer.completionFuture().thenApply(ignored -> new CompletionState(bytesUsed));
    }
}
