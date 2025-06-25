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

package io.crate.execution.jobs;

import static io.crate.data.SentinelRow.SENTINEL;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.jetbrains.annotations.NotNull;

import com.carrotsearch.hppc.IntIndexedContainer;

import io.crate.data.InMemoryBatchIterator;
import io.crate.data.Row1;
import io.crate.data.RowConsumer;
import io.crate.execution.dsl.phases.CountPhase;
import io.crate.execution.engine.collect.count.CountOperation;
import io.crate.metadata.TransactionContext;

public class CountTask extends AbstractTask {

    private final CountPhase countPhase;
    private final TransactionContext txnCtx;
    private final CountOperation countOperation;
    private final RowConsumer consumer;
    private final Map<String, IntIndexedContainer> indexShardMap;
    private CompletableFuture<Long> countFuture;
    private volatile Throwable killReason;

    CountTask(CountPhase countPhase,
              TransactionContext txnCtx,
              CountOperation countOperation,
              RowConsumer consumer,
              Map<String, IntIndexedContainer> indexShardMap) {
        super(countPhase.phaseId());
        this.countPhase = countPhase;
        this.txnCtx = txnCtx;
        this.countOperation = countOperation;
        this.consumer = consumer;
        this.indexShardMap = indexShardMap;
    }

    @Override
    public synchronized CompletableFuture<Void> innerStart() {
        try {
            countFuture = countOperation.count(txnCtx, indexShardMap, countPhase.where(), countPhase.onPartitionedTable());
        } catch (Throwable t) {
            consumer.accept(null, t);
            return null;
        }
        countFuture.whenComplete((rowCount, failure) -> {
            Throwable killed = killReason;  // 1 volatile read
            failure = killed == null ? failure : killed;
            if (failure == null) {
                consumer.accept(InMemoryBatchIterator.of(new Row1(rowCount), SENTINEL), null);
                close();
            } else {
                consumer.accept(null, failure);
                kill(failure);
            }
        });
        return null;
    }

    @Override
    public synchronized void innerKill(@NotNull Throwable throwable) {
        if (countFuture == null) {
            consumer.accept(null, throwable);
        } else {
            killReason = throwable;
            countFuture.cancel(true);
        }
    }

    @Override
    public String name() {
        return countPhase.name();
    }

    @Override
    public long bytesUsed() {
        return -1;
    }
}
