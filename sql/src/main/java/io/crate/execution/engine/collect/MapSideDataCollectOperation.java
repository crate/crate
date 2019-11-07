/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

import com.google.common.annotations.VisibleForTesting;
import io.crate.data.BatchIterator;
import io.crate.data.Row;
import io.crate.execution.dsl.phases.CollectPhase;
import io.crate.execution.dsl.phases.RoutedCollectPhase;
import io.crate.execution.engine.collect.sources.CollectSource;
import io.crate.execution.engine.collect.sources.CollectSourceResolver;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.TransactionContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;

/**
 * collect local data from node/shards/docs on nodes where the data resides (aka Mapper nodes)
 */
@Singleton
public class MapSideDataCollectOperation {

    private static final Logger LOGGER = LogManager.getLogger(MapSideDataCollectOperation.class);

    private final CollectSourceResolver collectSourceResolver;
    private final ThreadPool threadPool;
    private final Queue<Runnable> pendingMemoryIntenseExecutions = new ArrayBlockingQueue<>(30);

    @Inject
    public MapSideDataCollectOperation(CollectSourceResolver collectSourceResolver, ThreadPool threadPool) {
        this.collectSourceResolver = collectSourceResolver;
        this.threadPool = threadPool;
        threadPool.generic().execute(() -> {
            while (true) {
                Runnable runnable = pendingMemoryIntenseExecutions.poll();
                if (runnable == null) {
                    Thread.sleep(20);
                } else {
                    try {
                        runnable.run();
                    } catch (Throwable t) {
                        LOGG
                    }
                }
            }
        });
    }

    public BatchIterator<Row> createIterator(TransactionContext txnCtx,
                                             CollectPhase collectPhase,
                                             boolean requiresScroll,
                                             CollectTask collectTask) {
        CollectSource service = collectSourceResolver.getService(collectPhase);
        return service.getIterator(txnCtx, collectPhase, collectTask, requiresScroll);
    }

    public void launch(Runnable runnable, CollectPhase collectPhase, boolean involvesIO) {
        if (collectPhase instanceof RoutedCollectPhase) {
            if (largeMemoryUsageExpected((RoutedCollectPhase) collectPhase)) {
                if (!pendingMemoryIntenseExecutions.offer(runnable)) {
                    throw new RejectedExecutionException("No capacity left to run queries which are expected to require a lot of memory");
                }
            }
        }
        String threadPoolName = threadPoolName(collectPhase, involvesIO);
        Executor executor = threadPool.executor(threadPoolName);
        executor.execute(runnable);
    }

    private static boolean largeMemoryUsageExpected(RoutedCollectPhase routedCollectPhase) {
        Integer nodePageSizeHint = routedCollectPhase.nodePageSizeHint();
        return nodePageSizeHint != null && nodePageSizeHint > 5000 && routedCollectPhase.orderBy() != null;
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
