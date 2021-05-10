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

import io.crate.data.BatchIterator;
import io.crate.data.Row;
import io.crate.execution.dsl.phases.CollectPhase;
import io.crate.execution.engine.collect.sources.CollectSource;
import io.crate.execution.engine.collect.sources.CollectSourceResolver;
import io.crate.metadata.TransactionContext;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;

/**
 * collect local data from node/shards/docs on nodes where the data resides (aka Mapper nodes)
 */
@Singleton
public class MapSideDataCollectOperation {

    private final CollectSourceResolver collectSourceResolver;
    private final ThreadPool threadPool;

    @Inject
    public MapSideDataCollectOperation(CollectSourceResolver collectSourceResolver, ThreadPool threadPool) {
        this.collectSourceResolver = collectSourceResolver;
        this.threadPool = threadPool;
    }

    public CompletableFuture<BatchIterator<Row>> createIterator(TransactionContext txnCtx,
                                                                CollectPhase collectPhase,
                                                                boolean requiresScroll,
                                                                CollectTask collectTask) {
        CollectSource service = collectSourceResolver.getService(collectPhase);
        return service.getIterator(txnCtx, collectPhase, collectTask, requiresScroll);
    }

    public void launch(Runnable runnable, String threadPoolName) throws RejectedExecutionException {
        Executor executor = threadPool.executor(threadPoolName);
        executor.execute(runnable);
    }
}
