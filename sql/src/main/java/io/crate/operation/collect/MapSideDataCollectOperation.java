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

package io.crate.operation.collect;

import io.crate.data.RowConsumer;
import io.crate.operation.collect.sources.CollectSource;
import io.crate.operation.collect.sources.CollectSourceResolver;
import io.crate.planner.node.dql.CollectPhase;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.threadpool.ThreadPool;

import javax.annotation.Nonnull;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;

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

    /**
     * dispatch by the following criteria:
     * <p>
     * * if local node id is contained in routing:<br>
     * * if no shards are given:<br>
     * &nbsp; -&gt; run row granularity level collect<br>
     * &nbsp; except for doc level:
     * &nbsp; &nbsp; if table if partitioned:
     * &nbsp; &nbsp; -&gt; edge case for empty partitioned table
     * &nbsp; &nbsp; else:
     * &nbsp; &nbsp; -&gt; collect from information schema
     * * if shards are given:<br>
     * &nbsp; -&gt; run shard or doc level collect<br>
     * * else if we got cluster RowGranularity:<br>
     * &nbsp; -&gt; run node level collect (cluster level)<br>
     * </p>
     */
    public CrateCollector createCollector(CollectPhase collectPhase,
                                          RowConsumer consumer,
                                          final JobCollectContext jobCollectContext) {
        CollectSource service = collectSourceResolver.getService(collectPhase);
        return service.getCollector(collectPhase, consumer, jobCollectContext);
    }

    public void launchCollector(@Nonnull CrateCollector collector, String threadPoolName) throws RejectedExecutionException {
        Executor executor = threadPool.executor(threadPoolName);
        if (executor instanceof ThreadPoolExecutor) {
            executor.execute(collector::doCollect);
        } else {
            collector.doCollect();
        }
    }
}
