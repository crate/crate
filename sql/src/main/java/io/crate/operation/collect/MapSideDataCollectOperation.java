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

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import io.crate.data.BatchConsumer;
import io.crate.operation.ThreadPools;
import io.crate.operation.collect.sources.CollectSource;
import io.crate.operation.collect.sources.CollectSourceResolver;
import io.crate.planner.node.dql.CollectPhase;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Collection;
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
    public Collection<CrateCollector> createCollectors(CollectPhase collectPhase,
                                                       BatchConsumer consumer,
                                                       final JobCollectContext jobCollectContext) {
        CollectSource service = collectSourceResolver.getService(collectPhase);
        return service.getCollectors(collectPhase, consumer, jobCollectContext);
    }

    public void launchCollectors(Collection<CrateCollector> shardCollectors, String threadPoolName) throws RejectedExecutionException {
        assert !shardCollectors.isEmpty() : "must have at least one collector to launch";
        Executor executor = threadPool.executor(threadPoolName);
        if (executor instanceof ThreadPoolExecutor) {
            ThreadPools.runWithAvailableThreads(
                (ThreadPoolExecutor) executor,
                collectors2Runnables(shardCollectors));
        } else {
            // assume executor is just a wrapper to 1 thread
            for (CrateCollector collector : shardCollectors) {
                collector.doCollect();
            }
        }
    }

    private Collection<Runnable> collectors2Runnables(Collection<CrateCollector> collectors) {
        return Collections2.transform(collectors, new Function<CrateCollector, Runnable>() {
            @Override
            public Runnable apply(final CrateCollector collector) {
                return new Runnable() {
                    @Override
                    public void run() {
                        collector.doCollect();
                    }
                };
            }
        });
    }
}
