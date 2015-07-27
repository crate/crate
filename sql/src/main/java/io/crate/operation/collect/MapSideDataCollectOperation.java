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
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import io.crate.analyze.EvaluatingNormalizer;
import io.crate.exceptions.UnhandledServerException;
import io.crate.metadata.Functions;
import io.crate.metadata.NestedReferenceResolver;
import io.crate.operation.RowDownstream;
import io.crate.operation.RowUpstream;
import io.crate.operation.ThreadPools;
import io.crate.operation.collect.sources.CollectSource;
import io.crate.operation.collect.sources.CollectSourceResolver;
import io.crate.operation.reference.sys.node.NodeSysExpression;
import io.crate.operation.reference.sys.node.NodeSysReferenceResolver;
import io.crate.planner.RowGranularity;
import io.crate.planner.node.dql.CollectPhase;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.threadpool.ThreadPool;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * collect local data from node/shards/docs on nodes where the data resides (aka Mapper nodes)
 */
@Singleton
public class MapSideDataCollectOperation implements CollectOperation, RowUpstream {

    private final EvaluatingNormalizer clusterNormalizer;
    private final ClusterService clusterService;
    private final CollectSourceResolver collectSourceResolver;
    private final ThreadPoolExecutor executor;
    private final ListeningExecutorService listeningExecutorService;
    private final int poolSize;
    private final Functions functions;
    private final NodeSysExpression nodeSysExpression;

    @Override
    public void pause() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void resume() {
        throw new UnsupportedOperationException();
    }

    private static class VoidFunction<Arg> implements Function<Arg, Void> {
        @Nullable
        @Override
        public Void apply(@Nullable Arg input) {
            return null;
        }
    }

    @Inject
    public MapSideDataCollectOperation(ClusterService clusterService,
                                       Functions functions,
                                       NestedReferenceResolver clusterReferenceResolver,
                                       NodeSysExpression nodeSysExpression,
                                       ThreadPool threadPool,
                                       CollectSourceResolver collectSourceResolver) {
        this.functions = functions;
        this.nodeSysExpression = nodeSysExpression;
        this.executor = (ThreadPoolExecutor)threadPool.executor(ThreadPool.Names.SEARCH);
        this.poolSize = executor.getCorePoolSize();
        this.listeningExecutorService = MoreExecutors.listeningDecorator(executor);
        this.clusterService = clusterService;
        this.collectSourceResolver = collectSourceResolver;

        clusterNormalizer = new EvaluatingNormalizer(functions, RowGranularity.CLUSTER, clusterReferenceResolver);
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
    @Override
    public Collection<CrateCollector> collect(CollectPhase collectPhase,
                                              RowDownstream downstream,
                                              final JobCollectContext jobCollectContext) {
        assert collectPhase.isRouted(); // not routed collect is not handled here
        assert collectPhase.jobId() != null : "no jobId present for collect operation";

        String localNodeId = clusterService.state().nodes().localNodeId();
        Set<String> routingNodes = collectPhase.routing().nodes();

        if (!routingNodes.contains(localNodeId) && !localNodeId.equals(collectPhase.handlerSideCollect())) {
            throw new UnhandledServerException("unsupported routing");
        } else {
            CollectSource service = collectSourceResolver.getService(collectPhase, localNodeId);
            collectPhase = normalize(collectPhase);

            if (collectPhase.whereClause().noMatch()) {
                downstream.registerUpstream(this).finish();
                return ImmutableList.of();
            }
            Collection<CrateCollector> collectors = service.getCollectors(collectPhase, downstream, jobCollectContext);
            try {
                launchCollectors(collectPhase, collectors);
            } catch (EsRejectedExecutionException | RejectedExecutionException e) {
                downstream.registerUpstream(this).fail(e);
            }
            return collectors;
        }
    }

    private CollectPhase normalize(CollectPhase collectPhase) {
        collectPhase = collectPhase.normalize(clusterNormalizer);
        switch (collectPhase.maxRowGranularity()) {
            case NODE:
            case DOC:
                EvaluatingNormalizer normalizer =
                        new EvaluatingNormalizer(functions, RowGranularity.NODE, new NodeSysReferenceResolver(nodeSysExpression));
                return collectPhase.normalize(normalizer);
        }
        return collectPhase;
    }

    private ListenableFuture<List<Void>> launchCollectors(CollectPhase collectPhase,
                                                          final Collection<CrateCollector> shardCollectors) throws RejectedExecutionException {
        if (collectPhase.maxRowGranularity() == RowGranularity.SHARD) {
            // run sequential to prevent sys.shards queries from using too many threads
            // and overflowing the threadpool queues
            return listeningExecutorService.submit(new Callable<List<Void>>() {
                @Override
                public List<Void> call() throws Exception {
                    for (CrateCollector collector : shardCollectors) {
                        collector.doCollect();
                    }
                    return ImmutableList.of();
                }
            });
        } else {
            return ThreadPools.runWithAvailableThreads(
                    executor,
                    poolSize,
                    collectors2Callables(shardCollectors),
                    new VoidFunction<List<Void>>());
        }
    }

    private Collection<Callable<Void>> collectors2Callables(Collection<CrateCollector> collectors) {
        return Collections2.transform(collectors, new Function<CrateCollector, Callable<Void>>() {

            @Override
            public Callable<Void> apply(final CrateCollector collector) {
                return new Callable<Void>() {
                    @Override
                    public Void call() throws Exception {
                        collector.doCollect();
                        return null;
                    }
                };
            }
        });
    }
}
