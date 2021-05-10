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

import com.carrotsearch.hppc.IntArrayList;

import io.crate.common.collections.Lists2;
import io.crate.data.BatchIterator;
import io.crate.data.Buckets;
import io.crate.data.CollectingBatchIterator;
import io.crate.data.CollectingRowConsumer;
import io.crate.data.Row;
import io.crate.exceptions.Exceptions;
import io.crate.execution.TransportActionProvider;
import io.crate.execution.dsl.phases.RoutedCollectPhase;
import io.crate.execution.dsl.projection.Projections;
import io.crate.execution.engine.collect.collectors.RemoteCollector;
import io.crate.execution.engine.collect.collectors.ShardStateObserver;
import io.crate.execution.engine.collect.sources.ShardCollectorProviderFactory;
import io.crate.execution.jobs.TasksService;
import io.crate.metadata.Routing;
import io.crate.planner.distribution.DistributionInfo;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.stream.Collector;
import java.util.stream.Collectors;

/**
 * Used to create RemoteCollectors
 * Those RemoteCollectors act as proxy collectors to collect data from a shard located on another node.
 */
@Singleton
public class RemoteCollectorFactory {

    private static final int SENDER_PHASE_ID = 0;

    private final ClusterService clusterService;
    private final TasksService tasksService;
    private final TransportActionProvider transportActionProvider;
    private final IndicesService indicesService;
    private final Executor searchTp;

    @Inject
    public RemoteCollectorFactory(ClusterService clusterService,
                                  TasksService tasksService,
                                  TransportActionProvider transportActionProvider,
                                  IndicesService indicesService,
                                  ThreadPool threadPool) {
        this.clusterService = clusterService;
        this.tasksService = tasksService;
        this.transportActionProvider = transportActionProvider;
        this.indicesService = indicesService;
        searchTp = threadPool.executor(ThreadPool.Names.SEARCH);
    }

    /**
     * create a RemoteCollector
     * The RemoteCollector will collect data from another node using a wormhole as if it was collecting on this node.
     * <p>
     * This should only be used if a shard is not available on the current node due to a relocation
     */
    public CompletableFuture<BatchIterator<Row>> createCollector(ShardId shardId,
                                                                 RoutedCollectPhase collectPhase,
                                                                 CollectTask collectTask,
                                                                 ShardCollectorProviderFactory shardCollectorProviderFactory) {
        ShardStateObserver shardStateObserver = new ShardStateObserver(clusterService);
        CompletableFuture<ShardRouting> shardBecameActive = shardStateObserver.waitForActiveShard(shardId);
        Runnable onClose = () -> {};
        Consumer<Throwable> kill = killReason -> {
            shardBecameActive.cancel(true);
            shardBecameActive.completeExceptionally(killReason);
        };
        return shardBecameActive.thenApply(activePrimaryRouting ->
            CollectingBatchIterator.newInstance(
                onClose,
                kill,
                () -> retrieveRows(activePrimaryRouting, collectPhase, collectTask, shardCollectorProviderFactory),
                true
            )
        );
    }

    private CompletableFuture<List<Row>> retrieveRows(ShardRouting activePrimaryRouting,
                                                      RoutedCollectPhase collectPhase,
                                                      CollectTask collectTask,
                                                      ShardCollectorProviderFactory shardCollectorProviderFactory) {
        Collector<Row, ?, List<Object[]>> listCollector = Collectors.mapping(Row::materialize, Collectors.toList());
        CollectingRowConsumer<?, List<Object[]>> consumer = new CollectingRowConsumer<>(listCollector);
        String nodeId = activePrimaryRouting.currentNodeId();
        String localNodeId = clusterService.localNode().getId();
        if (localNodeId.equalsIgnoreCase(nodeId)) {
            var indexShard = indicesService.indexServiceSafe(activePrimaryRouting.index())
                .getShard(activePrimaryRouting.shardId().id());
            var collectorProvider = shardCollectorProviderFactory.create(indexShard);
            CompletableFuture<BatchIterator<Row>> it;
            try {
                it = collectorProvider.getFutureIterator(collectPhase, consumer.requiresScroll(), collectTask);
            } catch (Exception e) {
                return Exceptions.rethrowRuntimeException(e);
            }
            it.whenComplete(consumer);
        } else {
            UUID childJobId = UUID.randomUUID();
            RemoteCollector remoteCollector = new RemoteCollector(
                childJobId,
                collectTask.txnCtx().sessionSettings(),
                localNodeId,
                nodeId,
                transportActionProvider.transportJobInitAction(),
                transportActionProvider.transportKillJobsNodeAction(),
                searchTp,
                tasksService,
                collectTask.getRamAccounting(),
                consumer,
                createRemoteCollectPhase(childJobId, collectPhase, activePrimaryRouting.shardId(), nodeId)
            );
            remoteCollector.doCollect();
        }
        return consumer
            .completionFuture()
            .thenApply(rows -> Lists2.mapLazy(rows, Buckets.arrayToSharedRow()));
    }

    private static RoutedCollectPhase createRemoteCollectPhase(UUID childJobId,
                                                               RoutedCollectPhase collectPhase,
                                                               ShardId shardId,
                                                               String nodeId) {

        Routing routing = new Routing(
            Map.of(
                nodeId, Map.of(shardId.getIndexName(), IntArrayList.from(shardId.getId()))
            )
        );
        return new RoutedCollectPhase(
            childJobId,
            SENDER_PHASE_ID,
            collectPhase.name(),
            routing,
            collectPhase.maxRowGranularity(),
            collectPhase.toCollect(),
            new ArrayList<>(Projections.shardProjections(collectPhase.projections())),
            collectPhase.where(),
            DistributionInfo.DEFAULT_BROADCAST
        );
    }
}
