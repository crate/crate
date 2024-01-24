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

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.node.Node;
import org.elasticsearch.threadpool.ThreadPool;

import com.carrotsearch.hppc.IntArrayList;

import io.crate.common.exceptions.Exceptions;
import io.crate.data.BatchIterator;
import io.crate.data.CapturingRowConsumer;
import io.crate.data.Row;
import io.crate.execution.dsl.phases.RoutedCollectPhase;
import io.crate.execution.dsl.projection.Projections;
import io.crate.execution.engine.collect.collectors.RemoteCollector;
import io.crate.execution.engine.collect.collectors.ShardStateObserver;
import io.crate.execution.engine.collect.sources.ShardCollectorProviderFactory;
import io.crate.execution.jobs.TasksService;
import io.crate.execution.jobs.kill.KillJobsNodeAction;
import io.crate.execution.jobs.transport.JobAction;
import io.crate.metadata.Routing;
import io.crate.planner.distribution.DistributionInfo;

/**
 * Used to create RemoteCollectors
 * Those RemoteCollectors act as proxy collectors to collect data from a shard located on another node.
 */
@Singleton
public class RemoteCollectorFactory {

    private static final Logger LOGGER = LogManager.getLogger(RemoteCollectorFactory.class);
    private static final int SENDER_PHASE_ID = 0;

    private final ClusterService clusterService;
    private final TasksService tasksService;
    private final ElasticsearchClient elasticsearchClient;
    private final IndicesService indicesService;
    private final Executor searchTp;

    @Inject
    public RemoteCollectorFactory(ClusterService clusterService,
                                  TasksService tasksService,
                                  Node node,
                                  IndicesService indicesService,
                                  ThreadPool threadPool) {
        this.clusterService = clusterService;
        this.tasksService = tasksService;
        this.elasticsearchClient = node.client();
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
                                                                 ShardCollectorProviderFactory shardCollectorProviderFactory,
                                                                 boolean requiresScroll) {
        ShardStateObserver shardStateObserver = new ShardStateObserver(clusterService);
        return shardStateObserver.waitForActiveShard(shardId).thenCompose(primaryRouting -> localOrRemoteCollect(
            primaryRouting,
            collectPhase,
            collectTask,
            shardCollectorProviderFactory,
            requiresScroll
        ));
    }

    private CompletableFuture<BatchIterator<Row>> localOrRemoteCollect(
            ShardRouting primaryRouting,
            RoutedCollectPhase collectPhase,
            CollectTask collectTask,
            ShardCollectorProviderFactory collectorFactory,
            boolean requiresScroll) {
        String nodeId = primaryRouting.currentNodeId();
        String localNodeId = clusterService.localNode().getId();
        if (localNodeId.equalsIgnoreCase(nodeId)) {
            var indexShard = indicesService.indexServiceSafe(primaryRouting.index())
                .getShard(primaryRouting.shardId().id());
            var collectorProvider = collectorFactory.create(indexShard);
            try {
                return collectorProvider.awaitShardSearchActive().thenApply(
                    biFactory -> biFactory.getIterator(collectPhase, requiresScroll, collectTask));
            } catch (Exception e) {
                throw Exceptions.toRuntimeException(e);
            }
        } else {
            return remoteBatchIterator(primaryRouting, collectPhase, collectTask, requiresScroll);
        }
    }

    private CompletableFuture<BatchIterator<Row>> remoteBatchIterator(ShardRouting primaryRouting,
                                                                      RoutedCollectPhase collectPhase,
                                                                      CollectTask collectTask,
                                                                      boolean requiresScroll) {
        CapturingRowConsumer consumer = new CapturingRowConsumer(requiresScroll, collectTask.completionFuture());
        String remoteNodeId = primaryRouting.currentNodeId();
        String localNodeId = clusterService.localNode().getId();
        UUID childJobId = UUIDs.dirtyUUID();
        LOGGER.trace(
            "Creating child remote collect with id={} for parent job={}",
            childJobId,
            collectPhase.jobId()
        );
        RemoteCollector remoteCollector = new RemoteCollector(
            childJobId,
            collectTask.txnCtx().sessionSettings(),
            localNodeId,
            remoteNodeId,
            req -> elasticsearchClient.execute(JobAction.INSTANCE, req),
            req -> elasticsearchClient.execute(KillJobsNodeAction.INSTANCE, req),
            searchTp,
            tasksService,
            collectTask.getRamAccounting(),
            consumer,
            createRemoteCollectPhase(childJobId, collectPhase, primaryRouting.shardId(), remoteNodeId)
        );
        collectTask.completionFuture().exceptionally(err -> {
            remoteCollector.kill(err);
            consumer.capturedBatchIterator().whenComplete((bi, ignored) -> {
                if (bi != null) {
                    bi.kill(err);
                }
            });
            return null;
        });
        remoteCollector.doCollect();
        return consumer.capturedBatchIterator();
    }

    private static RoutedCollectPhase createRemoteCollectPhase(UUID childJobId,
                                                               RoutedCollectPhase collectPhase,
                                                               ShardId shardId,
                                                               String nodeId) {

        Routing routing = new Routing(
            Map.of(
                nodeId, Map.of(shardId.getIndexName(), IntArrayList.from(shardId.id()))
            )
        );
        return new RoutedCollectPhase(
            childJobId,
            SENDER_PHASE_ID,
            collectPhase.name(),
            routing,
            collectPhase.maxRowGranularity(),
            collectPhase.toCollect(),
            Projections.shardProjections(collectPhase.projections()),
            collectPhase.where(),
            DistributionInfo.DEFAULT_BROADCAST
        );
    }
}
