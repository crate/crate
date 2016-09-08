/*
 * Licensed to Crate.IO GmbH ("Crate") under one or more contributor
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

package io.crate.operation.collect.sources;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import io.crate.action.job.SharedShardContext;
import io.crate.action.job.SharedShardContexts;
import io.crate.analyze.EvaluatingNormalizer;
import io.crate.analyze.OrderBy;
import io.crate.core.collections.Buckets;
import io.crate.core.collections.Row;
import io.crate.exceptions.UnhandledServerException;
import io.crate.executor.transport.TransportActionProvider;
import io.crate.metadata.Functions;
import io.crate.metadata.PartitionName;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.shard.unassigned.UnassignedShard;
import io.crate.operation.ImplementationSymbolVisitor;
import io.crate.operation.RowDownstream;
import io.crate.operation.collect.*;
import io.crate.operation.collect.collectors.CompositeCollector;
import io.crate.operation.collect.collectors.MultiShardScoreDocCollector;
import io.crate.operation.collect.collectors.OrderedDocCollector;
import io.crate.operation.projectors.*;
import io.crate.operation.projectors.sorting.OrderingByPosition;
import io.crate.operation.reference.sys.node.local.NodeSysExpression;
import io.crate.operation.reference.sys.node.local.NodeSysReferenceResolver;
import io.crate.planner.consumer.OrderByPositionVisitor;
import io.crate.planner.node.dql.CollectPhase;
import io.crate.planner.node.dql.RoutedCollectPhase;
import io.crate.planner.projection.Projection;
import org.elasticsearch.action.bulk.BulkRetryCoordinatorPool;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IllegalIndexShardStateException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardNotFoundException;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.*;
import java.util.concurrent.ExecutorService;

/**
 * Factory to create collectors which collect data from shards.
 *
 *
 * There are different patterns on how shards are collected:
 *
 * - Multiple Collectors with shard level projectors (only unordered):
 *
 *      C/S1   C/S2
 *       |      |
 *       P      P  < i/o & computation should happen here to benefit from threading
 *       \     /
 *        \   /
 *        Merger
 *          |
 *      RowReceiver
 *
 *
 * - Ordered with one Collector that has 1+ child shard collectors
 *   This single collector is switching between the child-collectors to provide a correct sorted result
 *
 *     +---------------------+
 *     | MultiShardCollector |
 *     |   S1   S2           |
 *     +---------------------+
 *             |
 *          RowReceiver
 *
 */
@Singleton
public class ShardCollectSource implements CollectSource {

    private static final ESLogger LOGGER = Loggers.getLogger(ShardCollectSource.class);
    private final Settings settings;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final IndicesService indicesService;
    private final Functions functions;
    private final ClusterService clusterService;
    private final ThreadPool threadPool;
    private final RemoteCollectorFactory remoteCollectorFactory;
    private final SystemCollectSource systemCollectSource;
    private final TransportActionProvider transportActionProvider;
    private final BulkRetryCoordinatorPool bulkRetryCoordinatorPool;
    private final NodeSysExpression nodeSysExpression;
    private final ListeningExecutorService executor;

    @Inject
    public ShardCollectSource(Settings settings,
                              IndexNameExpressionResolver indexNameExpressionResolver,
                              IndicesService indicesService,
                              Functions functions,
                              ClusterService clusterService,
                              ThreadPool threadPool,
                              TransportActionProvider transportActionProvider,
                              BulkRetryCoordinatorPool bulkRetryCoordinatorPool,
                              RemoteCollectorFactory remoteCollectorFactory,
                              SystemCollectSource systemCollectSource,
                              NodeSysExpression nodeSysExpression) {
        this.settings = settings;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.indicesService = indicesService;
        this.functions = functions;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.remoteCollectorFactory = remoteCollectorFactory;
        this.systemCollectSource = systemCollectSource;
        this.executor = MoreExecutors.listeningDecorator((ExecutorService) threadPool.executor(ThreadPool.Names.SEARCH));
        this.transportActionProvider = transportActionProvider;
        this.bulkRetryCoordinatorPool = bulkRetryCoordinatorPool;
        this.nodeSysExpression = nodeSysExpression;
    }

    @Override
    public Collection<CrateCollector> getCollectors(CollectPhase phase, RowReceiver downstream, JobCollectContext jobCollectContext) {
        RoutedCollectPhase collectPhase = (RoutedCollectPhase) phase;
        NodeSysReferenceResolver referenceResolver = new NodeSysReferenceResolver(nodeSysExpression);
        ImplementationSymbolVisitor implementationSymbolVisitor = new ImplementationSymbolVisitor(functions);
        EvaluatingNormalizer nodeNormalizer = new EvaluatingNormalizer(functions,
                RowGranularity.DOC,
                referenceResolver);
        RoutedCollectPhase normalizedPhase = collectPhase.normalize(nodeNormalizer, null);

        ProjectorFactory projectorFactory = new ProjectionToProjectorVisitor(
                clusterService,
                functions,
                indexNameExpressionResolver,
                threadPool,
                settings,
                transportActionProvider,
                bulkRetryCoordinatorPool,
                implementationSymbolVisitor,
                nodeNormalizer
        );
        String localNodeId = clusterService.localNode().id();

        if (normalizedPhase.maxRowGranularity() == RowGranularity.SHARD) {
            // it's possible to use FlatProjectorChain instead of ShardProjectorChain as a shortcut because
            // the rows are "pre-created" on a shard level.
            // The getShardsCollector method always only uses a single RowReceiver and not one per shard)
            FlatProjectorChain flatProjectorChain = FlatProjectorChain.withAttachedDownstream(
                projectorFactory,
                jobCollectContext.queryPhaseRamAccountingContext(),
                normalizedPhase.projections(),
                downstream,
                collectPhase.jobId());
            return Collections.singletonList(
                getShardsCollector(collectPhase, normalizedPhase, localNodeId, flatProjectorChain));
        }

        OrderBy orderBy = normalizedPhase.orderBy();
        if (normalizedPhase.maxRowGranularity() == RowGranularity.DOC && orderBy != null) {
            FlatProjectorChain flatProjectorChain = FlatProjectorChain.withAttachedDownstream(
                projectorFactory,
                jobCollectContext.queryPhaseRamAccountingContext(),
                normalizedPhase.projections(),
                downstream,
                collectPhase.jobId()
            );
            return ImmutableList.of(createMultiShardScoreDocCollector(
                    normalizedPhase,
                    flatProjectorChain,
                    jobCollectContext,
                    localNodeId)
            );
        }

        // actual shards might be less if table is partitioned and a partition has been deleted meanwhile
        final int maxNumShards = normalizedPhase.routing().numShards(localNodeId);
        RowDownstream.Factory rowDownstreamFactory;
        CompositeCollector.Builder builder = null;
        boolean hasAnyShardProjections = false;
        for (Projection projection : normalizedPhase.projections()) {
            if (projection.requiredGranularity().ordinal() >= RowGranularity.SHARD.ordinal()) {
                hasAnyShardProjections = true;
                break;
            }
        }
        if (hasAnyShardProjections) {
            rowDownstreamFactory = new RowDownstream.Factory() {
                @Override
                public RowDownstream create(RowReceiver rowReceiver) {
                    if (maxNumShards == 1) {
                        LOGGER.debug(
                            "Getting RowDownstream for 1 upstream, repeat support: " + rowReceiver.requirements());
                        return new SingleUpstreamRowDownstream(rowReceiver);
                    }
                    LOGGER.debug("Getting RowDownstream for multiple upstreams; unsorted; repeat support: "
                                 + rowReceiver.requirements());
                    return RowMergers.passThroughRowMerger(rowReceiver);
                }
            };
        } else {
            builder = new CompositeCollector.Builder();
            rowDownstreamFactory = builder.rowDownstreamFactory();
        }

        ShardProjectorChain projectorChain = new ShardProjectorChain(
            normalizedPhase.jobId(),
            normalizedPhase.projections(),
            rowDownstreamFactory,
            downstream,
            projectorFactory,
            jobCollectContext.queryPhaseRamAccountingContext());

        Map<String, Map<String, List<Integer>>> locations = normalizedPhase.routing().locations();
        final List<CrateCollector> shardCollectors = new ArrayList<>(maxNumShards);

        Map<String, List<Integer>> indexShards = locations.get(localNodeId);
        if (indexShards != null) {
            shardCollectors.addAll(
                    getDocCollectors(jobCollectContext, normalizedPhase, projectorChain, indexShards));
        }
        projectorChain.prepare();
        if (builder == null) {
            return shardCollectors;
        }
        CrateCollector collector;
        if (shardCollectors.isEmpty()) {
            collector = RowsCollector.empty(projectorChain.newShardDownstreamProjector(projectorFactory));
        } else {
            collector = builder.build(shardCollectors);
        }
        return Collections.singletonList(collector);
    }

    private CrateCollector createMultiShardScoreDocCollector(RoutedCollectPhase collectPhase,
                                                             FlatProjectorChain flatProjectorChain,
                                                             JobCollectContext jobCollectContext,
                                                             String localNodeId) {

        Map<String, Map<String, List<Integer>>> locations = collectPhase.routing().locations();
        SharedShardContexts sharedShardContexts = jobCollectContext.sharedShardContexts();
        Map<String, List<Integer>> indexShards = locations.get(localNodeId);
        List<OrderedDocCollector> orderedDocCollectors = new ArrayList<>();
        for (Map.Entry<String, List<Integer>> entry : indexShards.entrySet()) {
            String indexName = entry.getKey();

            for (Integer shardId : entry.getValue()) {
                SharedShardContext context = sharedShardContexts.getOrCreateContext(new ShardId(indexName, shardId));

                try {
                    Injector shardInjector = context.indexService().shardInjectorSafe(shardId);
                    ShardCollectService shardCollectService = shardInjector.getInstance(ShardCollectService.class);
                    orderedDocCollectors.add(shardCollectService.getOrderedCollector(collectPhase,
                        context,
                        jobCollectContext,
                        flatProjectorChain.firstProjector().requirements().contains(Requirement.REPEAT)));
                } catch (ShardNotFoundException | IllegalIndexShardStateException e) {
                    throw e;
                } catch (IndexNotFoundException e) {
                    if (PartitionName.isPartition(indexName)) {
                        break;
                    }
                    throw e;
                } catch (Throwable t) {
                    throw new UnhandledServerException(t);
                }
            }
        }

        OrderBy orderBy = collectPhase.orderBy();
        assert orderBy != null;
        return new MultiShardScoreDocCollector(
                orderedDocCollectors,
                OrderingByPosition.rowOrdering(
                        OrderByPositionVisitor.orderByPositions(orderBy.orderBySymbols(), collectPhase.toCollect()),
                        orderBy.reverseFlags(),
                        orderBy.nullsFirst()
                ),
                flatProjectorChain,
                executor
        );
    }

    private Collection<CrateCollector> getDocCollectors(JobCollectContext jobCollectContext,
                                                        RoutedCollectPhase collectPhase,
                                                        ShardProjectorChain projectorChain,
                                                        Map<String, List<Integer>> indexShards) {

        List<CrateCollector> crateCollectors = new ArrayList<>();
        for (Map.Entry<String, List<Integer>> entry : indexShards.entrySet()) {
            String indexName = entry.getKey();
            IndexService indexService;
            try {
                indexService = indicesService.indexServiceSafe(indexName);
            } catch (IndexNotFoundException e) {
                if (PartitionName.isPartition(indexName)) {
                    continue;
                }
                throw e;
            }

            for (Integer shardId : entry.getValue()) {
                Injector shardInjector;
                try {
                    shardInjector = indexService.shardInjectorSafe(shardId);
                    ShardCollectService shardCollectService = shardInjector.getInstance(ShardCollectService.class);
                    CrateCollector collector = shardCollectService.getDocCollector(
                        collectPhase,
                        projectorChain,
                        jobCollectContext
                    );
                    crateCollectors.add(collector);
                } catch (ShardNotFoundException | IllegalIndexShardStateException e) {
                    crateCollectors.add(remoteCollectorFactory.createCollector(
                        indexName, shardId, collectPhase, projectorChain, jobCollectContext.queryPhaseRamAccountingContext()));
                } catch (InterruptedException e) {
                    projectorChain.fail(e);
                    throw Throwables.propagate(e);
                } catch (Throwable t) {
                    projectorChain.fail(t);
                    throw new UnhandledServerException(t);
                }
            }
        }
        return crateCollectors;
    }

    private CrateCollector getShardsCollector(RoutedCollectPhase collectPhase,
                                              RoutedCollectPhase normalizedPhase,
                                              String localNodeId,
                                              FlatProjectorChain flatProjectorChain) {
        Map<String, Map<String, List<Integer>>> locations = collectPhase.routing().locations();
        List<UnassignedShard> unassignedShards = new ArrayList<>();
        List<Object[]> rows = new ArrayList<>();
        Map<String, List<Integer>> indexShardsMap = locations.get(localNodeId);

        for (Map.Entry<String, List<Integer>> indexShards : indexShardsMap.entrySet()) {
            String indexName = indexShards.getKey();
            List<Integer> shards = indexShards.getValue();
            IndexService indexService = indicesService.indexService(indexName);
            if (indexService == null) {
                for (Integer shard : shards) {
                    unassignedShards.add(toUnassignedShard(new ShardId(indexName, UnassignedShard.markAssigned(shard))));
                }
                continue;
            }
            for (Integer shard : shards) {
                if (UnassignedShard.isUnassigned(shard)) {
                    unassignedShards.add(toUnassignedShard(new ShardId(indexName, UnassignedShard.markAssigned(shard))));
                    continue;
                }
                try {
                    ShardCollectService shardCollectService =
                            indexService.shardInjectorSafe(shard).getInstance(ShardCollectService.class);

                    Object[] row = shardCollectService.getRowForShard(normalizedPhase);
                    if (row != null) {
                        rows.add(row);
                    }
                } catch (ShardNotFoundException | IllegalIndexShardStateException e) {
                    unassignedShards.add(toUnassignedShard(new ShardId(indexName, shard)));
                } catch (Throwable t) {
                    throw new UnhandledServerException(t);
                }
            }
        }
        if (!unassignedShards.isEmpty()) {
            // since unassigned shards aren't really on any node we use the collectPhase which is NOT normalized here
            // because otherwise if _node was also selected it would contain something which is wrong
            for (Object[] objects : Iterables.transform(
                    systemCollectSource.toRowsIterable(collectPhase, unassignedShards, false), Row.MATERIALIZE)) {
                rows.add(objects);
            }
        }

        if (collectPhase.orderBy() != null) {
            Collections.sort(rows, OrderingByPosition.arrayOrdering(collectPhase).reverse());
        }
        return new RowsCollector(
            flatProjectorChain.firstProjector(),
            Iterables.transform(rows, Buckets.arrayToRowFunction()));
    }

    private UnassignedShard toUnassignedShard(ShardId shardId) {
        return new UnassignedShard(shardId, clusterService, false, ShardRoutingState.UNASSIGNED);
    }
}
