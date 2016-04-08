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
import io.crate.exceptions.TableUnknownException;
import io.crate.exceptions.UnhandledServerException;
import io.crate.executor.transport.TransportActionProvider;
import io.crate.metadata.Functions;
import io.crate.metadata.PartitionName;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.shard.unassigned.UnassignedShard;
import io.crate.operation.ImplementationSymbolVisitor;
import io.crate.operation.collect.CrateCollector;
import io.crate.operation.collect.JobCollectContext;
import io.crate.operation.collect.RowsCollector;
import io.crate.operation.collect.ShardCollectService;
import io.crate.operation.collect.collectors.MultiShardScoreDocCollector;
import io.crate.operation.collect.collectors.OrderedDocCollector;
import io.crate.operation.projectors.*;
import io.crate.operation.projectors.sorting.OrderingByPosition;
import io.crate.operation.reference.sys.node.NodeSysExpression;
import io.crate.operation.reference.sys.node.NodeSysReferenceResolver;
import io.crate.planner.consumer.OrderByPositionVisitor;
import io.crate.planner.node.dql.CollectPhase;
import io.crate.planner.node.dql.RoutedCollectPhase;
import org.elasticsearch.action.bulk.BulkRetryCoordinatorPool;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IllegalIndexShardStateException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardNotFoundException;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.*;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutorService;

@Singleton
public class ShardCollectSource implements CollectSource {

    private final Settings settings;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final IndicesService indicesService;
    private final Functions functions;
    private final ClusterService clusterService;
    private final ThreadPool threadPool;
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
                              SystemCollectSource systemCollectSource,
                              NodeSysExpression nodeSysExpression) {
        this.settings = settings;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.indicesService = indicesService;
        this.functions = functions;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
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
                RowGranularity.NODE,
                referenceResolver);
        RoutedCollectPhase normalizedPhase = collectPhase.normalize(nodeNormalizer);

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
        OrderBy orderBy = normalizedPhase.orderBy();
        if (normalizedPhase.maxRowGranularity() == RowGranularity.DOC && orderBy != null) {
            FlatProjectorChain flatProjectorChain;
            if (normalizedPhase.hasProjections()) {
                flatProjectorChain = FlatProjectorChain.withAttachedDownstream(
                        projectorFactory,
                        jobCollectContext.queryPhaseRamAccountingContext(),
                        normalizedPhase.projections(),
                        downstream,
                        collectPhase.jobId()
                );
            } else {
                flatProjectorChain = FlatProjectorChain.withReceivers(ImmutableList.of(downstream));
            }
            return ImmutableList.of(createMultiShardScoreDocCollector(
                    normalizedPhase,
                    flatProjectorChain,
                    jobCollectContext,
                    localNodeId)
            );
        }


        // actual shards might be less if table is partitioned and a partition has been deleted meanwhile
        int maxNumShards = normalizedPhase.routing().numShards(localNodeId);

        ShardProjectorChain projectorChain = ShardProjectorChain.passThroughMerge(
                normalizedPhase.jobId(),
                maxNumShards,
                normalizedPhase.projections(),
                downstream,
                projectorFactory,
                jobCollectContext.queryPhaseRamAccountingContext());

        Map<String, Map<String, List<Integer>>> locations = normalizedPhase.routing().locations();
        final List<CrateCollector> shardCollectors = new ArrayList<>(maxNumShards);

        if (normalizedPhase.maxRowGranularity() == RowGranularity.SHARD) {
            shardCollectors.add(
                    getShardsCollector(collectPhase, normalizedPhase, projectorFactory, localNodeId, projectorChain));
        } else {
            Map<String, List<Integer>> indexShards = locations.get(localNodeId);
            if (indexShards != null) {
                shardCollectors.addAll(
                        getDocCollectors(jobCollectContext, normalizedPhase, projectorChain, indexShards));
            }
        }
        projectorChain.prepare(jobCollectContext);
        return shardCollectors;
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
                    orderedDocCollectors.add(shardCollectService.getOrderedCollector(collectPhase, context, jobCollectContext));
                } catch (ShardNotFoundException | CancellationException | IllegalIndexShardStateException e) {
                    throw e;
                } catch (IndexNotFoundException e) {
                    if (PartitionName.isPartition(indexName)) {
                        break;
                    }
                    throw new TableUnknownException(indexName, e);
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
                throw new TableUnknownException(entry.getKey(), e);
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
                } catch (ShardNotFoundException | CancellationException | IllegalIndexShardStateException e) {
                    projectorChain.fail(e);
                    throw e;
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
                                              ProjectorFactory projectorFactory,
                                              String localNodeId,
                                              ShardProjectorChain projectorChain) {
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
                    projectorChain.fail(t);
                    throw new UnhandledServerException(t);
                }
            }
        }
        if (!unassignedShards.isEmpty()) {
            // since unassigned shards aren't really on any node we use the collectPhase which is NOT normalized here
            // because otherwise if _node was also selected it would contain something which is wrong
            for (Object[] objects : Iterables.transform(
                    systemCollectSource.toRowsIterable(collectPhase, unassignedShards), Row.MATERIALIZE)) {
                rows.add(objects);
            }
        }

        if (collectPhase.orderBy() != null) {
            Collections.sort(rows, OrderingByPosition.arrayOrdering(collectPhase).reverse());
        }
        return new RowsCollector(
                projectorChain.newShardDownstreamProjector(projectorFactory),
                Iterables.transform(rows, Buckets.arrayToRowFunction()));
    }

    private UnassignedShard toUnassignedShard(ShardId shardId) {
        return new UnassignedShard(shardId, clusterService, false, ShardRoutingState.UNASSIGNED);
    }
}
