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
import io.crate.analyze.symbol.Symbols;
import io.crate.blob.v2.BlobIndicesService;
import io.crate.blob.v2.BlobShard;
import io.crate.data.*;
import io.crate.exceptions.UnhandledServerException;
import io.crate.executor.transport.TransportActionProvider;
import io.crate.lucene.LuceneQueryBuilder;
import io.crate.metadata.*;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.metadata.shard.unassigned.UnassignedShard;
import io.crate.operation.InputFactory;
import io.crate.operation.collect.*;
import io.crate.operation.collect.collectors.CompositeCollector;
import io.crate.operation.collect.collectors.OrderedDocCollector;
import io.crate.operation.collect.collectors.OrderedLuceneBatchIteratorFactory;
import io.crate.operation.projectors.*;
import io.crate.operation.projectors.sorting.OrderingByPosition;
import io.crate.operation.reference.sys.node.local.NodeSysExpression;
import io.crate.operation.reference.sys.node.local.NodeSysReferenceResolver;
import io.crate.planner.consumer.OrderByPositionVisitor;
import io.crate.planner.node.dql.CollectPhase;
import io.crate.planner.node.dql.RoutedCollectPhase;
import io.crate.planner.projection.Projections;
import org.elasticsearch.action.bulk.BulkRetryCoordinatorPool;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IllegalIndexShardStateException;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardNotFoundException;
import org.elasticsearch.indices.IndicesLifecycle;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

import static io.crate.blob.v2.BlobIndex.isBlobIndex;

/**
 * Factory to create collectors which collect data from shards.
 * <p>
 * <p>
 * There are different patterns on how shards are collected:
 * <p>
 * - Multiple Collectors with shard level projectors (only unordered):
 * <p>
 * C/S1   C/S2
 * |      |
 * P      P  < i/o & computation should happen here to benefit from threading
 * \     /
 * \   /
 * Merger
 * |
 * RowReceiver
 * <p>
 * <p>
 * - Ordered with one Collector that has 1+ child shard collectors
 * This single collector is switching between the child-collectors to provide a correct sorted result
 * <p>
 * +---------------------+
 * | MultiShardCollector |
 * |   S1   S2           |
 * +---------------------+
 * |
 * RowReceiver
 */
@Singleton
public class ShardCollectSource extends AbstractComponent implements CollectSource {

    private final Schemas schemas;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final IndicesService indicesService;
    private final ClusterService clusterService;
    private final ThreadPool threadPool;
    private final TransportActionProvider transportActionProvider;
    private final BulkRetryCoordinatorPool bulkRetryCoordinatorPool;
    private final RemoteCollectorFactory remoteCollectorFactory;
    private final SystemCollectSource systemCollectSource;
    private final ListeningExecutorService executor;
    private final EvaluatingNormalizer nodeNormalizer;
    private final ProjectorFactory sharedProjectorFactory;
    private final BlobIndicesService blobIndicesService;

    private final Map<ShardId, ShardCollectorProvider> shards = new ConcurrentHashMap<>();
    private final Functions functions;
    private final LuceneQueryBuilder luceneQueryBuilder;


    @Inject
    public ShardCollectSource(Settings settings,
                              Schemas schemas,
                              IndexNameExpressionResolver indexNameExpressionResolver,
                              IndicesService indicesService,
                              Functions functions,
                              ClusterService clusterService,
                              LuceneQueryBuilder luceneQueryBuilder,
                              ThreadPool threadPool,
                              TransportActionProvider transportActionProvider,
                              BulkRetryCoordinatorPool bulkRetryCoordinatorPool,
                              RemoteCollectorFactory remoteCollectorFactory,
                              SystemCollectSource systemCollectSource,
                              NodeSysExpression nodeSysExpression,
                              IndicesLifecycle indicesLifecycle,
                              BlobIndicesService blobIndicesService) {
        super(settings);
        this.luceneQueryBuilder = luceneQueryBuilder;
        this.schemas = schemas;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.indicesService = indicesService;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.transportActionProvider = transportActionProvider;
        this.bulkRetryCoordinatorPool = bulkRetryCoordinatorPool;
        this.remoteCollectorFactory = remoteCollectorFactory;
        this.systemCollectSource = systemCollectSource;
        this.executor = MoreExecutors.listeningDecorator((ExecutorService) threadPool.executor(ThreadPool.Names.SEARCH));
        this.blobIndicesService = blobIndicesService;
        this.functions = functions;
        NodeSysReferenceResolver referenceResolver = new NodeSysReferenceResolver(nodeSysExpression);
        nodeNormalizer = new EvaluatingNormalizer(
            functions,
            RowGranularity.DOC,
            ReplaceMode.COPY,
            referenceResolver,
            null);

        sharedProjectorFactory = new ProjectionToProjectorVisitor(
            clusterService,
            functions,
            indexNameExpressionResolver,
            threadPool,
            settings,
            transportActionProvider,
            bulkRetryCoordinatorPool,
            new InputFactory(functions),
            nodeNormalizer,
            systemCollectSource::getRowUpdater
        );

        indicesLifecycle.addListener(new LifecycleListener());
    }

    private class LifecycleListener extends IndicesLifecycle.Listener {

        @Override
        public void afterIndexShardCreated(IndexShard indexShard) {
            logger.debug("creating shard in {} {} {}", ShardCollectSource.this, indexShard.shardId(), shards.size());
            assert !shards.containsKey(indexShard.shardId()) : "shard entry already exists upon add";
            ShardCollectorProvider provider;
            if (isBlobIndex(indexShard.shardId().getIndex())) {
                BlobShard blobShard = blobIndicesService.blobShardSafe(indexShard.shardId());
                provider = new BlobShardCollectorProvider(blobShard, clusterService, functions,
                    indexNameExpressionResolver, threadPool, settings, transportActionProvider, bulkRetryCoordinatorPool);
            } else {
                provider = new LuceneShardCollectorProvider(
                    schemas, luceneQueryBuilder, clusterService, functions, indexNameExpressionResolver, threadPool,
                    settings, transportActionProvider, bulkRetryCoordinatorPool, indexShard);
            }
            shards.put(indexShard.shardId(), provider);

        }

        @Override
        public void beforeIndexShardClosed(ShardId shardId, @Nullable IndexShard indexShard, Settings indexSettings) {
            logger.debug("removing shard upon close in {} shard={} numShards={}", ShardCollectSource.this, shardId, shards.size());
            assert shards.containsKey(shardId) : "shard entry missing upon close";
            shards.remove(shardId);
        }

        @Override
        public void beforeIndexShardDeleted(ShardId shardId, Settings indexSettings) {
            if (shards.remove(shardId) != null) {
                logger.debug("removed shard upon delete in {} shard={} remainingShards={}", ShardCollectSource.this, shardId, shards.size());
            } else {
                logger.debug("shard not found upon delete in {} shard={} remainingShards={}", ShardCollectSource.this, shardId, shards.size());
            }
        }
    }


    @Override
    public Collection<CrateCollector> getCollectors(CollectPhase phase,
                                                    RowReceiver lastRR,
                                                    JobCollectContext jobCollectContext) {
        RoutedCollectPhase collectPhase = (RoutedCollectPhase) phase;
        RoutedCollectPhase normalizedPhase = collectPhase.normalize(nodeNormalizer, null);

        String localNodeId = clusterService.localNode().getId();


        RowReceiver firstNodeRR = ProjectorChain.prependProjectors(
            lastRR,
            Projections.nodeProjections(normalizedPhase.projections()),
            collectPhase.jobId(),
            jobCollectContext.queryPhaseRamAccountingContext(),
            sharedProjectorFactory
        );
        if (normalizedPhase.maxRowGranularity() == RowGranularity.SHARD) {
            // it's possible to use FlatProjectorChain instead of ShardProjectorChain as a shortcut because
            // the rows are "pre-created" on a shard level.
            // The getShardsCollector method always only uses a single RowReceiver and not one per shard)
            return Collections.singletonList(
                getShardsCollector(collectPhase, normalizedPhase, localNodeId, firstNodeRR));
        }
        OrderBy orderBy = normalizedPhase.orderBy();
        if (normalizedPhase.maxRowGranularity() == RowGranularity.DOC && orderBy != null) {
            return ImmutableList.of(createMultiShardScoreDocCollector(
                normalizedPhase,
                firstNodeRR,
                jobCollectContext,
                localNodeId)
            );
        }

        // actual shards might be less if table is partitioned and a partition has been deleted meanwhile
        final int maxNumShards = normalizedPhase.routing().numShards(localNodeId);
        boolean hasShardProjections = Projections.hasAnyShardProjections(normalizedPhase.projections());
        Map<String, Map<String, List<Integer>>> locations = normalizedPhase.routing().locations();
        final List<CrateCollector.Builder> builders = new ArrayList<>(maxNumShards);

        Map<String, List<Integer>> indexShards = locations.get(localNodeId);
        if (indexShards != null) {
            builders.addAll(
                getDocCollectors(jobCollectContext, normalizedPhase, lastRR.requirements(), indexShards));
        }

        switch (builders.size()) {
            case 0:
                return Collections.singletonList(RowsCollector.empty(firstNodeRR));
            case 1:
                CrateCollector.Builder collectorBuilder = builders.iterator().next();
                return Collections.singletonList(collectorBuilder.build(
                    collectorBuilder.applyProjections(new BatchConsumerToRowReceiver(firstNodeRR)), firstNodeRR));
            default:
                if (hasShardProjections) {
                    // 1 Collector per shard to benefit from concurrency (each collector is run in a thread)
                    // It doesn't support repeat.

                    BatchConsumerToRowReceiver finalConsumer = new BatchConsumerToRowReceiver(firstNodeRR);
                    BatchConsumer multiConsumer = CompositeCollector.newMultiConsumer(
                        builders.size(),
                        finalConsumer,
                        iterators -> new AsyncCompositeBatchIterator(executor, iterators)
                    );
                    List<CrateCollector> collectors = new ArrayList<>(builders.size());
                    for (CrateCollector.Builder builder : builders) {
                        collectors.add(builder.build(builder.applyProjections(multiConsumer), firstNodeRR));
                    }
                    return collectors;
                } else {
                    // If there are no shard-projections there is no real benefit from concurrency gained by using multiple collectors.
                    // CompositeCollector to collects single-threaded sequentially.
                    return Collections.singletonList(
                        CompositeCollector.syncCompositeCollector(
                            builders, new BatchConsumerToRowReceiver(firstNodeRR), firstNodeRR));
                }
        }
    }

    private CrateCollector createMultiShardScoreDocCollector(RoutedCollectPhase collectPhase,
                                                             RowReceiver rowReceiver,
                                                             JobCollectContext jobCollectContext,
                                                             String localNodeId) {

        Map<String, Map<String, List<Integer>>> locations = collectPhase.routing().locations();
        SharedShardContexts sharedShardContexts = jobCollectContext.sharedShardContexts();
        Map<String, List<Integer>> indexShards = locations.get(localNodeId);
        List<OrderedDocCollector> orderedDocCollectors = new ArrayList<>();
        for (Map.Entry<String, List<Integer>> entry : indexShards.entrySet()) {
            String indexName = entry.getKey();

            for (Integer shardNum : entry.getValue()) {
                ShardId shardId = new ShardId(indexName, shardNum);
                SharedShardContext context = sharedShardContexts.getOrCreateContext(shardId);

                try {
                    ShardCollectorProvider shardCollectorProvider = getCollectorProviderSafe(shardId);
                    orderedDocCollectors.add(shardCollectorProvider.getOrderedCollector(collectPhase,
                        context,
                        jobCollectContext,
                        rowReceiver.requirements().contains(Requirement.REPEAT)));
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
        assert orderBy != null : "orderBy must not be null";
        return BatchIteratorCollectorBridge.newInstance(
            OrderedLuceneBatchIteratorFactory.newInstance(
                orderedDocCollectors,
                collectPhase.toCollect().size(),
                OrderingByPosition.rowOrdering(
                    OrderByPositionVisitor.orderByPositions(orderBy.orderBySymbols(), collectPhase.toCollect()),
                    orderBy.reverseFlags(),
                    orderBy.nullsFirst()
                ),
                executor,
                rowReceiver.requirements().contains(Requirement.REPEAT)
            ),
            new BatchConsumerToRowReceiver(rowReceiver),
            rowReceiver
        );
    }

    private ShardCollectorProvider getCollectorProviderSafe(ShardId shardId) {
        ShardCollectorProvider shardCollectorProvider = shards.get(shardId);
        if (shardCollectorProvider == null) {
            throw new ShardNotFoundException(shardId);
        }
        return shardCollectorProvider;
    }

    private Collection<CrateCollector.Builder> getDocCollectors(JobCollectContext jobCollectContext,
                                                                RoutedCollectPhase collectPhase,
                                                                Set<Requirement> downstreamRequirements,
                                                                Map<String, List<Integer>> indexShards) {

        List<CrateCollector.Builder> crateCollectors = new ArrayList<>();
        for (Map.Entry<String, List<Integer>> entry : indexShards.entrySet()) {
            String indexName = entry.getKey();
            try {
                indicesService.indexServiceSafe(indexName);
            } catch (IndexNotFoundException e) {
                if (PartitionName.isPartition(indexName)) {
                    continue;
                }
                throw e;
            }
            for (Integer shardNum : entry.getValue()) {
                ShardId shardId = new ShardId(indexName, shardNum);
                try {
                    ShardCollectorProvider shardCollectorProvider = getCollectorProviderSafe(shardId);
                    CrateCollector.Builder collector = shardCollectorProvider.getCollectorBuilder(
                        collectPhase,
                        downstreamRequirements,
                        jobCollectContext
                    );
                    crateCollectors.add(collector);
                } catch (ShardNotFoundException | IllegalIndexShardStateException e) {
                    // If toCollect contains a docId it means that this is a QueryThenFetch operation.
                    // In such a case RemoteCollect cannot be used because on that node the FetchContext is missing
                    // and the reader required in the fetchPhase would be missing.
                    if (Symbols.containsColumn(collectPhase.toCollect(), DocSysColumns.FETCHID)) {
                        throw e;
                    }
                    crateCollectors.add(remoteCollectorFactory.createCollector(
                        shardId.getIndex(), shardId.id(), collectPhase, jobCollectContext.queryPhaseRamAccountingContext()));
                } catch (InterruptedException e) {
                    throw Throwables.propagate(e);
                } catch (Throwable t) {
                    throw new UnhandledServerException(t);
                }
            }
        }
        return crateCollectors;
    }

    private CrateCollector getShardsCollector(RoutedCollectPhase collectPhase,
                                              RoutedCollectPhase normalizedPhase,
                                              String localNodeId,
                                              RowReceiver rowReceiver) {
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
                ShardId shardId = new ShardId(indexName, shard);
                try {
                    ShardCollectorProvider shardCollectorProvider = getCollectorProviderSafe(shardId);
                    Object[] row = shardCollectorProvider.getRowForShard(normalizedPhase);
                    if (row != null) {
                        rows.add(row);
                    }
                } catch (ShardNotFoundException | IllegalIndexShardStateException e) {
                    unassignedShards.add(toUnassignedShard(shardId));
                } catch (Throwable t) {
                    t.printStackTrace();
                    throw new UnhandledServerException(t);
                }
            }
        }
        if (!unassignedShards.isEmpty()) {
            // since unassigned shards aren't really on any node we use the collectPhase which is NOT normalized here
            // because otherwise if _node was also selected it would contain something which is wrong
            for (Row row :
                systemCollectSource.toRowsIterableTransformation(collectPhase, false).apply(unassignedShards)) {
                rows.add(row.materialize());
            }
        }
        if (collectPhase.orderBy() != null) {
            rows.sort(OrderingByPosition.arrayOrdering(collectPhase).reverse());
        }
        return BatchIteratorCollectorBridge.newInstance(
            RowsBatchIterator.newInstance(
                Iterables.transform(rows, Buckets.arrayToRowFunction()),
                collectPhase.outputTypes().size()
            ),
            new BatchConsumerToRowReceiver(rowReceiver),
            rowReceiver
        );
    }

    private UnassignedShard toUnassignedShard(ShardId shardId) {
        return new UnassignedShard(shardId, clusterService, false, ShardRoutingState.UNASSIGNED);
    }
}
