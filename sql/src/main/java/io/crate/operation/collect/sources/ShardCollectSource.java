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
import com.google.common.collect.Iterables;
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
import io.crate.operation.projectors.ProjectingBatchConsumer;
import io.crate.operation.projectors.ProjectionToProjectorVisitor;
import io.crate.operation.projectors.ProjectorFactory;
import io.crate.operation.projectors.sorting.OrderingByPosition;
import io.crate.operation.reference.sys.node.local.NodeSysExpression;
import io.crate.operation.reference.sys.node.local.NodeSysReferenceResolver;
import io.crate.planner.consumer.OrderByPositionVisitor;
import io.crate.planner.node.dql.CollectPhase;
import io.crate.planner.node.dql.RoutedCollectPhase;
import io.crate.planner.projection.Projections;
import io.crate.plugin.IndexEventListenerProxy;
import org.elasticsearch.action.bulk.BulkRetryCoordinatorPool;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.*;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

import static io.crate.blob.v2.BlobIndex.isBlobIndex;

/**
 * Factory to create a collector which collects data from 1 or more shards.
 * <p>
 * <p>
 * A Collector is a component which can be used to "launch" a collect operation.
 * Once launched, a {@link BatchConsumer} will receive a {@link BatchIterator}, which it consumes to generate a result.
 * </p>
 * <p>
 * <p>
 * To support collection from multiple shards a {@link CompositeCollector} collector is used.
 * This CompositeCollector can have multiple sub-collectors (1 per shard)
 * </p>
 * <p>
 * <p>
 * <p>
 * <b>concurrent consumption</b>
 * <p>
 * For grouping and aggregation operations it's advantageous to run them concurrently. This can be done
 * if there are multiple shards.
 * <p>
 * Since there is just a single Collector returned by {@link #getCollector(CollectPhase, BatchConsumer, JobCollectContext)}
 * and there is only a single {@link BatchConsumer} receiving a {@link BatchIterator} which cannot be consumed concurrently
 * the following pattern is used:
 * <p>
 * <pre>
 *                  CompositeCollector
 *
 *             Collector1                  Collector2
 *                |                            |
 *             doCollect                   doCollect
 *                |                            |
 *         shardConsumer                   shardConsumer
 *            accept(s1BatchIterator)       accept(s2BatchIterator)
 *                 \                          /
 *                  \                       /
 *                   \                     /
 *               +---------------------------------+
 *               |       MultiConsumer             |
 *               |  AsyncCompositeBatchIterator    |      loadNextBatch will run
 *               |                                 |        s1bi.loadNextBatch + s2bi.loadNextBatch concurrently.
 *               |   s1bi            s2bi          |
 *               +----------------------------------        s1bi/s2bi loadNextBatch encapsulate CPU heavy
 *                          |                               aggregation / grouping
 *                          |
 *                       nodeConsumer // consumes the compositeBatchIterator
 *
 * </pre>
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
    private final Executor executor;
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
                              IndexEventListenerProxy indexEventListenerProxy,
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
        this.executor = new DirectFallbackExecutor(threadPool.executor(ThreadPool.Names.SEARCH));
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

        indexEventListenerProxy.addLast(new LifecycleListener());
    }

    /**
     * Executor that delegates the execution of tasks to the provided {@link Executor} until
     * it starts rejecting tasks with {@link EsRejectedExecutionException} in which case it executes the
     * tasks directly
     */
    public static class DirectFallbackExecutor implements Executor {

        private Executor delegateExecutor;

        public DirectFallbackExecutor(Executor delegateExecutor) {
            this.delegateExecutor = delegateExecutor;
        }

        @Override
        public void execute(Runnable command) {
            try {
                delegateExecutor.execute(command);
            } catch (EsRejectedExecutionException e) {
                command.run();
            }
        }
    }

    private class LifecycleListener implements IndexEventListener {

        @Override
        public void afterIndexShardCreated(IndexShard indexShard) {
            logger.debug("creating shard in {} {} {}", ShardCollectSource.this, indexShard.shardId(), shards.size());
            assert !shards.containsKey(indexShard.shardId()) : "shard entry already exists upon add";
            ShardCollectorProvider provider;
            if (isBlobIndex(indexShard.shardId().getIndexName())) {
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
    public CrateCollector getCollector(CollectPhase phase,
                                       BatchConsumer lastConsumer,
                                       JobCollectContext jobCollectContext) {
        RoutedCollectPhase collectPhase = (RoutedCollectPhase) phase;
        RoutedCollectPhase normalizedPhase = collectPhase.normalize(nodeNormalizer, null);

        String localNodeId = clusterService.localNode().getId();


        BatchConsumer firstConsumer = ProjectingBatchConsumer.create(
            lastConsumer,
            Projections.nodeProjections(normalizedPhase.projections()),
            collectPhase.jobId(),
            jobCollectContext.queryPhaseRamAccountingContext(),
            sharedProjectorFactory
        );
        if (normalizedPhase.maxRowGranularity() == RowGranularity.SHARD) {
            // it's possible to use FlatProjectorChain instead of ShardProjectorChain as a shortcut because
            // the rows are "pre-created" on a shard level.
            // The getShardsCollector method always only uses a single RowReceiver and not one per shard)
            return getShardsCollector(collectPhase, normalizedPhase, localNodeId, firstConsumer);
        }
        OrderBy orderBy = normalizedPhase.orderBy();
        if (normalizedPhase.maxRowGranularity() == RowGranularity.DOC && orderBy != null) {
            return createMultiShardScoreDocCollector(
                normalizedPhase,
                firstConsumer,
                jobCollectContext,
                localNodeId
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
                getDocCollectors(jobCollectContext, normalizedPhase, lastConsumer.requiresScroll(), indexShards));
        }

        switch (builders.size()) {
            case 0:
                return RowsCollector.empty(firstConsumer);
            case 1:
                CrateCollector.Builder collectorBuilder = builders.iterator().next();
                return collectorBuilder.build(collectorBuilder.applyProjections(firstConsumer));
            default:
                if (hasShardProjections) {
                    // use AsyncCompositeBatchIterator for multi-threaded loadNextBatch
                    // in order to process shard-based projections concurrently
                    return new CompositeCollector(
                        builders,
                        firstConsumer,
                        iterators -> new AsyncCompositeBatchIterator(executor, iterators)
                    );
                } else {
                    return new CompositeCollector(builders, firstConsumer, CompositeBatchIterator::new);
                }
        }
    }

    private CrateCollector createMultiShardScoreDocCollector(RoutedCollectPhase collectPhase,
                                                             BatchConsumer consumer,
                                                             JobCollectContext jobCollectContext,
                                                             String localNodeId) {

        Map<String, Map<String, List<Integer>>> locations = collectPhase.routing().locations();
        SharedShardContexts sharedShardContexts = jobCollectContext.sharedShardContexts();
        Map<String, List<Integer>> indexShards = locations.get(localNodeId);
        List<OrderedDocCollector> orderedDocCollectors = new ArrayList<>();
        MetaData metaData = clusterService.state().metaData();
        for (Map.Entry<String, List<Integer>> entry : indexShards.entrySet()) {
            String indexName = entry.getKey();
            Index index = metaData.index(indexName).getIndex();

            for (Integer shardNum : entry.getValue()) {
                ShardId shardId = new ShardId(index, shardNum);
                SharedShardContext context = sharedShardContexts.getOrCreateContext(shardId);

                try {
                    ShardCollectorProvider shardCollectorProvider = getCollectorProviderSafe(shardId);
                    orderedDocCollectors.add(shardCollectorProvider.getOrderedCollector(collectPhase,
                        context,
                        jobCollectContext,
                        consumer.requiresScroll()));
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
                consumer.requiresScroll()
            ),
            consumer
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
                                                                boolean requiresScroll,
                                                                Map<String, List<Integer>> indexShards) {

        MetaData metaData = clusterService.state().metaData();
        List<CrateCollector.Builder> crateCollectors = new ArrayList<>();
        for (Map.Entry<String, List<Integer>> entry : indexShards.entrySet()) {
            String indexName = entry.getKey();
            IndexMetaData indexMD = metaData.index(indexName);
            if (indexMD == null) {
                if (PartitionName.isPartition(indexName)) {
                    continue;
                }
                throw new IndexNotFoundException(indexName);
            }
            Index index = indexMD.getIndex();
            try {
                indicesService.indexServiceSafe(index);
            } catch (IndexNotFoundException e) {
                if (PartitionName.isPartition(indexName)) {
                    continue;
                }
                throw e;
            }
            for (Integer shardNum : entry.getValue()) {
                ShardId shardId = new ShardId(index, shardNum);
                try {
                    ShardCollectorProvider shardCollectorProvider = getCollectorProviderSafe(shardId);
                    CrateCollector.Builder collector = shardCollectorProvider.getCollectorBuilder(
                        collectPhase,
                        requiresScroll,
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
                        indexName, shardId.id(), collectPhase, jobCollectContext.queryPhaseRamAccountingContext()));
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
                                              BatchConsumer consumer) {
        Map<String, Map<String, List<Integer>>> locations = collectPhase.routing().locations();
        List<UnassignedShard> unassignedShards = new ArrayList<>();
        List<Object[]> rows = new ArrayList<>();
        Map<String, List<Integer>> indexShardsMap = locations.get(localNodeId);
        MetaData metaData = clusterService.state().metaData();

        for (Map.Entry<String, List<Integer>> indexShards : indexShardsMap.entrySet()) {
            String indexName = indexShards.getKey();
            Index index = metaData.index(indexName).getIndex();
            List<Integer> shards = indexShards.getValue();
            IndexService indexService = indicesService.indexService(index);
            if (indexService == null) {
                for (Integer shard : shards) {
                    unassignedShards.add(toUnassignedShard(new ShardId(index, UnassignedShard.markAssigned(shard))));
                }
                continue;
            }
            for (Integer shard : shards) {
                if (UnassignedShard.isUnassigned(shard)) {
                    unassignedShards.add(toUnassignedShard(new ShardId(index, UnassignedShard.markAssigned(shard))));
                    continue;
                }
                ShardId shardId = new ShardId(index, shard);
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
                Iterables.transform(rows, Buckets.arrayToRowFunction()), collectPhase.outputTypes().size()),
            consumer
        );
    }

    private UnassignedShard toUnassignedShard(ShardId shardId) {
        return new UnassignedShard(shardId, clusterService, false, ShardRoutingState.UNASSIGNED);
    }
}
