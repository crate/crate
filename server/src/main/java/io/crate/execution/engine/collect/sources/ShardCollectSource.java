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

package io.crate.execution.engine.collect.sources;

import com.carrotsearch.hppc.IntIndexedContainer;
import com.carrotsearch.hppc.cursors.IntCursor;
import com.google.common.base.Suppliers;
import com.google.common.collect.Iterables;
import io.crate.analyze.OrderBy;
import io.crate.blob.v2.BlobIndicesService;
import io.crate.breaker.RowAccountingWithEstimators;
import io.crate.concurrent.CompletableFutures;
import io.crate.data.BatchIterator;
import io.crate.data.CompositeBatchIterator;
import io.crate.data.InMemoryBatchIterator;
import io.crate.data.Row;
import io.crate.data.SentinelRow;
import io.crate.exceptions.Exceptions;
import io.crate.execution.TransportActionProvider;
import io.crate.execution.dsl.phases.CollectPhase;
import io.crate.execution.dsl.phases.RoutedCollectPhase;
import io.crate.execution.dsl.projection.Projections;
import io.crate.execution.engine.collect.CollectTask;
import io.crate.execution.engine.collect.RemoteCollectorFactory;
import io.crate.execution.engine.collect.RowsTransformer;
import io.crate.execution.engine.collect.ShardCollectorProvider;
import io.crate.execution.engine.collect.collectors.OrderedDocCollector;
import io.crate.execution.engine.collect.collectors.OrderedLuceneBatchIteratorFactory;
import io.crate.execution.engine.pipeline.ProjectionToProjectorVisitor;
import io.crate.execution.engine.pipeline.ProjectorFactory;
import io.crate.execution.engine.pipeline.Projectors;
import io.crate.execution.engine.sort.OrderingByPosition;
import io.crate.execution.jobs.NodeLimits;
import io.crate.execution.jobs.SharedShardContext;
import io.crate.execution.jobs.SharedShardContexts;
import io.crate.expression.InputFactory;
import io.crate.expression.eval.EvaluatingNormalizer;
import io.crate.expression.reference.StaticTableReferenceResolver;
import io.crate.expression.reference.sys.shard.ShardRowContext;
import io.crate.expression.symbol.Symbols;
import io.crate.lucene.LuceneQueryBuilder;
import io.crate.metadata.IndexParts;
import io.crate.metadata.MapBackedRefResolver;
import io.crate.metadata.NodeContext;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.Schemas;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.metadata.shard.unassigned.UnassignedShard;
import io.crate.metadata.sys.SysShardsTableInfo;
import io.crate.planner.consumer.OrderByPositionVisitor;
import io.crate.plugin.IndexEventListenerProxy;
import io.crate.types.DataType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IllegalIndexShardStateException;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardNotFoundException;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.threadpool.ThreadPool;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.IntSupplier;
import java.util.function.Supplier;

import static io.crate.execution.support.ThreadPools.numIdleThreads;

/**
 * Used to create BatchIterators to gather documents stored within shards or to gather information about shards themselves.
 *
 * <p>
 *     The data is always exposed as a single BatchIterator.
 * </p>
 *
 * <h2>Concurrent consumption</h2>
 *
 * <p>
 *     In order to parallelize the consumption of multiple *inner* BatchIterators for certain operations
 *     (e.g. Aggregations, Group By..), a CompositeBatchIterator is used that loads all sources concurrently.
 * </p>
 * <p>
 *     This might look as follows:
 * </p>
 *
 * <pre>
 *     shard/searcher                        shard/searcher
 *       |                                         |
 *    LuceneBatchIterator                  LuceneBatchIterator
 *       |                                         |
 *    CollectingBatchIterator            CollectingBatchIterator
 *       \                                        /
 *        \_____________                _________/
 *                      \              /
 *                    AsyncCompositeBatchIterator
 *                     (with concurrent/ loadNextBatch of sources)
 * </pre>
 *
 * In other cases multiple shards are simply processed sequentially by concatenating the BatchIterators
 */
@Singleton
public class ShardCollectSource implements CollectSource {

    private static final Logger LOGGER = LogManager.getLogger(ShardCollectSource.class);

    private final IndicesService indicesService;
    private final ClusterService clusterService;
    private final RemoteCollectorFactory remoteCollectorFactory;
    private final Executor executor;
    private final ProjectorFactory sharedProjectorFactory;
    private final InputFactory inputFactory;

    private final Map<ShardId, Supplier<ShardCollectorProvider>> shards = new ConcurrentHashMap<>();
    private final ShardCollectorProviderFactory shardCollectorProviderFactory;
    private final StaticTableReferenceResolver<UnassignedShard> unassignedShardReferenceResolver;
    private final StaticTableReferenceResolver<ShardRowContext> shardReferenceResolver;
    private final IntSupplier availableThreads;

    @Inject
    public ShardCollectSource(Settings settings,
                              Schemas schemas,
                              IndicesService indicesService,
                              NodeContext nodeCtx,
                              ClusterService clusterService,
                              NodeLimits nodeJobsCounter,
                              LuceneQueryBuilder luceneQueryBuilder,
                              ThreadPool threadPool,
                              TransportActionProvider transportActionProvider,
                              RemoteCollectorFactory remoteCollectorFactory,
                              SystemCollectSource systemCollectSource,
                              IndexEventListenerProxy indexEventListenerProxy,
                              BlobIndicesService blobIndicesService,
                              PageCacheRecycler pageCacheRecycler,
                              CircuitBreakerService circuitBreakerService) {
        this.unassignedShardReferenceResolver = new StaticTableReferenceResolver<>(
            SysShardsTableInfo.unassignedShardsExpressions());
        this.shardReferenceResolver = new StaticTableReferenceResolver<>(SysShardsTableInfo.create().expressions());
        this.indicesService = indicesService;
        this.clusterService = clusterService;
        this.remoteCollectorFactory = remoteCollectorFactory;
        ThreadPoolExecutor executor = (ThreadPoolExecutor) threadPool.executor(ThreadPool.Names.SEARCH);
        this.availableThreads = numIdleThreads(executor, EsExecutors.numberOfProcessors(settings));
        this.executor = executor;
        this.inputFactory = new InputFactory(nodeCtx);
        BigArrays bigArrays = new BigArrays(pageCacheRecycler, circuitBreakerService, HierarchyCircuitBreakerService.QUERY, true);
        this.shardCollectorProviderFactory = new ShardCollectorProviderFactory(
            clusterService,
            circuitBreakerService,
            settings,
            schemas,
            threadPool,
            transportActionProvider,
            blobIndicesService,
            nodeCtx,
            luceneQueryBuilder,
            nodeJobsCounter,
            bigArrays);
        EvaluatingNormalizer nodeNormalizer = new EvaluatingNormalizer(
            nodeCtx,
            RowGranularity.DOC,
            new MapBackedRefResolver(Collections.emptyMap()),
            null);

        sharedProjectorFactory = new ProjectionToProjectorVisitor(
            clusterService,
            nodeJobsCounter,
            circuitBreakerService,
            nodeCtx,
            threadPool,
            settings,
            transportActionProvider,
            inputFactory,
            nodeNormalizer,
            systemCollectSource::getRowUpdater,
            systemCollectSource::tableDefinition
        );

        indexEventListenerProxy.addLast(new LifecycleListener());
    }

    public ProjectorFactory getProjectorFactory(ShardId shardId) {
        ShardCollectorProvider collectorProvider = getCollectorProviderSafe(shardId);
        return collectorProvider.getProjectorFactory();
    }

    private class LifecycleListener implements IndexEventListener {

        @Override
        public void afterIndexShardCreated(IndexShard indexShard) {
            LOGGER.debug("creating shard in {} {} {}", ShardCollectSource.this, indexShard.shardId(), shards.size());
            assert !shards.containsKey(indexShard.shardId()) : "shard entry already exists upon add";

            /* The creation of a ShardCollectorProvider accesses the clusterState, which leads to an
             * assertionError if accessed within a ClusterState-Update thread.
             *
             * So we wrap the creation in a supplier to create the providers lazy
             */
            Supplier<ShardCollectorProvider> providerSupplier = Suppliers.memoize(() -> shardCollectorProviderFactory.create(indexShard));
            shards.put(indexShard.shardId(), providerSupplier);
        }

        @Override
        public void beforeIndexShardClosed(ShardId shardId, @Nullable IndexShard indexShard, Settings indexSettings) {
            LOGGER.debug("removing shard upon close in {} shard={} numShards={}", ShardCollectSource.this, shardId, shards.size());
            shards.remove(shardId);
        }

        @Override
        public void beforeIndexShardDeleted(ShardId shardId, Settings indexSettings) {
            if (shards.remove(shardId) != null) {
                LOGGER.debug("removed shard upon delete in {} shard={} remainingShards={}", ShardCollectSource.this, shardId, shards.size());
            } else {
                LOGGER.debug("shard not found upon delete in {} shard={} remainingShards={}", ShardCollectSource.this, shardId, shards.size());
            }
        }
    }


    @Override
    public CompletableFuture<BatchIterator<Row>> getIterator(TransactionContext txnCtx,
                                                             CollectPhase phase,
                                                             CollectTask collectTask,
                                                             boolean supportMoveToStart) {
        RoutedCollectPhase collectPhase = (RoutedCollectPhase) phase;
        String localNodeId = clusterService.localNode().getId();

        Projectors projectors = new Projectors(
            collectPhase.projections(),
            collectPhase.jobId(),
            collectTask.txnCtx(),
            collectTask.getRamAccounting(),
            collectTask.memoryManager(),
            sharedProjectorFactory
        );
        boolean requireMoveToStartSupport = supportMoveToStart && !projectors.providesIndependentScroll();

        if (collectPhase.maxRowGranularity() == RowGranularity.SHARD) {
            return CompletableFuture.completedFuture(projectors.wrap(InMemoryBatchIterator.of(
                getShardsIterator(collectTask.txnCtx(), collectPhase, localNodeId), SentinelRow.SENTINEL, true)));
        }
        OrderBy orderBy = collectPhase.orderBy();
        if (collectPhase.maxRowGranularity() == RowGranularity.DOC && orderBy != null) {
            return createMultiShardScoreDocCollector(
                collectPhase,
                requireMoveToStartSupport,
                collectTask,
                localNodeId
            ).thenApply(projectors::wrap);
        }

        boolean hasShardProjections = Projections.hasAnyShardProjections(collectPhase.projections());
        Map<String, IntIndexedContainer> indexShards = collectPhase.routing().locations().get(localNodeId);
        List<CompletableFuture<BatchIterator<Row>>> iterators = indexShards == null
            ? Collections.emptyList()
            : getIterators(collectTask, collectPhase, requireMoveToStartSupport, indexShards);

        final CompletableFuture<BatchIterator<Row>> result;
        switch (iterators.size()) {
            case 0:
                result = CompletableFuture.completedFuture(InMemoryBatchIterator.empty(SentinelRow.SENTINEL));
                break;

            case 1:
                result = iterators.get(0);
                break;

            default:
                if (hasShardProjections) {
                    // use AsyncCompositeBatchIterator for multi-threaded loadNextBatch
                    // in order to process shard-based projections concurrently

                    //noinspection unchecked
                    result = CompletableFutures.allAsList(iterators)
                        .thenApply(its -> CompositeBatchIterator.asyncComposite(
                            executor,
                            availableThreads,
                            its.toArray(new BatchIterator[0])
                        ));
                } else {
                    //noinspection unchecked
                    result = CompletableFutures.allAsList(iterators)
                        .thenApply(its -> CompositeBatchIterator.seqComposite(its.toArray(new BatchIterator[0])));
                }
        }
        return result.thenApply(it -> projectors.wrap(it));
    }

    private CompletableFuture<BatchIterator<Row>> createMultiShardScoreDocCollector(RoutedCollectPhase collectPhase,
                                                                                    boolean supportMoveToStart,
                                                                                    CollectTask collectTask,
                                                                                    String localNodeId) {

        Map<String, Map<String, IntIndexedContainer>> locations = collectPhase.routing().locations();
        SharedShardContexts sharedShardContexts = collectTask.sharedShardContexts();
        Map<String, IntIndexedContainer> indexShards = locations.get(localNodeId);
        List<CompletableFuture<OrderedDocCollector>> orderedDocCollectors = new ArrayList<>();
        Metadata metadata = clusterService.state().metadata();
        for (Map.Entry<String, IntIndexedContainer> entry : indexShards.entrySet()) {
            String indexName = entry.getKey();
            Index index = metadata.index(indexName).getIndex();

            for (IntCursor shard : entry.getValue()) {
                ShardId shardId = new ShardId(index, shard.value);

                try {
                    SharedShardContext context = sharedShardContexts.getOrCreateContext(shardId);
                    ShardCollectorProvider shardCollectorProvider = getCollectorProviderSafe(shardId);
                    orderedDocCollectors.add(shardCollectorProvider.getFutureOrderedCollector(
                        collectPhase,
                        context,
                        collectTask,
                        supportMoveToStart)
                    );
                } catch (ShardNotFoundException | IllegalIndexShardStateException e) {
                    throw e;
                } catch (IndexNotFoundException e) {
                    if (IndexParts.isPartitioned(indexName)) {
                        break;
                    }
                    throw e;
                }
            }
        }
        List<DataType<?>> columnTypes = Symbols.typeView(collectPhase.toCollect());

        OrderBy orderBy = collectPhase.orderBy();
        assert orderBy != null : "orderBy must not be null";

        return CompletableFutures.allAsList(orderedDocCollectors).thenApply(collectors -> OrderedLuceneBatchIteratorFactory.newInstance(
            collectors,
            OrderingByPosition.rowOrdering(
                OrderByPositionVisitor.orderByPositions(orderBy.orderBySymbols(), collectPhase.toCollect()),
                orderBy.reverseFlags(),
                orderBy.nullsFirst()
            ),
            new RowAccountingWithEstimators(columnTypes, collectTask.getRamAccounting()),
            executor,
            availableThreads,
            supportMoveToStart
        ));
    }

    private ShardCollectorProvider getCollectorProviderSafe(ShardId shardId) {
        Supplier<ShardCollectorProvider> supplier = shards.get(shardId);
        if (supplier == null) {
            throw new ShardNotFoundException(shardId);
        }
        ShardCollectorProvider shardCollectorProvider = supplier.get();
        if (shardCollectorProvider == null) {
            throw new ShardNotFoundException(shardId);
        }
        return shardCollectorProvider;
    }

    private List<CompletableFuture<BatchIterator<Row>>> getIterators(CollectTask collectTask,
                                                                     RoutedCollectPhase collectPhase,
                                                                     boolean requiresScroll,
                                                                     Map<String, IntIndexedContainer> indexShards) {

        Metadata metadata = clusterService.state().metadata();
        List<CompletableFuture<BatchIterator<Row>>> iterators = new ArrayList<>();
        for (Map.Entry<String, IntIndexedContainer> entry : indexShards.entrySet()) {
            String indexName = entry.getKey();
            IndexMetadata indexMD = metadata.index(indexName);
            if (indexMD == null) {
                if (IndexParts.isPartitioned(indexName)) {
                    continue;
                }
                throw new IndexNotFoundException(indexName);
            }
            Index index = indexMD.getIndex();
            try {
                indicesService.indexServiceSafe(index);
            } catch (IndexNotFoundException e) {
                if (IndexParts.isPartitioned(indexName)) {
                    continue;
                }
                throw e;
            }
            for (IntCursor shardCursor: entry.getValue()) {
                ShardId shardId = new ShardId(index, shardCursor.value);
                try {
                    ShardCollectorProvider shardCollectorProvider = getCollectorProviderSafe(shardId);
                    CompletableFuture<BatchIterator<Row>> iterator = shardCollectorProvider.getFutureIterator(
                        collectPhase,
                        requiresScroll,
                        collectTask
                    );
                    iterators.add(iterator);
                } catch (ShardNotFoundException | IllegalIndexShardStateException e) {
                    // If toCollect contains a docId it means that this is a QueryThenFetch operation.
                    // In such a case RemoteCollect cannot be used because on that node the FetchTask is missing
                    // and the reader required in the fetchPhase would be missing.
                    if (Symbols.containsColumn(collectPhase.toCollect(), DocSysColumns.FETCHID)) {
                        throw e;
                    }
                    iterators.add(remoteCollectorFactory.createCollector(shardId, collectPhase, collectTask, shardCollectorProviderFactory));
                } catch (IndexNotFoundException e) {
                    // Prevent wrapping this to not break retry-detection
                    throw e;
                } catch (Throwable t) {
                    Exceptions.rethrowRuntimeException(t);
                }
            }
        }
        return iterators;
    }

    private Iterable<Row> getShardsIterator(TransactionContext txnCtx, RoutedCollectPhase collectPhase, String localNodeId) {
        Map<String, Map<String, IntIndexedContainer>> locations = collectPhase.routing().locations();
        List<UnassignedShard> unassignedShards = new ArrayList<>();
        List<ShardRowContext> shardRowContexts = new ArrayList<>();
        Map<String, IntIndexedContainer> indexShardsMap = locations.get(localNodeId);
        Metadata metadata = clusterService.state().metadata();


        for (Map.Entry<String, IntIndexedContainer> indexShards : indexShardsMap.entrySet()) {
            String indexName = indexShards.getKey();
            IndexMetadata indexMetadata = metadata.index(indexName);
            if (indexMetadata == null) {
                continue;
            }
            Index index = indexMetadata.getIndex();
            IntIndexedContainer shards = indexShards.getValue();
            IndexService indexService = indicesService.indexService(index);
            if (indexService == null) {
                for (IntCursor shard : shards) {
                    unassignedShards.add(toUnassignedShard(index.getName(), UnassignedShard.markAssigned(shard.value)));
                }
                continue;
            }
            for (IntCursor shard : shards) {
                if (UnassignedShard.isUnassigned(shard.value)) {
                    unassignedShards.add(toUnassignedShard(index.getName(), UnassignedShard.markAssigned(shard.value)));
                    continue;
                }
                ShardId shardId = new ShardId(index, shard.value);
                try {
                    ShardCollectorProvider shardCollectorProvider = getCollectorProviderSafe(shardId);
                    shardRowContexts.add(shardCollectorProvider.shardRowContext());
                } catch (ShardNotFoundException | IllegalIndexShardStateException e) {
                    unassignedShards.add(toUnassignedShard(index.getName(), shard.value));
                }
            }
        }

        Iterable<Row> assignedShardRows = RowsTransformer.toRowsIterable(
            txnCtx, inputFactory, shardReferenceResolver, collectPhase, shardRowContexts, false);
        Iterable<Row> rows;
        if (unassignedShards.size() > 0) {
            Iterable<Row> unassignedShardRows = RowsTransformer.toRowsIterable(
                txnCtx, inputFactory, unassignedShardReferenceResolver, collectPhase, unassignedShards, false);
            rows = Iterables.concat(assignedShardRows, unassignedShardRows);
        } else {
            rows = assignedShardRows;
        }

        if (collectPhase.orderBy() != null) {
            return RowsTransformer.sortRows(Iterables.transform(rows, Row::materialize), collectPhase);
        }

        return rows;
    }

    private UnassignedShard toUnassignedShard(String indexName, int shardId) {
        return new UnassignedShard(shardId, indexName, clusterService, false, ShardRoutingState.UNASSIGNED);
    }
}
