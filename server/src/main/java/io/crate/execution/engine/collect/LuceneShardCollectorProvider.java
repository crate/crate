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
import java.util.function.Supplier;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.threadpool.ThreadPool;
import org.jspecify.annotations.Nullable;

import io.crate.data.BatchIterator;
import io.crate.data.InMemoryBatchIterator;
import io.crate.data.Row;
import io.crate.data.SentinelRow;
import io.crate.execution.dsl.phases.RoutedCollectPhase;
import io.crate.execution.engine.collect.collectors.LuceneBatchIterator;
import io.crate.execution.engine.collect.collectors.LuceneOrderedDocCollector;
import io.crate.execution.engine.collect.collectors.OptimizeQueryForSearchAfter;
import io.crate.execution.engine.collect.collectors.OrderedDocCollector;
import io.crate.execution.engine.export.FileOutputFactory;
import io.crate.execution.engine.sort.LuceneSort;
import io.crate.execution.jobs.NodeLimits;
import io.crate.execution.jobs.SharedShardContext;
import io.crate.expression.InputFactory;
import io.crate.expression.reference.doc.lucene.CollectorContext;
import io.crate.expression.reference.doc.lucene.LuceneCollectorExpression;
import io.crate.expression.reference.doc.lucene.LuceneReferenceResolver;
import io.crate.expression.reference.doc.lucene.StoredRowLookup;
import io.crate.expression.reference.sys.shard.ShardRowContext;
import io.crate.expression.symbol.Symbols;
import io.crate.lucene.LuceneQueryBuilder;
import io.crate.metadata.NodeContext;
import io.crate.metadata.PartitionName;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.doc.SysColumns;

public class LuceneShardCollectorProvider extends ShardCollectorProvider {

    private static final Logger LOGGER = LogManager.getLogger(LuceneShardCollectorProvider.class);

    private final Supplier<String> localNodeId;
    private final LuceneQueryBuilder luceneQueryBuilder;
    private final NodeContext nodeCtx;
    private final DocInputFactory docInputFactory;
    private final BigArrays bigArrays;
    private final ClusterService clusterService;

    private final LuceneReferenceResolver referenceResolver;

    public LuceneShardCollectorProvider(LuceneQueryBuilder luceneQueryBuilder,
                                        ClusterService clusterService,
                                        NodeLimits nodeJobsCounter,
                                        CircuitBreakerService circuitBreakerService,
                                        NodeContext nodeCtx,
                                        ThreadPool threadPool,
                                        Settings settings,
                                        Client elasticsearchClient,
                                        IndexShard indexShard,
                                        BigArrays bigArrays,
                                        Map<String, FileOutputFactory> fileOutputFactoryMap) {
        super(
            clusterService,
            circuitBreakerService,
            nodeJobsCounter,
            nodeCtx,
            threadPool,
            settings,
            elasticsearchClient,
            indexShard,
            new ShardRowContext(indexShard, clusterService),
            fileOutputFactoryMap
        );
        this.luceneQueryBuilder = luceneQueryBuilder;
        this.nodeCtx = nodeCtx;
        this.localNodeId = () -> clusterService.localNode().getId();
        // As this instance is cached but the relation name or table schema may change for the same index UUID,
        // we must not store the partition name or table info inside the instance but resolve on the fly instead.
        PartitionName partitionName = clusterService.state().metadata().getPartitionName(indexShard.shardId().getIndexUUID());
        DocTableInfo table = nodeCtx.schemas().getTableInfo(partitionName.relationName());
        this.referenceResolver = new LuceneReferenceResolver(
            partitionName.values(),
            table.partitionedByColumns(),
            table.primaryKey(),
            indexShard.getVersionCreated(),
            table.isParentReferenceIgnored()
        );
        this.docInputFactory = new DocInputFactory(nodeCtx, referenceResolver);
        this.bigArrays = bigArrays;
        this.clusterService = clusterService;
    }

    @Override
    protected BatchIterator<Row> getUnorderedIterator(RoutedCollectPhase collectPhase,
                                                      boolean requiresScroll,
                                                      CollectTask collectTask) {
        ShardId shardId = indexShard.shardId();
        SharedShardContext sharedShardContext = collectTask.sharedShardContexts().getOrCreateContext(shardId);
        var searcher = sharedShardContext.acquireSearcher("unordered-iterator: " + formatSource(collectPhase));
        collectTask.addSearcher(sharedShardContext.readerId(), searcher);
        // A closed shard has no mapper service and cannot be queried with lucene,
        // therefore skip it
        if (indexShard.isClosed()) {
            return InMemoryBatchIterator.empty(SentinelRow.SENTINEL);
        }
        IndexService indexService = sharedShardContext.indexService();
        PartitionName partitionName = clusterService.state().metadata().getPartitionName(indexShard.shardId().getIndexUUID());
        DocTableInfo table = nodeCtx.schemas().getTableInfo(partitionName.relationName());
        Version shardCreatedVersion = indexShard.getVersionCreated();
        LuceneQueryBuilder.Context queryContext = luceneQueryBuilder.convert(
            collectPhase.where(),
            collectTask.txnCtx(),
            partitionName.values(),
            indexService.indexAnalyzers(),
            table,
            shardCreatedVersion,
            indexService.cache(),
            collectTask::raiseIfKilled
        );
        InputFactory.Context<? extends LuceneCollectorExpression<?>> docCtx =
            docInputFactory.extractImplementations(collectTask.txnCtx(), collectPhase);

        return new LuceneBatchIterator(
            searcher.item(),
            queryContext.query(),
            queryContext.minScore(),
            Symbols.hasColumn(collectPhase.toCollect(), SysColumns.SCORE),
            new CollectorContext(sharedShardContext.readerId(), () -> StoredRowLookup.create(shardCreatedVersion, table, partitionName.values())),
            docCtx.topLevelInputs(),
            docCtx.expressions()
        );
    }

    @Nullable
    @Override
    protected BatchIterator<Row> getProjectionFusedIterator(RoutedCollectPhase normalizedPhase, CollectTask collectTask) {
        PartitionName partitionName = clusterService.state().metadata().getPartitionName(indexShard.shardId().getIndexUUID());
        DocTableInfo table = nodeCtx.schemas().getTableInfo(partitionName.relationName());
        var it = GroupByOptimizedIterator.tryOptimizeSingleStringKey(
            indexShard,
            table,
            partitionName.values(),
            luceneQueryBuilder,
            bigArrays,
            new InputFactory(nodeCtx),
            docInputFactory,
            normalizedPhase,
            collectTask
        );
        if (it != null) {
            return it;
        }
        it = DocValuesGroupByOptimizedIterator.tryOptimize(
            nodeCtx.functions(),
            referenceResolver,
            indexShard,
            table,
            partitionName.values(),
            luceneQueryBuilder,
            docInputFactory,
            normalizedPhase,
            collectTask
        );
        if (it != null) {
            return it;
        }
        return DocValuesAggregates.tryOptimize(
            nodeCtx.functions(),
            referenceResolver,
            indexShard,
            table,
            partitionName.values(),
            luceneQueryBuilder,
            normalizedPhase,
            collectTask
        );
    }

    @Override
    public OrderedDocCollector getOrderedCollector(RoutedCollectPhase phase,
                                                   SharedShardContext sharedShardContext,
                                                   CollectTask collectTask,
                                                   boolean requiresRepeat) {
        CollectorContext collectorContext;
        InputFactory.Context<? extends LuceneCollectorExpression<?>> ctx;
        var searcher = sharedShardContext.acquireSearcher("ordered-collector: " + formatSource(phase));
        collectTask.addSearcher(sharedShardContext.readerId(), searcher);
        IndexService indexService = sharedShardContext.indexService();
        PartitionName partitionName = clusterService.state().metadata().getPartitionName(indexShard.shardId().getIndexUUID());
        DocTableInfo table = nodeCtx.schemas().getTableInfo(partitionName.relationName());
        Version shardCreatedVersion = indexShard.getVersionCreated();
        final var queryContext = luceneQueryBuilder.convert(
            phase.where(),
            collectTask.txnCtx(),
            partitionName.values(),
            indexService.indexAnalyzers(),
            table,
            shardCreatedVersion,
            indexService.cache(),
            collectTask::raiseIfKilled
        );
        ctx = docInputFactory.extractImplementations(collectTask.txnCtx(), phase);
        collectorContext = new CollectorContext(
            sharedShardContext.readerId(),
            () -> StoredRowLookup.create(shardCreatedVersion, table, partitionName.values())
        );
        int batchSize = phase.shardQueueSize(localNodeId.get());
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("[{}][{}] creating LuceneOrderedDocCollector. Expected number of rows to be collected: {}",
                indexShard.routingEntry().currentNodeId(),
                indexShard.shardId(),
                batchSize);
        }
        var optimizeQueryForSearchAfter = new OptimizeQueryForSearchAfter(phase.orderBy());
        return new LuceneOrderedDocCollector(
            indexShard.shardId(),
            searcher.item(),
            queryContext.query(),
            queryContext.minScore(),
            Symbols.hasColumn(phase.toCollect(), SysColumns.SCORE),
            batchSize,
            collectTask.getRamAccounting(),
            collectorContext,
            optimizeQueryForSearchAfter,
            LuceneSort.generate(collectTask.txnCtx(), collectorContext, phase.orderBy(), docInputFactory),
            ctx.topLevelInputs(),
            ctx.expressions()
        );
    }

    static String formatSource(RoutedCollectPhase phase) {
        return phase.jobId().toString() + '-' + phase.phaseId() + '-' + phase.name();
    }
}
