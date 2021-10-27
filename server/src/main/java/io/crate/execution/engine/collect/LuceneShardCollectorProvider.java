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

import java.util.function.Supplier;

import javax.annotation.Nullable;

import io.crate.data.InMemoryBatchIterator;
import io.crate.data.SentinelRow;
import io.crate.metadata.NodeContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.threadpool.ThreadPool;

import io.crate.data.BatchIterator;
import io.crate.data.Row;
import io.crate.execution.TransportActionProvider;
import io.crate.execution.dsl.phases.RoutedCollectPhase;
import io.crate.execution.engine.collect.collectors.LuceneBatchIterator;
import io.crate.execution.engine.collect.collectors.LuceneOrderedDocCollector;
import io.crate.execution.engine.collect.collectors.OptimizeQueryForSearchAfter;
import io.crate.execution.engine.collect.collectors.OrderedDocCollector;
import io.crate.execution.engine.sort.LuceneSortGenerator;
import io.crate.execution.jobs.NodeLimits;
import io.crate.execution.jobs.SharedShardContext;
import io.crate.expression.InputFactory;
import io.crate.expression.reference.doc.lucene.CollectorContext;
import io.crate.expression.reference.doc.lucene.LuceneCollectorExpression;
import io.crate.expression.reference.doc.lucene.LuceneReferenceResolver;
import io.crate.expression.reference.sys.shard.ShardRowContext;
import io.crate.expression.symbol.Symbols;
import io.crate.lucene.FieldTypeLookup;
import io.crate.lucene.LuceneQueryBuilder;
import io.crate.metadata.RelationName;
import io.crate.metadata.Schemas;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.metadata.doc.DocTableInfo;

public class LuceneShardCollectorProvider extends ShardCollectorProvider {

    private static final Logger LOGGER = LogManager.getLogger(LuceneShardCollectorProvider.class);

    private final Supplier<String> localNodeId;
    private final LuceneQueryBuilder luceneQueryBuilder;
    private final NodeContext nodeCtx;
    private final DocInputFactory docInputFactory;
    private final BigArrays bigArrays;
    private final FieldTypeLookup fieldTypeLookup;
    private final DocTableInfo table;

    public LuceneShardCollectorProvider(Schemas schemas,
                                        LuceneQueryBuilder luceneQueryBuilder,
                                        ClusterService clusterService,
                                        NodeLimits nodeJobsCounter,
                                        CircuitBreakerService circuitBreakerService,
                                        NodeContext nodeCtx,
                                        ThreadPool threadPool,
                                        Settings settings,
                                        TransportActionProvider transportActionProvider,
                                        IndexShard indexShard,
                                        BigArrays bigArrays) {
        super(
            clusterService,
            circuitBreakerService,
            schemas,
            nodeJobsCounter,
            nodeCtx,
            threadPool,
            settings,
            transportActionProvider,
            indexShard,
            new ShardRowContext(indexShard, clusterService)
        );
        this.luceneQueryBuilder = luceneQueryBuilder;
        this.nodeCtx = nodeCtx;
        this.localNodeId = () -> clusterService.localNode().getId();
        var mapperService = indexShard.mapperService();
        if (mapperService == null) {
            fieldTypeLookup = name -> null;
        } else {
            fieldTypeLookup = mapperService::fullName;
        }
        var relationName = RelationName.fromIndexName(indexShard.shardId().getIndexName());
        this.table = schemas.getTableInfo(relationName);
        this.docInputFactory = new DocInputFactory(
            nodeCtx,
            new LuceneReferenceResolver(
                indexShard.shardId().getIndexName(),
                fieldTypeLookup,
                table.partitionedByColumns()
            )
        );
        this.bigArrays = bigArrays;
    }

    @Override
    protected BatchIterator<Row> getUnorderedIterator(RoutedCollectPhase collectPhase,
                                                      boolean requiresScroll,
                                                      CollectTask collectTask) {
        ShardId shardId = indexShard.shardId();
        SharedShardContext sharedShardContext = collectTask.sharedShardContexts().getOrCreateContext(shardId);
        var searcher = sharedShardContext.acquireSearcher("unordered-iterator: " + formatSource(collectPhase));
        collectTask.addSearcher(sharedShardContext.readerId(), searcher);
        IndexShard sharedShardContextShard = sharedShardContext.indexShard();
        // A closed shard has no mapper service and cannot be queried with lucene,
        // therefore skip it
        boolean isClosed = sharedShardContextShard.mapperService() == null;
        if (isClosed) {
            return InMemoryBatchIterator.empty(SentinelRow.SENTINEL);
        }
        QueryShardContext queryShardContext = sharedShardContext.indexService().newQueryShardContext();
        LuceneQueryBuilder.Context queryContext = luceneQueryBuilder.convert(
            collectPhase.where(),
            collectTask.txnCtx(),
            sharedShardContextShard.mapperService(),
            sharedShardContextShard.shardId().getIndexName(),
            queryShardContext,
            table,
            sharedShardContext.indexService().cache()
        );
        InputFactory.Context<? extends LuceneCollectorExpression<?>> docCtx =
            docInputFactory.extractImplementations(collectTask.txnCtx(), collectPhase);

        return new LuceneBatchIterator(
            searcher.item(),
            queryContext.query(),
            queryContext.minScore(),
            Symbols.containsColumn(collectPhase.toCollect(), DocSysColumns.SCORE),
            new CollectorContext(sharedShardContext.readerId()),
            docCtx.topLevelInputs(),
            docCtx.expressions()
        );
    }

    @Nullable
    @Override
    protected BatchIterator<Row> getProjectionFusedIterator(RoutedCollectPhase normalizedPhase, CollectTask collectTask) {
        var it = GroupByOptimizedIterator.tryOptimizeSingleStringKey(
            indexShard,
            table,
            luceneQueryBuilder,
            fieldTypeLookup,
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
            indexShard,
            table,
            luceneQueryBuilder,
            fieldTypeLookup,
            docInputFactory,
            normalizedPhase,
            collectTask
        );
        if (it != null) {
            return it;
        }
        return DocValuesAggregates.tryOptimize(
            nodeCtx.functions(),
            indexShard,
            table,
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
        RoutedCollectPhase collectPhase = phase.normalize(shardNormalizer, collectTask.txnCtx());

        CollectorContext collectorContext;
        InputFactory.Context<? extends LuceneCollectorExpression<?>> ctx;
        var searcher = sharedShardContext.acquireSearcher("ordered-collector: " + formatSource(phase));
        collectTask.addSearcher(sharedShardContext.readerId(), searcher);
        IndexService indexService = sharedShardContext.indexService();
        QueryShardContext queryShardContext = indexService.newQueryShardContext();
        final var queryContext = luceneQueryBuilder.convert(
            collectPhase.where(),
            collectTask.txnCtx(),
            indexService.mapperService(),
            indexShard.shardId().getIndexName(),
            queryShardContext,
            table,
            indexService.cache()
        );
        ctx = docInputFactory.extractImplementations(collectTask.txnCtx(), collectPhase);
        collectorContext = new CollectorContext(sharedShardContext.readerId());
        int batchSize = collectPhase.shardQueueSize(localNodeId.get());
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("[{}][{}] creating LuceneOrderedDocCollector. Expected number of rows to be collected: {}",
                sharedShardContext.indexShard().routingEntry().currentNodeId(),
                sharedShardContext.indexShard().shardId(),
                batchSize);
        }
        OptimizeQueryForSearchAfter optimizeQueryForSearchAfter = new OptimizeQueryForSearchAfter(
            collectPhase.orderBy(),
            queryContext.queryShardContext(),
            fieldTypeLookup
        );
        return new LuceneOrderedDocCollector(
            indexShard.shardId(),
            searcher.item(),
            queryContext.query(),
            queryContext.minScore(),
            Symbols.containsColumn(collectPhase.toCollect(), DocSysColumns.SCORE),
            batchSize,
            collectTask.getRamAccounting(),
            collectorContext,
            optimizeQueryForSearchAfter,
            LuceneSortGenerator.generateLuceneSort(collectTask.txnCtx(), collectorContext, collectPhase.orderBy(), docInputFactory, fieldTypeLookup),
            ctx.topLevelInputs(),
            ctx.expressions()
        );
    }

    static String formatSource(RoutedCollectPhase phase) {
        return phase.jobId().toString() + '-' + phase.phaseId() + '-' + phase.name();
    }
}
