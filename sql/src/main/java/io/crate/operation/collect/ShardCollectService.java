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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.crate.action.job.SharedShardContext;
import io.crate.action.sql.query.CrateSearchContext;
import io.crate.action.sql.query.LuceneSortGenerator;
import io.crate.analyze.EvaluatingNormalizer;
import io.crate.analyze.symbol.Symbols;
import io.crate.blob.v2.BlobIndices;
import io.crate.core.collections.Row;
import io.crate.executor.transport.TransportActionProvider;
import io.crate.lucene.CrateDocIndexService;
import io.crate.metadata.AbstractReferenceResolver;
import io.crate.metadata.Functions;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.metadata.shard.RecoveryShardReferenceResolver;
import io.crate.metadata.shard.ShardReferenceResolver;
import io.crate.metadata.shard.blob.BlobShardReferenceResolver;
import io.crate.operation.ImplementationSymbolVisitor;
import io.crate.operation.Input;
import io.crate.operation.collect.collectors.*;
import io.crate.operation.projectors.ProjectionToProjectorVisitor;
import io.crate.operation.projectors.Requirement;
import io.crate.operation.projectors.RowReceiver;
import io.crate.operation.projectors.ShardProjectorChain;
import io.crate.operation.reference.doc.lucene.CollectorContext;
import io.crate.planner.node.dql.RoutedCollectPhase;
import org.elasticsearch.action.bulk.BulkRetryCoordinatorPool;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.threadpool.ThreadPool;

import javax.annotation.Nullable;
import java.io.File;
import java.util.List;
import java.util.concurrent.Executor;

@Singleton
public class ShardCollectService {

    private static final ESLogger LOGGER = Loggers.getLogger(ShardCollectService.class);

    private final CollectInputSymbolVisitor<?> docInputSymbolVisitor;
    private final SearchContextFactory searchContextFactory;
    private final ThreadPool threadPool;
    private final ClusterService clusterService;
    private final ShardId shardId;
    private final IndexShard indexShard;
    private final ImplementationSymbolVisitor shardImplementationSymbolVisitor;
    private final EvaluatingNormalizer shardNormalizer;
    private final ProjectionToProjectorVisitor projectorVisitor;
    private final boolean isBlobShard;
    private final BlobIndices blobIndices;
    private final MapperService mapperService;
    private final IndexFieldDataService indexFieldDataService;
    private final Functions functions;
    private final AbstractReferenceResolver shardResolver;

    @Inject
    public ShardCollectService(SearchContextFactory searchContextFactory,
                               IndexNameExpressionResolver indexNameExpressionResolver,
                               ThreadPool threadPool,
                               ClusterService clusterService,
                               Settings settings,
                               TransportActionProvider transportActionProvider,
                               BulkRetryCoordinatorPool bulkRetryCoordinatorPool,
                               ShardId shardId,
                               IndexShard indexShard,
                               Functions functions,
                               ShardReferenceResolver referenceResolver,
                               BlobIndices blobIndices,
                               MapperService mapperService,
                               IndexFieldDataService indexFieldDataService,
                               BlobShardReferenceResolver blobShardReferenceResolver,
                               CrateDocIndexService crateDocIndexService) {
        this.searchContextFactory = searchContextFactory;
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        this.shardId = shardId;
        this.indexShard = indexShard;
        this.blobIndices = blobIndices;
        this.mapperService = mapperService;
        this.indexFieldDataService = indexFieldDataService;
        isBlobShard = BlobIndices.isBlobShard(this.shardId);

        shardResolver = isBlobShard ? blobShardReferenceResolver : referenceResolver;
        docInputSymbolVisitor = crateDocIndexService.docInputSymbolVisitor();

        this.functions = functions;
        this.shardImplementationSymbolVisitor = new ImplementationSymbolVisitor(functions);

        this.shardNormalizer = new EvaluatingNormalizer(
                functions,
                RowGranularity.SHARD,
                shardResolver
        );
        this.projectorVisitor = new ProjectionToProjectorVisitor(
                clusterService,
                functions,
                indexNameExpressionResolver,
                threadPool,
                settings,
                transportActionProvider,
                bulkRetryCoordinatorPool,
                shardImplementationSymbolVisitor,
                shardNormalizer,
                shardId
        );
    }

    @Nullable
    public Object[] getRowForShard(RoutedCollectPhase collectPhase) {
        assert collectPhase.maxRowGranularity() == RowGranularity.SHARD : "granularity must be SHARD";

        EvaluatingNormalizer shardNormalizer = new EvaluatingNormalizer(
                functions,
                RowGranularity.SHARD,
                new RecoveryShardReferenceResolver(shardResolver, indexShard)
        );
        collectPhase =  collectPhase.normalize(shardNormalizer, null);
        if (collectPhase.whereClause().noMatch()) {
            return null;
        }
        assert !collectPhase.whereClause().hasQuery()
                : "whereClause shouldn't have a query after normalize. Should be NO_MATCH or MATCH_ALL";

        List<Input<?>> inputs = shardImplementationSymbolVisitor.extractImplementations(collectPhase.toCollect()).topLevelInputs();
        Object[] row = new Object[inputs.size()];
        for (int i = 0; i < row.length; i++) {
            row[i] = inputs.get(i).value();
        }
        return row;
    }

    /**
     * get a collector
     *
     * @param collectPhase describes the collectOperation
     * @param projectorChain the shard projector chain to get the downstream from
     * @return collector wrapping different collect implementations, call {@link io.crate.operation.collect.CrateCollector#doCollect()} )} to start
     * collecting with this collector
     */
    public CrateCollector getDocCollector(RoutedCollectPhase collectPhase,
                                          ShardProjectorChain projectorChain,
                                          JobCollectContext jobCollectContext) throws Exception {
        assert collectPhase.orderBy() == null : "getDocCollector shouldn't be called if there is an orderBy on the collectPhase";
        RoutedCollectPhase normalizedCollectNode = collectPhase.normalize(shardNormalizer, null);
        RowReceiver downstream = projectorChain.newShardDownstreamProjector(projectorVisitor);

        if (normalizedCollectNode.whereClause().noMatch()) {
            return RowsCollector.empty(downstream);
        }
        assert normalizedCollectNode.maxRowGranularity() == RowGranularity.DOC : "granularity must be DOC";
        if (isBlobShard) {
            return new RowsCollector(downstream,
                getBlobRows(collectPhase, downstream.requirements().contains(Requirement.REPEAT)));
        } else {
            return getLuceneIndexCollector(threadPool, normalizedCollectNode, downstream, jobCollectContext);
        }
    }

    @VisibleForTesting
    Iterable<Row> getBlobRows(RoutedCollectPhase collectPhase, boolean requiresRepeat) {
        Iterable<File> files = blobIndices.blobShardSafe(shardId).blobContainer().getFiles();
        Iterable<Row> rows = RowsTransformer.toRowsIterable(docInputSymbolVisitor, collectPhase, files);
        if (requiresRepeat) {
            return ImmutableList.copyOf(rows);
        }
        return rows;
    }

    private CrateCollector getLuceneIndexCollector(ThreadPool threadPool,
                                                   final RoutedCollectPhase collectPhase,
                                                   RowReceiver rowReceiver,
                                                   final JobCollectContext jobCollectContext) throws Exception {
        SharedShardContext sharedShardContext = jobCollectContext.sharedShardContexts().getOrCreateContext(shardId);
        Engine.Searcher searcher = sharedShardContext.searcher();
        IndexShard indexShard = sharedShardContext.indexShard();
        CrateSearchContext searchContext = null;
        try {
             searchContext = searchContextFactory.createContext(
                    sharedShardContext.readerId(),
                    indexShard,
                    searcher,
                    collectPhase.whereClause()
            );
            jobCollectContext.addSearchContext(sharedShardContext.readerId(), searchContext);
            CollectInputSymbolVisitor.Context docCtx = docInputSymbolVisitor.extractImplementations(collectPhase);
            Executor executor = threadPool.executor(ThreadPool.Names.SEARCH);

            return new CrateDocCollector(
                searchContext,
                executor,
                Symbols.containsColumn(collectPhase.toCollect(), DocSysColumns.SCORE),
                jobCollectContext.queryPhaseRamAccountingContext(),
                rowReceiver,
                docCtx.topLevelInputs(),
                docCtx.docLevelExpressions()
            );
        } catch (Throwable t) {
            if (searchContext == null) {
                searcher.close();
            } else {
                searchContext.close(); // will close searcher too
            }
            throw t;
        }
    }

    public OrderedDocCollector getOrderedCollector(RoutedCollectPhase collectPhase,
                                                   SharedShardContext sharedShardContext,
                                                   JobCollectContext jobCollectContext,
                                                   boolean requiresRepeat) {
        RoutedCollectPhase normalizedCollectPhase = collectPhase.normalize(shardNormalizer, null);

        if (isBlobShard) {
            return getBlobOrderedDocCollector(normalizedCollectPhase, requiresRepeat);
        } else {
            return getLuceneOrderedDocCollector(normalizedCollectPhase, sharedShardContext, jobCollectContext);
        }
    }

    private OrderedDocCollector getBlobOrderedDocCollector(RoutedCollectPhase collectPhase, boolean requiresRepeat) {
        return new BlobOrderedDocCollector(shardId, getBlobRows(collectPhase, requiresRepeat));
    }

    private OrderedDocCollector getLuceneOrderedDocCollector(RoutedCollectPhase collectPhase, SharedShardContext sharedShardContext, JobCollectContext jobCollectContext) {
        CrateSearchContext searchContext = null;
        CollectorContext collectorContext;
        CollectInputSymbolVisitor.Context ctx;
        try {
            searchContext = searchContextFactory.createContext(
                    sharedShardContext.readerId(),
                    sharedShardContext.indexShard(),
                    sharedShardContext.searcher(),
                    collectPhase.whereClause()
            );
            jobCollectContext.addSearchContext(sharedShardContext.readerId(), searchContext);
            ctx = docInputSymbolVisitor.extractImplementations(collectPhase);

            collectorContext = new CollectorContext(
                    mapperService,
                    indexFieldDataService,
                    new CollectorFieldsVisitor(ctx.docLevelExpressions().size()),
                    sharedShardContext.readerId()
            );
        } catch (Throwable t) {
            if (searchContext != null) {
                searchContext.close();
            }
            throw t;
        }
        int batchSize = collectPhase.shardQueueSize(clusterService.localNode().id());
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("[{}][{}] creating LuceneOrderedDocCollector. Expected number of rows to be collected: {}",
                sharedShardContext.indexShard().routingEntry().currentNodeId(),
                sharedShardContext.indexShard().shardId(),
                batchSize);
        }
        return new LuceneOrderedDocCollector(
                searchContext,
                Symbols.containsColumn(collectPhase.toCollect(), DocSysColumns.SCORE),
                batchSize,
                collectorContext,
                collectPhase.orderBy(),
                LuceneSortGenerator.generateLuceneSort(collectorContext, collectPhase.orderBy(), docInputSymbolVisitor),
                ctx.topLevelInputs(),
                ctx.docLevelExpressions()
        );
    }
}
