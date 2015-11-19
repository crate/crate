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

import io.crate.action.job.SharedShardContext;
import io.crate.action.sql.query.CrateSearchContext;
import io.crate.action.sql.query.LuceneSortGenerator;
import io.crate.analyze.EvaluatingNormalizer;
import io.crate.analyze.symbol.Literal;
import io.crate.analyze.symbol.Symbols;
import io.crate.blob.v2.BlobIndices;
import io.crate.executor.transport.TransportActionProvider;
import io.crate.lucene.CrateDocIndexService;
import io.crate.metadata.*;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.metadata.shard.RecoveryShardReferenceResolver;
import io.crate.metadata.shard.ShardReferenceResolver;
import io.crate.metadata.shard.blob.BlobShardReferenceResolver;
import io.crate.operation.ImplementationSymbolVisitor;
import io.crate.operation.Input;
import io.crate.operation.collect.blobs.BlobDocCollector;
import io.crate.operation.collect.collectors.CollectorFieldsVisitor;
import io.crate.operation.collect.collectors.CrateDocCollector;
import io.crate.operation.collect.collectors.OrderedDocCollector;
import io.crate.operation.projectors.ProjectionToProjectorVisitor;
import io.crate.operation.projectors.RowReceiver;
import io.crate.operation.projectors.ShardProjectorChain;
import io.crate.operation.reference.doc.lucene.CollectorContext;
import io.crate.planner.node.dql.CollectPhase;
import org.elasticsearch.action.bulk.BulkRetryCoordinatorPool;
import org.elasticsearch.cluster.ClusterService;
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
    public Object[] getRowForShard(CollectPhase collectPhase) {
        assert collectPhase.maxRowGranularity() == RowGranularity.SHARD : "granularity must be SHARD";

        EvaluatingNormalizer shardNormalizer = new EvaluatingNormalizer(
                functions,
                RowGranularity.SHARD,
                new RecoveryShardReferenceResolver(shardResolver, indexShard)
        );
        collectPhase =  collectPhase.normalize(shardNormalizer);
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
    public CrateCollector getDocCollector(CollectPhase collectPhase,
                                          ShardProjectorChain projectorChain,
                                          JobCollectContext jobCollectContext) throws Exception {
        assert collectPhase.orderBy() == null : "getDocCollector shouldn't be called if there is an orderBy on the collectPhase";
        CollectPhase normalizedCollectNode = collectPhase.normalize(shardNormalizer);

        if (normalizedCollectNode.whereClause().noMatch()) {
            RowReceiver downstream = projectorChain.newShardDownstreamProjector(projectorVisitor);
            return RowsCollector.empty(downstream);
        }

        assert normalizedCollectNode.maxRowGranularity() == RowGranularity.DOC : "granularity must be DOC";
        if (isBlobShard) {
            RowReceiver downstream = projectorChain.newShardDownstreamProjector(projectorVisitor);
            return getBlobIndexCollector(normalizedCollectNode, downstream);
        } else {
            return getLuceneIndexCollector(threadPool, normalizedCollectNode, projectorChain, jobCollectContext);
        }
    }

    private CrateCollector getBlobIndexCollector(CollectPhase collectNode, RowReceiver downstream) {
        CollectInputSymbolVisitor.Context ctx = docInputSymbolVisitor.extractImplementations(collectNode);
        Input<Boolean> condition;
        if (collectNode.whereClause().hasQuery()) {
            condition = (Input)docInputSymbolVisitor.process(collectNode.whereClause().query(), ctx);
        } else {
            condition = Literal.newLiteral(true);
        }
        return new BlobDocCollector(
                blobIndices.blobShardSafe(shardId).blobContainer(),
                ctx.topLevelInputs(),
                ctx.docLevelExpressions(),
                condition,
                downstream
        );
    }

    private CrateCollector getLuceneIndexCollector(ThreadPool threadPool,
                                                   final CollectPhase collectNode,
                                                   final ShardProjectorChain projectorChain,
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
                    collectNode.whereClause()
            );
            jobCollectContext.addSearchContext(sharedShardContext.readerId(), searchContext);
            CollectInputSymbolVisitor.Context docCtx = docInputSymbolVisitor.extractImplementations(collectNode);
            Executor executor = threadPool.executor(ThreadPool.Names.SEARCH);

            return new CrateDocCollector(
                    searchContext,
                    executor,
                    jobCollectContext.keepAliveListener(),
                    jobCollectContext.queryPhaseRamAccountingContext(),
                    projectorChain.newShardDownstreamProjector(projectorVisitor),
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

    public OrderedDocCollector getOrderedCollector(CollectPhase collectPhase,
                                                SharedShardContext sharedShardContext,
                                                JobCollectContext jobCollectContext) {
        collectPhase = collectPhase.normalize(shardNormalizer);

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
            LOGGER.trace("[{}][{}] creating OrderedDocCollector. Expected number of rows to be collected: {}",
                    sharedShardContext.indexShard().routingEntry().currentNodeId(),
                    sharedShardContext.indexShard().shardId(),
                    batchSize);
        }
        return new OrderedDocCollector(
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
