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
import io.crate.analyze.EvaluatingNormalizer;
import io.crate.analyze.OrderBy;
import io.crate.blob.v2.BlobIndices;
import io.crate.executor.transport.TransportActionProvider;
import io.crate.metadata.Functions;
import io.crate.metadata.shard.ShardReferenceResolver;
import io.crate.metadata.shard.blob.BlobShardReferenceResolver;
import io.crate.operation.ImplementationSymbolVisitor;
import io.crate.operation.Input;
import io.crate.operation.collect.blobs.BlobDocCollector;
import io.crate.operation.collect.collectors.CrateDocCollector;
import io.crate.operation.collect.collectors.ScoreDocCollector;
import io.crate.operation.projectors.ProjectionToProjectorVisitor;
import io.crate.operation.projectors.RowReceiver;
import io.crate.operation.projectors.ShardProjectorChain;
import io.crate.operation.projectors.sorting.OrderingByPosition;
import io.crate.operation.reference.ReferenceResolver;
import io.crate.operation.reference.doc.blob.BlobReferenceResolver;
import io.crate.operation.reference.doc.lucene.LuceneReferenceResolver;
import io.crate.planner.RowGranularity;
import io.crate.planner.consumer.OrderByPositionVisitor;
import io.crate.planner.node.dql.CollectPhase;
import io.crate.planner.symbol.Literal;
import org.elasticsearch.action.bulk.BulkRetryCoordinatorPool;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.concurrent.Executor;

@Singleton
public class ShardCollectService {

    private final CollectInputSymbolVisitor<?> docInputSymbolVisitor;
    private final SearchContextFactory searchContextFactory;
    private final ThreadPool threadPool;
    private final ShardId shardId;
    private final ImplementationSymbolVisitor shardImplementationSymbolVisitor;
    private final EvaluatingNormalizer shardNormalizer;
    private final ProjectionToProjectorVisitor projectorVisitor;
    private final boolean isBlobShard;
    private final BlobIndices blobIndices;

    @Inject
    public ShardCollectService(SearchContextFactory searchContextFactory,
                               ThreadPool threadPool,
                               ClusterService clusterService,
                               Settings settings,
                               TransportActionProvider transportActionProvider,
                               BulkRetryCoordinatorPool bulkRetryCoordinatorPool,
                               ShardId shardId,
                               Functions functions,
                               ShardReferenceResolver referenceResolver,
                               BlobIndices blobIndices,
                               BlobShardReferenceResolver blobShardReferenceResolver,
                               MapperService mapperService) {
        this.searchContextFactory = searchContextFactory;
        this.threadPool = threadPool;
        this.shardId = shardId;
        this.blobIndices = blobIndices;
        isBlobShard = BlobIndices.isBlobShard(this.shardId);

        ReferenceResolver<? extends Input<?>> resolver = (isBlobShard ? BlobReferenceResolver.INSTANCE : new LuceneReferenceResolver(mapperService));
        this.docInputSymbolVisitor = new CollectInputSymbolVisitor<>(
                functions,
                resolver
        );
        this.shardImplementationSymbolVisitor = new ImplementationSymbolVisitor(
                (isBlobShard ? blobShardReferenceResolver :referenceResolver),
                functions,
                RowGranularity.SHARD
        );
        this.shardNormalizer = new EvaluatingNormalizer(
                functions,
                RowGranularity.SHARD,
                (isBlobShard ? blobShardReferenceResolver : referenceResolver)
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

    public CrateCollector getShardCollector(CollectPhase collectPhase, RowReceiver rowReceiver) {
        assert collectPhase.maxRowGranularity() == RowGranularity.SHARD : "granularity must be SHARD";
        collectPhase =  collectPhase.normalize(shardNormalizer);
        if (collectPhase.whereClause().noMatch()) {
            return RowsCollector.empty(rowReceiver);
        }
        assert !collectPhase.whereClause().hasQuery()
                : "whereClause shouldn't have a query after normalize. Should be NO_MATCH or MATCH_ALL";
        return RowsCollector.single(
                shardImplementationSymbolVisitor.extractImplementations(collectPhase).topLevelInputs(),
                rowReceiver
        );
    }

    /**
     * get a collector
     *
     * @param collectNode describes the collectOperation
     * @param projectorChain the shard projector chain to get the downstream from
     * @return collector wrapping different collect implementations, call {@link io.crate.operation.collect.CrateCollector#doCollect()} )} to start
     * collecting with this collector
     */
    public CrateCollector getDocCollector(CollectPhase collectNode,
                                          ShardProjectorChain projectorChain,
                                          JobCollectContext jobCollectContext,
                                          int jobSearchContextId,
                                          int pageSize) throws Exception {
        CollectPhase normalizedCollectNode = collectNode.normalize(shardNormalizer);

        if (normalizedCollectNode.whereClause().noMatch()) {
            RowReceiver downstream = projectorChain.newShardDownstreamProjector(projectorVisitor);
            return RowsCollector.empty(downstream);
        }

        assert normalizedCollectNode.maxRowGranularity() == RowGranularity.DOC : "granularity must be DOC";
        if (isBlobShard) {
            RowReceiver downstream = projectorChain.newShardDownstreamProjector(projectorVisitor);
            return getBlobIndexCollector(normalizedCollectNode, downstream);
        } else {
            return getLuceneIndexCollector(
                    threadPool,
                    normalizedCollectNode, projectorChain, jobCollectContext, jobSearchContextId, pageSize);
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
                                                   final JobCollectContext jobCollectContext,
                                                   final int jobSearchContextId,
                                                   int pageSize) throws Exception {
        SharedShardContext sharedShardContext = jobCollectContext.sharedShardContexts().getOrCreateContext(shardId);
        Engine.Searcher searcher = sharedShardContext.searcher();
        IndexShard indexShard = sharedShardContext.indexShard();
        CrateSearchContext searchContext = null;
        try {
             searchContext = searchContextFactory.createContext(
                    jobSearchContextId,
                    indexShard,
                    searcher,
                    collectNode.whereClause()
            );
            jobCollectContext.addSearchContext(jobSearchContextId, searchContext);
            CollectInputSymbolVisitor.Context docCtx = docInputSymbolVisitor.extractImplementations(collectNode);
            Executor executor = threadPool.executor(ThreadPool.Names.SEARCH);
            OrderBy orderBy = collectNode.orderBy();
            if (orderBy != null) {
                return new ScoreDocCollector(
                        searchContext,
                        jobCollectContext.keepAliveListener(),
                        executor,
                        collectNode,
                        projectorChain.scoreDocMerger(
                                OrderingByPosition.rowOrdering(
                                        OrderByPositionVisitor.orderByPositions(orderBy.orderBySymbols(), collectNode.toCollect()),
                                        orderBy.reverseFlags(),
                                        orderBy.nullsFirst()
                                )
                        ),
                        docCtx.topLevelInputs(),
                        docCtx.docLevelExpressions(),
                        docInputSymbolVisitor,
                        pageSize
                );
            } else {
                return new CrateDocCollector(
                        searchContext,
                        executor,
                        jobCollectContext.keepAliveListener(),
                        collectNode,
                        jobCollectContext.queryPhaseRamAccountingContext(),
                        projectorChain.newShardDownstreamProjector(projectorVisitor),
                        docCtx.topLevelInputs(),
                        docCtx.docLevelExpressions()
                );
            }
        } catch (Throwable t) {
            if (searchContext == null) {
                searcher.close();
            } else {
                searchContext.close(); // will close searcher too
            }
            throw t;
        }
    }
}
