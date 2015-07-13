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

import io.crate.action.sql.query.CrateSearchContext;
import io.crate.analyze.EvaluatingNormalizer;
import io.crate.blob.v2.BlobIndices;
import io.crate.exceptions.UnhandledServerException;
import io.crate.executor.transport.TransportActionProvider;
import io.crate.metadata.Functions;
import io.crate.metadata.shard.ShardReferenceResolver;
import io.crate.metadata.shard.blob.BlobShardReferenceResolver;
import io.crate.operation.ImplementationSymbolVisitor;
import io.crate.operation.Input;
import io.crate.operation.RowDownstream;
import io.crate.operation.collect.blobs.BlobDocCollector;
import io.crate.operation.projectors.ProjectionToProjectorVisitor;
import io.crate.operation.reference.DocLevelReferenceResolver;
import io.crate.operation.reference.doc.blob.BlobReferenceResolver;
import io.crate.operation.reference.doc.lucene.LuceneDocLevelReferenceResolver;
import io.crate.planner.RowGranularity;
import io.crate.planner.node.dql.CollectPhase;
import io.crate.planner.symbol.Literal;
import org.elasticsearch.action.bulk.BulkRetryCoordinatorPool;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.threadpool.ThreadPool;

public class ShardCollectService {

    private final CollectInputSymbolVisitor<?> docInputSymbolVisitor;
    private final SearchContextFactory searchContextFactory;
    private final ShardId shardId;
    private final IndexService indexService;
    private final Functions functions;
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
                               IndexService indexService,
                               Functions functions,
                               ShardReferenceResolver referenceResolver,
                               BlobIndices blobIndices,
                               BlobShardReferenceResolver blobShardReferenceResolver,
                               MapperService mapperService) {
        this.searchContextFactory = searchContextFactory;
        this.shardId = shardId;
        this.indexService = indexService;
        this.functions = functions;
        this.blobIndices = blobIndices;
        isBlobShard = BlobIndices.isBlobShard(this.shardId);

        DocLevelReferenceResolver<? extends Input<?>> resolver = (isBlobShard ? BlobReferenceResolver.INSTANCE : new LuceneDocLevelReferenceResolver(mapperService));
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

    /**
     * get a collector
     *
     * @param collectNode describes the collectOperation
     * @param projectorChain the shard projector chain to get the downstream from
     * @return collector wrapping different collect implementations, call {@link io.crate.operation.collect.CrateCollector#doCollect()} )} to start
     * collecting with this collector
     */
    public CrateCollector getCollector(CollectPhase collectNode,
                                       ShardProjectorChain projectorChain,
                                       JobCollectContext jobCollectContext,
                                       int jobSearchContextId) throws Exception {
        CollectPhase normalizedCollectNode = collectNode.normalize(shardNormalizer);
        RowDownstream downstream = projectorChain.newShardDownstreamProjector(projectorVisitor);

        if (normalizedCollectNode.whereClause().noMatch()) {
            return new NoopCrateCollector(downstream);
        } else {
            RowGranularity granularity = normalizedCollectNode.maxRowGranularity();
            if (granularity == RowGranularity.DOC) {
                if (isBlobShard) {
                    return getBlobIndexCollector(normalizedCollectNode, downstream);
                } else {
                    return getLuceneIndexCollector(normalizedCollectNode, downstream, jobCollectContext, jobSearchContextId);
                }
            } else if (granularity == RowGranularity.SHARD) {
                ImplementationSymbolVisitor.Context shardCtx = shardImplementationSymbolVisitor.extractImplementations(normalizedCollectNode);
                return new SimpleOneRowCollector(shardCtx.topLevelInputs(), downstream);
            }
            throw new UnhandledServerException(String.format("Granularity %s not supported", granularity.name()));
        }
    }

    private CrateCollector getBlobIndexCollector(CollectPhase collectNode, RowDownstream downstream) {
        CollectInputSymbolVisitor.Context ctx = docInputSymbolVisitor.extractImplementations(collectNode);
        Input<Boolean> condition;
        if (collectNode.whereClause().hasQuery()) {
            condition = (Input)docInputSymbolVisitor.process(collectNode.whereClause().query(), ctx);
        } else {
            condition = Literal.newLiteral(true);
        }
        return new BlobDocCollector(
                blobIndices.blobShardSafe(shardId),
                ctx.topLevelInputs(),
                ctx.docLevelExpressions(),
                condition,
                downstream
        );
    }

    private CrateCollector getLuceneIndexCollector(final CollectPhase collectNode,
                                                   final RowDownstream downstream,
                                                   final JobCollectContext jobCollectContext,
                                                   final int jobSearchContextId) throws Exception {
        IndexShard indexShard = indexService.shardSafe(shardId.id());
        Engine.Searcher searcher = EngineSearcher.getSearcherWithRetry(indexShard, "search", null);
        CrateSearchContext searchContext = null;
        try {
             searchContext = searchContextFactory.createContext(
                    jobSearchContextId,
                    indexShard,
                    searcher,
                    collectNode.whereClause()
            );
            jobCollectContext.addContext(jobSearchContextId, searchContext);
            CollectInputSymbolVisitor.Context docCtx = docInputSymbolVisitor.extractImplementations(collectNode);
            return new LuceneDocCollector(
                    searchContext,
                    docCtx.topLevelInputs(),
                    docCtx.docLevelExpressions(),
                    collectNode,
                    functions,
                    downstream,
                    jobCollectContext.queryPhaseRamAccountingContext()
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
}
