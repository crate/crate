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

import io.crate.analyze.EvaluatingNormalizer;
import io.crate.blob.v2.BlobIndices;
import io.crate.breaker.CrateCircuitBreakerService;
import io.crate.exceptions.UnhandledServerException;
import io.crate.executor.transport.CollectContextService;
import io.crate.executor.transport.TransportActionProvider;
import io.crate.lucene.LuceneQueryBuilder;
import io.crate.metadata.Functions;
import io.crate.metadata.shard.ShardReferenceResolver;
import io.crate.metadata.shard.blob.BlobShardReferenceResolver;
import io.crate.operation.ImplementationSymbolVisitor;
import io.crate.operation.Input;
import io.crate.operation.RowDownstream;
import io.crate.operation.collect.blobs.BlobDocCollector;
import io.crate.operation.projectors.ProjectionToProjectorVisitor;
import io.crate.operation.projectors.Projector;
import io.crate.operation.reference.DocLevelReferenceResolver;
import io.crate.operation.reference.doc.blob.BlobReferenceResolver;
import io.crate.operation.reference.doc.lucene.LuceneDocLevelReferenceResolver;
import io.crate.planner.RowGranularity;
import io.crate.planner.node.dql.CollectNode;
import io.crate.planner.symbol.Literal;
import org.elasticsearch.cache.recycler.CacheRecycler;
import org.elasticsearch.cache.recycler.PageCacheRecycler;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;

public class ShardCollectService {

    private final CollectInputSymbolVisitor<?> docInputSymbolVisitor;
    private final ThreadPool threadPool;
    private final ClusterService clusterService;
    private LuceneQueryBuilder luceneQueryBuilder;
    private final ShardId shardId;
    private final IndexService indexService;
    private final ScriptService scriptService;
    private final CacheRecycler cacheRecycler;
    private final PageCacheRecycler pageCacheRecycler;
    private final BigArrays bigArrays;
    private final ImplementationSymbolVisitor shardImplementationSymbolVisitor;
    private final EvaluatingNormalizer shardNormalizer;
    private final ProjectionToProjectorVisitor projectorVisitor;
    private final boolean isBlobShard;
    private final Functions functions;
    private final BlobIndices blobIndices;

    private final CollectContextService collectContextService;

    @Inject
    public ShardCollectService(ThreadPool threadPool,
                               ClusterService clusterService,
                               LuceneQueryBuilder luceneQueryBuilder,
                               Settings settings,
                               TransportActionProvider transportActionProvider,
                               ShardId shardId,
                               IndexService indexService,
                               ScriptService scriptService,
                               CacheRecycler cacheRecycler,
                               PageCacheRecycler pageCacheRecycler,
                               BigArrays bigArrays,
                               Functions functions,
                               ShardReferenceResolver referenceResolver,
                               BlobIndices blobIndices,
                               BlobShardReferenceResolver blobShardReferenceResolver,
                               CrateCircuitBreakerService breakerService,
                               CollectContextService collectContextService) {
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        this.luceneQueryBuilder = luceneQueryBuilder;
        this.shardId = shardId;
        this.indexService = indexService;
        this.scriptService = scriptService;
        this.cacheRecycler = cacheRecycler;
        this.pageCacheRecycler = pageCacheRecycler;
        this.bigArrays = bigArrays;
        this.functions = functions;
        this.blobIndices = blobIndices;
        isBlobShard = BlobIndices.isBlobShard(this.shardId);

        DocLevelReferenceResolver<? extends Input<?>> resolver = (isBlobShard ? BlobReferenceResolver.INSTANCE : LuceneDocLevelReferenceResolver.INSTANCE);
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
                settings,
                transportActionProvider,
                shardImplementationSymbolVisitor,
                shardNormalizer,
                shardId,
                docInputSymbolVisitor);
        this.collectContextService = collectContextService;
    }

    /**
     * get a collector
     *
     * @param collectNode describes the collectOperation
     * @param projectorChain the shard projector chain to get the downstream from
     * @return collector wrapping different collect implementations, call {@link io.crate.operation.collect.CrateCollector#doCollect(io.crate.breaker.RamAccountingContext)} to start
     * collecting with this collector
     */
    public CrateCollector getCollector(CollectNode collectNode,
                                       ShardProjectorChain projectorChain) throws Exception {
        CollectNode normalizedCollectNode = collectNode.normalize(shardNormalizer);
        Projector downstream = projectorChain.newShardDownstreamProjector(projectorVisitor);

        if (normalizedCollectNode.whereClause().noMatch()) {
            return new NoopCrateCollector(downstream);
        } else {
            RowGranularity granularity = normalizedCollectNode.maxRowGranularity();
            if (granularity == RowGranularity.DOC) {
                if (isBlobShard) {
                    return getBlobIndexCollector(normalizedCollectNode, downstream);
                } else {
                    return getLuceneIndexCollector(normalizedCollectNode, downstream);
                }
            } else if (granularity == RowGranularity.SHARD) {
                ImplementationSymbolVisitor.Context shardCtx = shardImplementationSymbolVisitor.process(normalizedCollectNode);
                return new SimpleOneRowCollector(shardCtx.topLevelInputs(), shardCtx.collectExpressions(), downstream);
            }
            throw new UnhandledServerException(String.format("Granularity %s not supported", granularity.name()));
        }
    }

    private CrateCollector getBlobIndexCollector(CollectNode collectNode, RowDownstream downstream) {
        CollectInputSymbolVisitor.Context ctx = docInputSymbolVisitor.process(collectNode);
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

    private CrateCollector getLuceneIndexCollector(CollectNode collectNode, RowDownstream downstream) throws Exception {
        CollectInputSymbolVisitor.Context docCtx = docInputSymbolVisitor.process(collectNode);
        return new LuceneDocCollector(
                collectNode.jobId().get(),
                threadPool,
                clusterService,
                collectContextService,
                shardId,
                indexService,
                scriptService,
                cacheRecycler,
                pageCacheRecycler,
                bigArrays,
                docCtx.topLevelInputs(),
                docCtx.docLevelExpressions(),
                functions,
                collectNode.whereClause(),
                downstream);
    }
}
