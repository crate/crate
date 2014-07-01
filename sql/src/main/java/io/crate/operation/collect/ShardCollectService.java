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

import io.crate.action.SQLXContentQueryParser;
import io.crate.analyze.EvaluatingNormalizer;
import io.crate.blob.v2.BlobIndices;
import io.crate.exceptions.UnhandledServerException;
import io.crate.executor.transport.task.elasticsearch.ESQueryBuilder;
import io.crate.metadata.Functions;
import io.crate.metadata.shard.ShardReferenceResolver;
import io.crate.metadata.shard.blob.BlobShardReferenceResolver;
import io.crate.operation.ImplementationSymbolVisitor;
import io.crate.operation.Input;
import io.crate.operation.collect.blobs.BlobDocCollector;
import io.crate.operation.projectors.ProjectionToProjectorVisitor;
import io.crate.operation.projectors.Projector;
import io.crate.operation.reference.doc.blob.BlobReferenceResolver;
import io.crate.operation.reference.doc.lucene.LuceneDocLevelReferenceResolver;
import io.crate.planner.RowGranularity;
import io.crate.planner.node.dql.CollectNode;
import io.crate.planner.symbol.Literal;
import org.elasticsearch.action.admin.indices.create.TransportCreateIndexAction;
import org.elasticsearch.action.bulk.TransportShardBulkAction;
import org.elasticsearch.cache.recycler.CacheRecycler;
import org.elasticsearch.cache.recycler.PageCacheRecycler;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.script.ScriptService;

public class ShardCollectService {

    private final CollectInputSymbolVisitor<?> docInputSymbolVisitor;
    private final ClusterService clusterService;
    private final ShardId shardId;
    private final IndexService indexService;
    private final ScriptService scriptService;
    private final CacheRecycler cacheRecycler;
    private final PageCacheRecycler pageCacheRecycler;
    private final BigArrays bigArrays;
    private final SQLXContentQueryParser sqlxContentQueryParser;
    private final ESQueryBuilder queryBuilder;
    private final ImplementationSymbolVisitor shardImplementationSymbolVisitor;
    private final EvaluatingNormalizer shardNormalizer;
    private final ProjectionToProjectorVisitor projectorVisitor;
    private final boolean isBlobShard;
    private final BlobIndices blobIndices;

    @Inject
    public ShardCollectService(ClusterService clusterService,
                               Settings settings,
                               TransportShardBulkAction transportShardBulkAction,
                               TransportCreateIndexAction transportCreateIndexAction,
                               ShardId shardId,
                               IndexService indexService,
                               ScriptService scriptService,
                               CacheRecycler cacheRecycler,
                               PageCacheRecycler pageCacheRecycler,
                               BigArrays bigArrays,
                               SQLXContentQueryParser sqlxContentQueryParser,
                               Functions functions,
                               ShardReferenceResolver referenceResolver,
                               BlobIndices blobIndices,
                               BlobShardReferenceResolver blobShardReferenceResolver) {
        this.clusterService = clusterService;
        this.shardId = shardId;

        this.indexService = indexService;
        this.scriptService = scriptService;
        this.cacheRecycler = cacheRecycler;
        this.pageCacheRecycler = pageCacheRecycler;
        this.bigArrays = bigArrays;
        this.sqlxContentQueryParser = sqlxContentQueryParser;

        this.queryBuilder = new ESQueryBuilder();

        this.blobIndices = blobIndices;
        isBlobShard = BlobIndices.isBlobShard(this.shardId);
        this.docInputSymbolVisitor = new CollectInputSymbolVisitor<>(
                functions,
                (isBlobShard ? BlobReferenceResolver.INSTANCE : LuceneDocLevelReferenceResolver.INSTANCE)
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
                transportShardBulkAction,
                transportCreateIndexAction,
                shardImplementationSymbolVisitor,
                shardNormalizer);
    }

    /**
     * get a collector
     *
     * @param collectNode describes the collectOperation
     * @param projectorChain the shard projector chain to get the downstream from
     * @return collector wrapping different collect implementations, call {@link CrateCollector#doCollect()} to start
     * collecting with this collector
     */
    public CrateCollector getCollector(CollectNode collectNode,
                                       ShardProjectorChain projectorChain) throws Exception {
        CollectNode normalizedCollectNode = collectNode.normalize(shardNormalizer);
        Projector downstream = projectorChain.newShardDownstreamProjector(projectorVisitor);

        if (normalizedCollectNode.whereClause().noMatch()) {
            return CrateCollector.NOOP;
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

    private CrateCollector getBlobIndexCollector(CollectNode collectNode, Projector downstream) {
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

    private CrateCollector getLuceneIndexCollector(CollectNode collectNode, Projector downstream) throws Exception {
        CollectInputSymbolVisitor.Context docCtx = docInputSymbolVisitor.process(collectNode);
        BytesReference querySource = queryBuilder.convert(collectNode.whereClause());
        return new LuceneDocCollector(
                clusterService,
                shardId,
                indexService,
                scriptService,
                cacheRecycler,
                pageCacheRecycler,
                bigArrays,
                sqlxContentQueryParser,
                docCtx.topLevelInputs(),
                docCtx.docLevelExpressions(),
                querySource,
                downstream);
    }
}
