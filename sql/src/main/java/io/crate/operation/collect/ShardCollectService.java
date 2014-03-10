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
import io.crate.exceptions.CrateException;
import io.crate.executor.transport.task.elasticsearch.ESQueryBuilder;
import io.crate.metadata.Functions;
import io.crate.metadata.shard.ShardReferenceResolver;
import io.crate.metadata.shard.blob.BlobShardReferenceResolver;
import io.crate.operation.ImplementationSymbolVisitor;
import io.crate.operation.projectors.Projector;
import io.crate.operation.reference.doc.LuceneCollectorExpression;
import io.crate.operation.reference.doc.LuceneDocLevelReferenceResolver;
import io.crate.planner.RowGranularity;
import io.crate.planner.node.dql.CollectNode;
import org.elasticsearch.cache.recycler.CacheRecycler;
import org.elasticsearch.cache.recycler.PageCacheRecycler;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.script.ScriptService;

public class ShardCollectService {

    private final CollectInputSymbolVisitor<LuceneCollectorExpression<?>> docInputSymbolVisitor;
    private final ClusterService clusterService;
    private final ShardId shardId;
    private final IndexService indexService;
    private final ScriptService scriptService;
    private final CacheRecycler cacheRecycler;
    private final PageCacheRecycler pageCacheRecycler;
    private final SQLXContentQueryParser sqlxContentQueryParser;
    private final ESQueryBuilder queryBuilder;
    private final ImplementationSymbolVisitor shardInputSymbolVisitor;
    private final EvaluatingNormalizer shardNormalizer;

    @Inject
    public ShardCollectService(ClusterService clusterService,
                               ShardId shardId,
                               @IndexSettings Settings indexSettings,
                               IndexService indexService,
                               ScriptService scriptService,
                               CacheRecycler cacheRecycler,
                               PageCacheRecycler pageCacheRecycler,
                               SQLXContentQueryParser sqlxContentQueryParser,
                               Functions functions,
                               ShardReferenceResolver referenceResolver,
                               BlobShardReferenceResolver blobShardReferenceResolver) {
        this.clusterService = clusterService;
        this.shardId = shardId;

        this.indexService = indexService;
        this.scriptService = scriptService;
        this.cacheRecycler = cacheRecycler;
        this.pageCacheRecycler = pageCacheRecycler;
        this.sqlxContentQueryParser = sqlxContentQueryParser;

        this.docInputSymbolVisitor = new CollectInputSymbolVisitor<>(
                functions, LuceneDocLevelReferenceResolver.INSTANCE);
        this.queryBuilder = new ESQueryBuilder();

        boolean isBlobShard = BlobIndices.isBlobShard(this.shardId);
        this.shardInputSymbolVisitor = new ImplementationSymbolVisitor(
                (isBlobShard ? blobShardReferenceResolver :referenceResolver),
                functions,
                RowGranularity.SHARD
        );
        this.shardNormalizer = new EvaluatingNormalizer(
                functions,
                RowGranularity.SHARD,
                (isBlobShard ? blobShardReferenceResolver :referenceResolver)
        );
    }

    /**
     * get a collector
     *
     * @param collectNode describes the collectOperation
     * @param downStream    every returned collector should call {@link io.crate.operation.projectors.Projector#setNextRow(Object...)}
     *                    on this downStream Projector if a row is produced.
     * @return collector wrapping different collect implementations, call {@link CrateCollector#doCollect()} to start
     * collecting with this collector
     */
    public CrateCollector getCollector(CollectNode collectNode, Projector downStream) throws Exception {

        collectNode = collectNode.normalize(shardNormalizer);

        if (collectNode.whereClause().noMatch()) {
            return CrateCollector.NOOP;
        } else {
            RowGranularity granularity = collectNode.maxRowGranularity();
            if (granularity == RowGranularity.DOC) {
                CollectInputSymbolVisitor.Context docCtx = docInputSymbolVisitor.process(collectNode);
                BytesReference querySource = queryBuilder.convert(collectNode.whereClause());
                return new LuceneDocCollector(clusterService, shardId, indexService,
                        scriptService, cacheRecycler, pageCacheRecycler,sqlxContentQueryParser,
                        docCtx.topLevelInputs(),
                        docCtx.docLevelExpressions(),
                        querySource,
                        downStream);

            } else if (granularity == RowGranularity.SHARD) {
                ImplementationSymbolVisitor.Context shardCtx = shardInputSymbolVisitor.process(collectNode);
                return new SimpleOneRowCollector(shardCtx.topLevelInputs(), shardCtx.collectExpressions(), downStream);
            }
            throw new CrateException(String.format("Granularity %s not supported", granularity.name()));
        }
    }
}
