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

package io.crate.operator.operations.collect;

import io.crate.analyze.EvaluatingNormalizer;
import io.crate.analyze.elasticsearch.ESQueryBuilder;
import io.crate.metadata.Functions;
import io.crate.metadata.ReferenceResolver;
import io.crate.metadata.shard.ShardReferenceResolver;
import io.crate.operator.collector.CrateCollector;
import io.crate.operator.collector.LuceneDocCollector;
import io.crate.operator.collector.SimpleOneRowCollector;
import io.crate.operator.operations.CollectInputSymbolVisitor;
import io.crate.operator.operations.ImplementationSymbolVisitor;
import io.crate.operator.projectors.Projector;
import io.crate.operator.reference.doc.LuceneCollectorExpression;
import io.crate.operator.reference.doc.LuceneDocLevelReferenceResolver;
import io.crate.planner.RowGranularity;
import io.crate.planner.node.CollectNode;
import org.cratedb.action.SQLXContentQueryParser;
import org.cratedb.sql.CrateException;
import org.elasticsearch.cache.recycler.CacheRecycler;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.script.ScriptService;

public class ShardCollectService {

    public static CrateCollector NOOP_COLLECTOR = new CrateCollector() {
        @Override
        public void doCollect() {
        }
    };


    private final CollectInputSymbolVisitor<LuceneCollectorExpression<?>> docInputSymbolVisitor;
    private final ClusterService clusterService;
    private final ShardId shardId;
    private final IndexService indexService;
    private final ScriptService scriptService;
    private final CacheRecycler cacheRecycler;
    private final Functions functions;
    private final ReferenceResolver referenceResolver;
    private final SQLXContentQueryParser sqlxContentQueryParser;
    private final ESQueryBuilder queryBuilder;
    private final ImplementationSymbolVisitor shardInputSymbolVisitor;
    private final EvaluatingNormalizer shardNormalizer;

    @Inject
    public ShardCollectService(ClusterService clusterService,
                               ShardId shardId,
                               IndexService indexService,
                               ScriptService scriptService,
                               CacheRecycler cacheRecycler,
                               SQLXContentQueryParser sqlxContentQueryParser,
                               Functions functions,
                               ShardReferenceResolver referenceResolver) {
        this.clusterService = clusterService;
        this.shardId = shardId;
        this.indexService = indexService;
        this.scriptService = scriptService;
        this.cacheRecycler = cacheRecycler;
        this.sqlxContentQueryParser = sqlxContentQueryParser;
        this.functions = functions;
        this.referenceResolver = referenceResolver;
        this.shardInputSymbolVisitor = new ImplementationSymbolVisitor(
                referenceResolver, functions, RowGranularity.SHARD);
        this.docInputSymbolVisitor = new CollectInputSymbolVisitor<>(
                functions, LuceneDocLevelReferenceResolver.INSTANCE);
        this.queryBuilder = new ESQueryBuilder(functions, referenceResolver);
        this.shardNormalizer = new EvaluatingNormalizer(functions, RowGranularity.SHARD, referenceResolver);


    }

    /**
     * get a collector
     *
     * @param collectNode describes the collectOperation
     * @param upStream    every returned collector should call {@link io.crate.operator.projectors.Projector#setNextRow(Object...)}
     *                    on this upStream Projector if a row is produced.
     * @return collector wrapping different collect implementations, call {@link CrateCollector#doCollect()} to start
     * collecting with this collector
     */
    public CrateCollector getCollector(CollectNode collectNode, Projector upStream) throws Exception {

        collectNode = collectNode.normalize(shardNormalizer);

        if (collectNode.whereClause().noMatch()) {
            return NOOP_COLLECTOR;
        } else {
            RowGranularity granularity = collectNode.maxRowGranularity();
            if (granularity == RowGranularity.DOC) {
                CollectInputSymbolVisitor.Context docCtx = docInputSymbolVisitor.process(collectNode);
                BytesReference querySource = queryBuilder.convert(collectNode.whereClause());
                return new LuceneDocCollector(clusterService, shardId, indexService,
                        scriptService, cacheRecycler, sqlxContentQueryParser,
                        docCtx.topLevelInputs(),
                        docCtx.docLevelExpressions(),
                        querySource,
                        upStream);

            } else if (granularity == RowGranularity.SHARD) {
                ImplementationSymbolVisitor.Context shardCtx = shardInputSymbolVisitor.process(collectNode);
                return new SimpleOneRowCollector(shardCtx.topLevelInputs(), shardCtx.collectExpressions(), upStream);
            }
            throw new CrateException(String.format("Granularity %s not supported", granularity.name()));
        }
    }
}
