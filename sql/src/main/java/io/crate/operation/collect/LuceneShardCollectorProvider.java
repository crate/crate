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
import io.crate.action.sql.query.LuceneSortGenerator;
import io.crate.analyze.symbol.Symbols;
import io.crate.executor.transport.TransportActionProvider;
import io.crate.lucene.FieldTypeLookup;
import io.crate.lucene.LuceneQueryBuilder;
import io.crate.metadata.Functions;
import io.crate.metadata.Schemas;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.metadata.shard.ShardReferenceResolver;
import io.crate.operation.InputFactory;
import io.crate.operation.NodeJobsCounter;
import io.crate.operation.collect.collectors.CollectorFieldsVisitor;
import io.crate.operation.collect.collectors.CrateDocCollectorBuilder;
import io.crate.operation.collect.collectors.LuceneOrderedDocCollector;
import io.crate.operation.collect.collectors.OptimizeQueryForSearchAfter;
import io.crate.operation.collect.collectors.OrderedDocCollector;
import io.crate.operation.reference.doc.lucene.CollectorContext;
import io.crate.operation.reference.doc.lucene.LuceneCollectorExpression;
import io.crate.operation.reference.doc.lucene.LuceneReferenceResolver;
import io.crate.planner.node.dql.RoutedCollectPhase;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.function.Supplier;

public class LuceneShardCollectorProvider extends ShardCollectorProvider {

    private static final Logger LOGGER = Loggers.getLogger(LuceneShardCollectorProvider.class);

    private final Supplier<String> localNodeId;
    private final LuceneQueryBuilder luceneQueryBuilder;
    private final IndexShard indexShard;
    private final DocInputFactory docInputFactory;
    private final FieldTypeLookup fieldTypeLookup;

    public LuceneShardCollectorProvider(Schemas schemas,
                                        LuceneQueryBuilder luceneQueryBuilder,
                                        ClusterService clusterService,
                                        NodeJobsCounter nodeJobsCounter,
                                        Functions functions,
                                        ThreadPool threadPool,
                                        Settings settings,
                                        TransportActionProvider transportActionProvider,
                                        IndexShard indexShard,
                                        BigArrays bigArrays) {
        super(clusterService, nodeJobsCounter, ShardReferenceResolver.create(clusterService, schemas, indexShard),
            functions, threadPool, settings, transportActionProvider, indexShard, bigArrays);
        this.luceneQueryBuilder = luceneQueryBuilder;
        this.indexShard = indexShard;
        this.localNodeId = () -> clusterService.localNode().getId();
        fieldTypeLookup = indexShard.mapperService()::fullName;
        this.docInputFactory = new DocInputFactory(functions,
            fieldTypeLookup,
            new LuceneReferenceResolver(fieldTypeLookup, indexShard.indexSettings()));
    }

    @Override
    protected CrateCollector.Builder getBuilder(RoutedCollectPhase collectPhase,
                                                boolean requiresScroll,
                                                JobCollectContext jobCollectContext) {
        ShardId shardId = indexShard.shardId();
        SharedShardContext sharedShardContext = jobCollectContext.sharedShardContexts().getOrCreateContext(shardId);
        Engine.Searcher searcher = sharedShardContext.acquireSearcher();
        IndexShard indexShard = sharedShardContext.indexShard();
        try {
            LuceneQueryBuilder.Context queryContext = luceneQueryBuilder.convert(
                collectPhase.whereClause(),
                indexShard.mapperService(),
                sharedShardContext.indexService().newQueryShardContext(shardId.getId(), searcher.reader(), System::currentTimeMillis),
                indexShard.indexFieldDataService(),
                sharedShardContext.indexService().cache()
            );
            jobCollectContext.addSearcher(sharedShardContext.readerId(), searcher);
            InputFactory.Context<? extends LuceneCollectorExpression<?>> docCtx =
                docInputFactory.extractImplementations(collectPhase);

            return new CrateDocCollectorBuilder(
                searcher.searcher(),
                queryContext.query(),
                queryContext.minScore(),
                Symbols.containsColumn(collectPhase.toCollect(), DocSysColumns.SCORE),
                getCollectorContext(sharedShardContext.readerId(), docCtx),
                jobCollectContext.queryPhaseRamAccountingContext(),
                docCtx.topLevelInputs(),
                docCtx.expressions()
            );
        } catch (Throwable t) {
            searcher.close();
            throw t;
        }
    }

    @Override
    public OrderedDocCollector getOrderedCollector(RoutedCollectPhase phase,
                                                   SharedShardContext sharedShardContext,
                                                   JobCollectContext jobCollectContext,
                                                   boolean requiresRepeat) {
        RoutedCollectPhase collectPhase = phase.normalize(shardNormalizer, null);

        CollectorContext collectorContext;
        InputFactory.Context<? extends LuceneCollectorExpression<?>> ctx;
        Engine.Searcher searcher = null;
        LuceneQueryBuilder.Context queryContext;
        try {
            searcher = sharedShardContext.acquireSearcher();
            IndexService indexService = sharedShardContext.indexService();
            queryContext = luceneQueryBuilder.convert(
                collectPhase.whereClause(),
                indexService.mapperService(),
                indexService.newQueryShardContext(indexShard.shardId().getId(), searcher.reader(), System::currentTimeMillis),
                indexService.fieldData(),
                indexService.cache()
            );
            jobCollectContext.addSearcher(sharedShardContext.readerId(), searcher);
            ctx = docInputFactory.extractImplementations(collectPhase);
            collectorContext = getCollectorContext(sharedShardContext.readerId(), ctx);
        } catch (Throwable t) {
            if (searcher != null) {
                searcher.close();
            }
            throw t;
        }
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
            searcher.searcher(),
            queryContext.query(),
            queryContext.minScore(),
            Symbols.containsColumn(collectPhase.toCollect(), DocSysColumns.SCORE),
            batchSize,
            collectorContext,
            optimizeQueryForSearchAfter,
            LuceneSortGenerator.generateLuceneSort(collectorContext, collectPhase.orderBy(), docInputFactory, fieldTypeLookup),
            ctx.topLevelInputs(),
            ctx.expressions()
        );
    }

    private CollectorContext getCollectorContext(int readerId, InputFactory.Context ctx) {
        return new CollectorContext(
            indexShard.indexFieldDataService(),
            new CollectorFieldsVisitor(ctx.expressions().size()),
            readerId
        );
    }
}
