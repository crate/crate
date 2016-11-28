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
import io.crate.analyze.symbol.Symbols;
import io.crate.executor.transport.TransportActionProvider;
import io.crate.metadata.Functions;
import io.crate.metadata.Schemas;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.metadata.shard.ShardReferenceResolver;
import io.crate.operation.collect.collectors.CollectorFieldsVisitor;
import io.crate.operation.collect.collectors.CrateDocCollector;
import io.crate.operation.collect.collectors.LuceneOrderedDocCollector;
import io.crate.operation.collect.collectors.OrderedDocCollector;
import io.crate.operation.projectors.Requirement;
import io.crate.operation.reference.doc.lucene.CollectorContext;
import io.crate.operation.reference.doc.lucene.LuceneReferenceResolver;
import io.crate.planner.node.dql.RoutedCollectPhase;
import org.elasticsearch.action.bulk.BulkRetryCoordinatorPool;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Set;
import java.util.concurrent.Executor;

public class LuceneShardCollectorProvider extends ShardCollectorProvider {

    private static final ESLogger LOGGER = Loggers.getLogger(LuceneShardCollectorProvider.class);

    private final SearchContextFactory searchContextFactory;
    private final ThreadPool threadPool;
    private final String localNodeId;

    public LuceneShardCollectorProvider(Schemas schemas,
                                 SearchContextFactory searchContextFactory,
                                 ClusterService clusterService,
                                 Functions functions,
                                 IndexNameExpressionResolver indexNameExpressionResolver,
                                 ThreadPool threadPool,
                                 Settings settings,
                                 TransportActionProvider transportActionProvider,
                                 BulkRetryCoordinatorPool bulkRetryCoordinatorPool,
                                 IndexShard indexShard) {
        super(clusterService, new ShardReferenceResolver(clusterService, schemas, indexShard), functions,
            indexNameExpressionResolver, threadPool, settings, transportActionProvider, bulkRetryCoordinatorPool,
            indexShard,  new LuceneReferenceResolver(indexShard.mapperService()));
        this.searchContextFactory = searchContextFactory;
        this.threadPool = threadPool;
        this.localNodeId = clusterService.localNode().id();
    }

    @Override
    protected CrateCollector.Builder getBuilder(RoutedCollectPhase collectPhase,
                                                Set<Requirement> downstreamRequirements,
                                                JobCollectContext jobCollectContext) {
        SharedShardContext sharedShardContext = jobCollectContext.sharedShardContexts().getOrCreateContext(indexShard.shardId());
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

            return new CrateDocCollector.Builder(
                searchContext,
                executor,
                Symbols.containsColumn(collectPhase.toCollect(), DocSysColumns.SCORE),
                jobCollectContext.queryPhaseRamAccountingContext(),
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

    @Override
    public OrderedDocCollector getOrderedCollector(RoutedCollectPhase phase,
                                                   SharedShardContext sharedShardContext,
                                                   JobCollectContext jobCollectContext,
                                                   boolean requiresRepeat) {
        RoutedCollectPhase collectPhase = phase.normalize(shardNormalizer, null);

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
                indexShard.mapperService(),
                indexShard.indexFieldDataService(),
                new CollectorFieldsVisitor(ctx.docLevelExpressions().size()),
                sharedShardContext.readerId()
            );
        } catch (Throwable t) {
            if (searchContext != null) {
                searchContext.close();
            }
            throw t;
        }
        int batchSize = collectPhase.shardQueueSize(localNodeId);
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
