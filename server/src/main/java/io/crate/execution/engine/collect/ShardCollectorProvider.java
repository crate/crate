/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.execution.engine.collect;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.threadpool.ThreadPool;
import org.jetbrains.annotations.Nullable;

import io.crate.analyze.WhereClause;
import io.crate.data.BatchIterator;
import io.crate.data.InMemoryBatchIterator;
import io.crate.data.Row;
import io.crate.data.SentinelRow;
import io.crate.execution.dsl.phases.RoutedCollectPhase;
import io.crate.execution.dsl.projection.Projections;
import io.crate.execution.engine.collect.collectors.OrderedDocCollector;
import io.crate.execution.engine.export.FileOutputFactory;
import io.crate.execution.engine.pipeline.ProjectionToProjectorVisitor;
import io.crate.execution.engine.pipeline.ProjectorFactory;
import io.crate.execution.engine.pipeline.Projectors;
import io.crate.execution.jobs.NodeLimits;
import io.crate.execution.jobs.SharedShardContext;
import io.crate.expression.InputFactory;
import io.crate.expression.eval.EvaluatingNormalizer;
import io.crate.expression.reference.sys.shard.ShardRowContext;
import io.crate.metadata.NodeContext;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.shard.ShardReferenceResolver;

public abstract class ShardCollectorProvider {

    private final ProjectorFactory projectorFactory;
    private final ShardRowContext shardRowContext;
    protected final IndexShard indexShard;
    final EvaluatingNormalizer shardNormalizer;
    private final BatchIteratorFactory batchIteratorFactory;

    ShardCollectorProvider(ClusterService clusterService,
                           CircuitBreakerService circuitBreakerService,
                           NodeLimits nodeJobsCounter,
                           NodeContext nodeCtx,
                           ThreadPool threadPool,
                           Settings settings,
                           ElasticsearchClient elasticsearchClient,
                           IndexShard indexShard,
                           ShardRowContext shardRowContext,
                           Map<String, FileOutputFactory> fileOutputFactoryMap) {
        this.indexShard = indexShard;
        this.shardRowContext = shardRowContext;
        shardNormalizer = new EvaluatingNormalizer(
            nodeCtx,
            RowGranularity.SHARD,
            new ShardReferenceResolver(nodeCtx.schemas(), shardRowContext),
            null
        );
        projectorFactory = new ProjectionToProjectorVisitor(
            clusterService,
            nodeJobsCounter,
            circuitBreakerService,
            nodeCtx,
            threadPool,
            settings,
            elasticsearchClient,
            new InputFactory(nodeCtx),
            shardNormalizer,
            t -> null,
            t -> null,
            indexShard.indexSettings().getIndexVersionCreated(),
            indexShard.shardId(),
            fileOutputFactoryMap
        );
        this.batchIteratorFactory = new BatchIteratorFactory();
    }

    public class BatchIteratorFactory {

        public BatchIterator<Row> getIterator(RoutedCollectPhase collectPhase,
                                              boolean requiresScroll,
                                              CollectTask collectTask) {
            assert collectPhase.orderBy() == null
                : "getDocCollector shouldn't be called if there is an orderBy on the collectPhase";
            assert collectPhase.maxRowGranularity() == RowGranularity.DOC :
                "granularity must be DOC";

            boolean isOpenIndex = !indexShard.isClosed();
            if (isOpenIndex) {
                BatchIterator<Row> fusedIterator = getProjectionFusedIterator(collectPhase, collectTask);
                if (fusedIterator != null) {
                    return fusedIterator;
                }
            }
            final BatchIterator<Row> iterator;
            if (isOpenIndex && WhereClause.canMatch(collectPhase.where())) {
                iterator = getUnorderedIterator(collectPhase, requiresScroll, collectTask);
            } else {
                iterator = InMemoryBatchIterator.empty(SentinelRow.SENTINEL);
            }
            return Projectors.wrap(
                Projections.shardProjections(collectPhase.projections()),
                collectPhase.jobId(),
                collectTask.txnCtx(),
                collectTask.getRamAccounting(),
                collectTask.memoryManager(),
                projectorFactory,
                iterator
            );
        }

        public OrderedDocCollector getOrderedCollector(RoutedCollectPhase collectPhase,
                                                       SharedShardContext sharedShardContext,
                                                       CollectTask collectTask,
                                                       boolean requiresRepeat) {
            return ShardCollectorProvider.this.getOrderedCollector(
                collectPhase,
                sharedShardContext,
                collectTask,
                requiresRepeat);
        }
    }

    public ShardRowContext shardRowContext() {
        return shardRowContext;
    }

    public CompletableFuture<BatchIteratorFactory> awaitShardSearchActive() {
        return indexShard.awaitShardSearchActive().thenApply(ignored -> batchIteratorFactory);
    }


    /**
     * @return A BatchIterator which already applies the transformation described in the shardProjections of the collectPhase.
     *         This can be used to return a specialized BatchIterator for certain projections. If this returns null
     *         a fallback/default BatchIterator will be created and projections will be applied using the projectorFactory
     */
    @Nullable
    protected abstract BatchIterator<Row> getProjectionFusedIterator(RoutedCollectPhase normalizedPhase,
                                                                     CollectTask collectTask);

    protected abstract BatchIterator<Row> getUnorderedIterator(RoutedCollectPhase collectPhase,
                                                               boolean requiresScroll,
                                                               CollectTask collectTask);

    protected abstract OrderedDocCollector getOrderedCollector(RoutedCollectPhase collectPhase,
                                                               SharedShardContext sharedShardContext,
                                                               CollectTask collectTask,
                                                               boolean requiresRepeat);

    public ProjectorFactory getProjectorFactory() {
        return projectorFactory;
    }
}
