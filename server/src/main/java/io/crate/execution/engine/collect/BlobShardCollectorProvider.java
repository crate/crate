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

import java.io.File;
import java.util.Map;

import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.threadpool.ThreadPool;
import org.jetbrains.annotations.Nullable;

import io.crate.blob.v2.BlobShard;
import io.crate.common.collections.Lists;
import io.crate.data.BatchIterator;
import io.crate.data.InMemoryBatchIterator;
import io.crate.data.Row;
import io.crate.data.SentinelRow;
import io.crate.execution.dsl.phases.RoutedCollectPhase;
import io.crate.execution.engine.collect.collectors.BlobOrderedDocCollector;
import io.crate.execution.engine.collect.collectors.OrderedDocCollector;
import io.crate.execution.engine.export.FileOutputFactory;
import io.crate.execution.jobs.NodeLimits;
import io.crate.execution.jobs.SharedShardContext;
import io.crate.expression.InputFactory;
import io.crate.expression.reference.doc.blob.BlobReferenceResolver;
import io.crate.expression.reference.sys.shard.ShardRowContext;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Schemas;
import io.crate.metadata.TransactionContext;

public class BlobShardCollectorProvider extends ShardCollectorProvider {

    private final BlobShard blobShard;
    private final InputFactory inputFactory;

    public BlobShardCollectorProvider(BlobShard blobShard,
                                      ClusterService clusterService,
                                      Schemas schemas,
                                      NodeLimits nodeJobsCounter,
                                      CircuitBreakerService circuitBreakerService,
                                      NodeContext nodeCtx,
                                      ThreadPool threadPool,
                                      Settings settings,
                                      ElasticsearchClient elasticsearchClient,
                                      Map<String, FileOutputFactory> fileOutputFactoryMap) {
        super(
            clusterService,
            circuitBreakerService,
            schemas,
            nodeJobsCounter,
            nodeCtx,
            threadPool,
            settings,
            elasticsearchClient,
            blobShard.indexShard(),
            new ShardRowContext(blobShard, clusterService),
            fileOutputFactoryMap
        );
        inputFactory = new InputFactory(nodeCtx);
        this.blobShard = blobShard;
    }

    @Nullable
    @Override
    protected BatchIterator<Row> getProjectionFusedIterator(RoutedCollectPhase normalizedPhase, CollectTask collectTask) {
        return null;
    }

    @Override
    protected BatchIterator<Row> getUnorderedIterator(RoutedCollectPhase collectPhase,
                                                      boolean requiresScroll,
                                                      CollectTask collectTask) {
        return InMemoryBatchIterator.of(getBlobRows(collectTask.txnCtx(), collectPhase, requiresScroll), SentinelRow.SENTINEL,
                                        true);
    }

    private Iterable<Row> getBlobRows(TransactionContext txnCtx, RoutedCollectPhase collectPhase, boolean requiresRepeat) {
        Iterable<File> files = blobShard.blobContainer().getFiles();
        Iterable<Row> rows = RowsTransformer.toRowsIterable(txnCtx, inputFactory, BlobReferenceResolver.INSTANCE, collectPhase, files);
        if (requiresRepeat) {
            return Lists.of(rows);
        }
        return rows;
    }

    public OrderedDocCollector getOrderedCollector(RoutedCollectPhase collectPhase,
                                                   SharedShardContext sharedShardContext,
                                                   CollectTask collectTask,
                                                   boolean requiresRepeat) {
        RoutedCollectPhase normalizedCollectPhase = collectPhase.normalize(shardNormalizer, collectTask.txnCtx());
        return new BlobOrderedDocCollector(
            blobShard.indexShard().shardId(),
            getBlobRows(collectTask.txnCtx(), normalizedCollectPhase, requiresRepeat));
    }
}
