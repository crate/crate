/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.execution.engine.collect;

import com.google.common.collect.ImmutableList;
import io.crate.blob.v2.BlobShard;
import io.crate.data.BatchIterator;
import io.crate.data.InMemoryBatchIterator;
import io.crate.data.Row;
import io.crate.data.SentinelRow;
import io.crate.execution.TransportActionProvider;
import io.crate.execution.dsl.phases.RoutedCollectPhase;
import io.crate.execution.engine.collect.collectors.BlobOrderedDocCollector;
import io.crate.execution.engine.collect.collectors.OrderedDocCollector;
import io.crate.execution.jobs.NodeJobsCounter;
import io.crate.execution.jobs.SharedShardContext;
import io.crate.expression.InputFactory;
import io.crate.expression.reference.doc.blob.BlobReferenceResolver;
import io.crate.expression.reference.sys.shard.ShardRowContext;
import io.crate.metadata.Functions;
import io.crate.metadata.Schemas;
import io.crate.metadata.TransactionContext;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;

import javax.annotation.Nullable;
import java.io.File;

public class BlobShardCollectorProvider extends ShardCollectorProvider {

    private final BlobShard blobShard;
    private final InputFactory inputFactory;

    public BlobShardCollectorProvider(BlobShard blobShard,
                                      ClusterService clusterService,
                                      Schemas schemas,
                                      NodeJobsCounter nodeJobsCounter,
                                      Functions functions,
                                      ThreadPool threadPool,
                                      Settings settings,
                                      TransportActionProvider transportActionProvider) {
        super(
            clusterService,
            schemas,
            nodeJobsCounter,
            functions,
            threadPool,
            settings,
            transportActionProvider,
            blobShard.indexShard(),
            new ShardRowContext(blobShard, clusterService)
        );
        inputFactory = new InputFactory(functions);
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
            return ImmutableList.copyOf(rows);
        }
        return rows;
    }

    public OrderedDocCollector getOrderedCollector(RoutedCollectPhase collectPhase,
                                                   SharedShardContext sharedShardContext,
                                                   CollectTask collectTask,
                                                   boolean requiresRepeat) {
        RoutedCollectPhase normalizedCollectPhase = collectPhase.normalize(shardNormalizer, null);
        return new BlobOrderedDocCollector(
            blobShard.indexShard().shardId(),
            getBlobRows(collectTask.txnCtx(), normalizedCollectPhase, requiresRepeat));
    }
}
