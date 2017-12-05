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

package io.crate.operation.collect;

import com.google.common.collect.ImmutableList;
import io.crate.action.job.SharedShardContext;
import io.crate.blob.v2.BlobShard;
import io.crate.data.Row;
import io.crate.executor.transport.TransportActionProvider;
import io.crate.metadata.Functions;
import io.crate.metadata.shard.blob.BlobShardReferenceResolver;
import io.crate.operation.InputFactory;
import io.crate.operation.NodeJobsCounter;
import io.crate.operation.collect.collectors.BlobOrderedDocCollector;
import io.crate.operation.collect.collectors.OrderedDocCollector;
import io.crate.operation.reference.doc.blob.BlobReferenceResolver;
import io.crate.planner.node.dql.RoutedCollectPhase;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.File;

public class BlobShardCollectorProvider extends ShardCollectorProvider {

    private final BlobShard blobShard;
    private final InputFactory inputFactory;

    public BlobShardCollectorProvider(BlobShard blobShard,
                                      ClusterService clusterService,
                                      NodeJobsCounter nodeJobsCounter,
                                      Functions functions,
                                      ThreadPool threadPool,
                                      Settings settings,
                                      TransportActionProvider transportActionProvider,
                                      BigArrays bigArrays) {
        super(clusterService, nodeJobsCounter, BlobShardReferenceResolver.create(blobShard), functions,
            threadPool, settings, transportActionProvider, blobShard.indexShard(), bigArrays);
        inputFactory = new InputFactory(functions);
        this.blobShard = blobShard;
    }

    @Override
    protected CrateCollector.Builder getBuilder(RoutedCollectPhase collectPhase,
                                                boolean requiresScroll,
                                                JobCollectContext jobCollectContext) {
        return RowsCollector.builder(getBlobRows(collectPhase, requiresScroll));
    }

    private Iterable<Row> getBlobRows(RoutedCollectPhase collectPhase, boolean requiresRepeat) {
        Iterable<File> files = blobShard.blobContainer().getFiles();
        Iterable<Row> rows = RowsTransformer.toRowsIterable(inputFactory, BlobReferenceResolver.INSTANCE, collectPhase, files);
        if (requiresRepeat) {
            return ImmutableList.copyOf(rows);
        }
        return rows;
    }

    public OrderedDocCollector getOrderedCollector(RoutedCollectPhase collectPhase,
                                                   SharedShardContext sharedShardContext,
                                                   JobCollectContext jobCollectContext,
                                                   boolean requiresRepeat) {
        RoutedCollectPhase normalizedCollectPhase = collectPhase.normalize(shardNormalizer, null);
        return new BlobOrderedDocCollector(blobShard.indexShard().shardId(), getBlobRows(normalizedCollectPhase, requiresRepeat));
    }
}
