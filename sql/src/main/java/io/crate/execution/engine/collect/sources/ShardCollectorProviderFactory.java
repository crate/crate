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

package io.crate.execution.engine.collect.sources;

import io.crate.blob.v2.BlobIndicesService;
import io.crate.blob.v2.BlobShard;
import io.crate.execution.engine.collect.BlobShardCollectorProvider;
import io.crate.execution.engine.collect.LuceneShardCollectorProvider;
import io.crate.execution.engine.collect.ShardCollectorProvider;
import io.crate.execution.jobs.NodeJobsCounter;
import io.crate.executor.transport.TransportActionProvider;
import io.crate.lucene.LuceneQueryBuilder;
import io.crate.metadata.Functions;
import io.crate.metadata.Schemas;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.threadpool.ThreadPool;

import static io.crate.blob.v2.BlobIndex.isBlobIndex;

public class ShardCollectorProviderFactory {

    private final Schemas schemas;
    private final ClusterService clusterService;
    private final ThreadPool threadPool;
    private final TransportActionProvider transportActionProvider;
    private final BlobIndicesService blobIndicesService;

    private final Functions functions;
    private final LuceneQueryBuilder luceneQueryBuilder;
    private final NodeJobsCounter nodeJobsCounter;
    private final BigArrays bigArrays;
    private final Settings settings;

    ShardCollectorProviderFactory(ClusterService clusterService,
                                  Settings settings,
                                  Schemas schemas,
                                  ThreadPool threadPool,
                                  TransportActionProvider transportActionProvider,
                                  BlobIndicesService blobIndicesService,
                                  Functions functions,
                                  LuceneQueryBuilder luceneQueryBuilder,
                                  NodeJobsCounter nodeJobsCounter,
                                  BigArrays bigArrays) {
        this.settings = settings;
        this.schemas = schemas;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.transportActionProvider = transportActionProvider;
        this.blobIndicesService = blobIndicesService;
        this.functions = functions;
        this.luceneQueryBuilder = luceneQueryBuilder;
        this.nodeJobsCounter = nodeJobsCounter;
        this.bigArrays = bigArrays;
    }

    public ShardCollectorProvider create(IndexShard indexShard) {
        if (isBlobIndex(indexShard.shardId().getIndexName())) {
            BlobShard blobShard = blobIndicesService.blobShardSafe(indexShard.shardId());
            return new BlobShardCollectorProvider(
                blobShard,
                clusterService,
                nodeJobsCounter,
                functions,
                threadPool,
                settings,
                transportActionProvider,
                bigArrays);
        } else {
            return new LuceneShardCollectorProvider(
                schemas,
                luceneQueryBuilder,
                clusterService,
                nodeJobsCounter,
                functions,
                threadPool,
                settings,
                transportActionProvider,
                indexShard,
                bigArrays);
        }
    }
}
