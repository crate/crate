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

package io.crate.execution.engine.collect.sources;

import io.crate.blob.v2.BlobIndicesService;
import io.crate.blob.v2.BlobShard;
import io.crate.execution.engine.collect.BlobShardCollectorProvider;
import io.crate.execution.engine.collect.LuceneShardCollectorProvider;
import io.crate.execution.engine.collect.ShardCollectorProvider;
import io.crate.execution.engine.collect.files.SchemeSettings;
import io.crate.execution.engine.export.FileOutputFactory;
import io.crate.execution.jobs.NodeLimits;
import io.crate.lucene.LuceneQueryBuilder;
import io.crate.metadata.NodeContext;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.node.Node;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Map;

import static io.crate.blob.v2.BlobIndex.isBlobIndex;

@Singleton
public class ShardCollectorProviderFactory {

    private final ClusterService clusterService;
    private final ThreadPool threadPool;
    private final ElasticsearchClient elasticsearchClient;
    private final BlobIndicesService blobIndicesService;

    private final NodeContext nodeCtx;
    private final LuceneQueryBuilder luceneQueryBuilder;
    private final NodeLimits nodeJobsCounter;
    private final BigArrays bigArrays;
    private final Settings settings;
    private final CircuitBreakerService circuitBreakerService;
    private final Map<String, FileOutputFactory> fileOutputFactoryMap;
    private final Map<String, SchemeSettings> schemeSettingsMap;

    @Inject
    public ShardCollectorProviderFactory(ClusterService clusterService,
                                         CircuitBreakerService circuitBreakerService,
                                         Settings settings,
                                         ThreadPool threadPool,
                                         Node node,
                                         BlobIndicesService blobIndicesService,
                                         NodeContext nodeCtx,
                                         LuceneQueryBuilder luceneQueryBuilder,
                                         NodeLimits nodeJobsCounter,
                                         PageCacheRecycler pageCacheRecycler,
                                         Map<String, FileOutputFactory> fileOutputFactoryMap,
                                         Map<String, SchemeSettings> schemeSettingsMap) {
        this.settings = settings;
        this.circuitBreakerService = circuitBreakerService;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.elasticsearchClient = node.client();
        this.blobIndicesService = blobIndicesService;
        this.nodeCtx = nodeCtx;
        this.luceneQueryBuilder = luceneQueryBuilder;
        this.nodeJobsCounter = nodeJobsCounter;
        this.bigArrays = new BigArrays(pageCacheRecycler, circuitBreakerService, HierarchyCircuitBreakerService.QUERY, true);
        this.fileOutputFactoryMap = fileOutputFactoryMap;
        this.schemeSettingsMap = schemeSettingsMap;
    }

    public ShardCollectorProvider create(IndexShard indexShard) {
        if (isBlobIndex(indexShard.shardId().getIndexName())) {
            BlobShard blobShard = blobIndicesService.blobShardSafe(indexShard.shardId());
            return new BlobShardCollectorProvider(
                blobShard,
                clusterService,
                nodeJobsCounter,
                circuitBreakerService,
                nodeCtx,
                threadPool,
                settings,
                elasticsearchClient,
                fileOutputFactoryMap,
                schemeSettingsMap
            );
        } else {
            return new LuceneShardCollectorProvider(
                luceneQueryBuilder,
                clusterService,
                nodeJobsCounter,
                circuitBreakerService,
                nodeCtx,
                threadPool,
                settings,
                elasticsearchClient,
                indexShard,
                bigArrays,
                fileOutputFactoryMap,
                schemeSettingsMap);
        }
    }
}
