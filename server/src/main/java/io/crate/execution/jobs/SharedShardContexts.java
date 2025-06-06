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

package io.crate.execution.jobs;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.jetbrains.annotations.VisibleForTesting;

import com.carrotsearch.hppc.IntIndexedContainer;

import io.crate.execution.engine.fetch.FetchTask;
import io.crate.metadata.IndexName;

public class SharedShardContexts {

    private final IndicesService indicesService;
    private final BiFunction<ShardId, Engine.Searcher, Engine.Searcher> wrapSearcher;
    @VisibleForTesting
    final Map<ShardId, SharedShardContext> allocatedShards = new HashMap<>();
    @VisibleForTesting
    int readerId = 0;

    public SharedShardContexts(IndicesService indicesService,
                               BiFunction<ShardId, Engine.Searcher, Engine.Searcher> wrapSearcher) {
        this.indicesService = indicesService;
        this.wrapSearcher = wrapSearcher;
    }

    public CompletableFuture<Void> maybeRefreshReaders(Metadata metadata,
                                                       Map<String, IntIndexedContainer> shardsByIndex,
                                                       Map<String, Integer> bases) {
        ArrayList<CompletableFuture<Boolean>> refreshActions = new ArrayList<>();
        for (var entry : shardsByIndex.entrySet()) {
            String indexName = entry.getKey();
            Integer base = bases.get(indexName);
            if (base == null) {
                continue;
            }
            IndexMetadata indexMetadata = metadata.index(indexName);
            if (indexMetadata == null) {
                if (IndexName.isPartitioned(indexName)) {
                    continue;
                }
                refreshActions.add(CompletableFuture.failedFuture(new IndexNotFoundException(indexName)));
                continue;
            }
            IndexService indexService = indicesService.indexService(indexMetadata.getIndex());
            if (indexService == null) {
                if (!IndexName.isPartitioned(indexName)) {
                    refreshActions.add(CompletableFuture.failedFuture(new IndexNotFoundException(indexName)));
                }
                continue;
            }
            for (var shardCursor : entry.getValue()) {
                int shardId = shardCursor.value;
                IndexShard shard = indexService.getShard(shardId);
                refreshActions.add(shard.awaitShardSearchActive());
            }
        }
        return CompletableFuture.allOf(refreshActions.toArray(CompletableFuture[]::new));
    }

    /**
     * Gets a context for the provided shardId and readerId.
     * Creates the context if missing.
     *
     * This is intended to be called by {@link FetchTask} to create the contexts for shared by subsequent collect/fetch phases.
     * Getting an existing context is an edge case for when there are multiple fetch tasks over the same query.
     * (self-join involving virtual tables with limits and aggregations)
     **/
    public SharedShardContext prepareContext(ShardId shardId, int readerId) throws IndexNotFoundException {
        SharedShardContext sharedShardContext = allocatedShards.get(shardId);
        if (sharedShardContext == null) {
            IndexService indexService = indicesService.indexServiceSafe(shardId.getIndex());
            synchronized (this) {
                sharedShardContext = allocatedShards.get(shardId);
                if (sharedShardContext == null) {
                    sharedShardContext = new SharedShardContext(indexService, shardId, readerId, wrapSearcher);
                    allocatedShards.put(shardId, sharedShardContext);
                }
            }
        }
        assert sharedShardContext.readerId() == readerId :
            "FetchTask cannot create 2 contexts with same shardId and different readerId.\n" +
            "readerId is computed deterministically based on shard id";
        return sharedShardContext;
    }

    /**
     * Gets a context for the provided shardId.
     * Creates the context if missing with a readerId that auto-increments.
     **/
    public SharedShardContext getOrCreateContext(ShardId shardId) throws IndexNotFoundException {
        SharedShardContext sharedShardContext = allocatedShards.get(shardId);
        if (sharedShardContext == null) {
            IndexService indexService = indicesService.indexServiceSafe(shardId.getIndex());
            synchronized (this) {
                sharedShardContext = allocatedShards.get(shardId);
                if (sharedShardContext == null) {
                    sharedShardContext = new SharedShardContext(indexService, shardId, readerId, wrapSearcher);
                    allocatedShards.put(shardId, sharedShardContext);
                    readerId++;
                }
            }
        }
        return sharedShardContext;
    }

    @Override
    public String toString() {
        return "SharedShardContexts{" +
               "allocatedShards=" + allocatedShards.keySet() +
               '}';
    }
}
