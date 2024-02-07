/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.junit.Test;

public class SharedShardContextsTest {

    @Test
    public void test_getOrCreateContext_is_synchronized() {
        ShardId shardId1 = new ShardId("dummy1", "dummyIndex1", 1);
        ShardId shardId2 = new ShardId("dummy2", "dummyIndex2", 2);
        ShardId shardId3 = new ShardId("dummy3", "dummyIndex3", 3);

        IndicesService indicesService = mock(IndicesService.class);
        IndexService indexService = mock(IndexService.class);
        when(indicesService.indexServiceSafe(any())).thenReturn(indexService);

        SharedShardContexts sharedShardContexts = new SharedShardContexts(indicesService, null);

        Stream.of(
            shardId1, shardId1, shardId1,
            shardId2, shardId2, shardId2,
            shardId3, shardId3, shardId3
        ).parallel().forEach(sharedShardContexts::getOrCreateContext);

        assertThat(sharedShardContexts.allocatedShards).containsOnlyKeys(List.of(shardId1, shardId2, shardId3));
        var readerIds = sharedShardContexts.allocatedShards.values().stream().map(SharedShardContext::readerId).toList();
        assertThat(readerIds).containsExactlyInAnyOrder(0, 1, 2);
        assertThat(sharedShardContexts.readerId).isEqualTo(3);
    }

    @Test
    public void test_allocatedShards_accesses_are_synchronized() throws ExecutionException, InterruptedException {
        IndicesService indicesService = mock(IndicesService.class);
        IndexService indexService = mock(IndexService.class);
        when(indicesService.indexServiceSafe(any())).thenReturn(indexService);

        SharedShardContexts sharedShardContexts = new SharedShardContexts(indicesService, null);

        int[] idx = new int[] {0};
        var shards1 = Stream.generate(() -> {
            idx[0]++;
            return new ShardId("dummy" + idx[0], "dummyIndex" + idx[0], idx[0]);
        }).limit(20).toList();
        var shards2 = Stream.generate(() -> {
            idx[0]++;
            return new ShardId("dummy" + idx[0], "dummyIndex" + idx[0], idx[0]);
        }).limit(20).toList();

        var cf1 = CompletableFuture.runAsync(
            () -> shards1.stream().parallel().forEach(shardId -> sharedShardContexts.createContext(shardId, shardId.id()))
        );
        var cf2 = CompletableFuture.runAsync(
            () -> shards2.stream().parallel().forEach(sharedShardContexts::getOrCreateContext)
        );
        cf1.get();
        cf2.get();

        List<ShardId> allShards = new ArrayList<>();
        allShards.addAll(shards1);
        allShards.addAll(shards2);
        assertThat(sharedShardContexts.allocatedShards.size()).isEqualTo(allShards.size());
        assertThat(sharedShardContexts.allocatedShards).containsOnlyKeys(allShards);
    }
}
