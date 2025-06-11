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

package io.crate.execution.engine.indexing;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.UUID;

import org.elasticsearch.test.ESTestCase;
import org.junit.Before;
import org.junit.Test;

import io.crate.data.breaker.RamAccounting;
import io.crate.execution.dml.delete.ShardDeleteRequest;
import io.crate.metadata.PartitionName;
import io.crate.metadata.RelationName;

public class BulkShardCreationLimiterTest extends ESTestCase {

    private ShardedRequests<ShardDeleteRequest, ShardDeleteRequest.Item> shardedRequests;

    @Before
    public void setup() {
        RelationName relationName = new RelationName("doc", "tbl");
        UUID jobId = UUID.randomUUID();
        shardedRequests = new ShardedRequests<>(
            shardId -> new ShardDeleteRequest(shardId, jobId),
            RamAccounting.NO_ACCOUNTING
        );
        shardedRequests.add(
            new ShardDeleteRequest.Item("id1"),
            new PartitionName(relationName, List.of("1")).asIndexName(),
            null,
            RowSourceInfo.EMPTY_INSTANCE
        );
    }

    @Test
    public void testNumberOfShardsGreaterEqualThanLimit() throws Exception {
        int numberOfShards = BulkShardCreationLimiter.MAX_NEW_SHARDS_PER_NODE;
        int numberOfReplicas = 0;
        BulkShardCreationLimiter bulkShardCreationLimiter =
            new BulkShardCreationLimiter(numberOfShards, numberOfReplicas, 1);

        assertThat(bulkShardCreationLimiter.test(shardedRequests)).isTrue();
    }

    @Test
    public void testNumberOfShardsLessThanLimit() throws Exception {
        int numberOfShards = BulkShardCreationLimiter.MAX_NEW_SHARDS_PER_NODE - 1;
        int numberOfReplicas = 0;
        BulkShardCreationLimiter bulkShardCreationLimiter =
            new BulkShardCreationLimiter(numberOfShards, numberOfReplicas, 1);

        assertThat(bulkShardCreationLimiter.test(shardedRequests)).isFalse();
    }

    @Test
    public void testNumberOfShardsLessThanLimitWithTwoNodes() throws Exception {
        int numberOfShards = BulkShardCreationLimiter.MAX_NEW_SHARDS_PER_NODE - 1;
        int numberOfReplicas = 0;
        BulkShardCreationLimiter bulkShardCreationLimiter =
            new BulkShardCreationLimiter(numberOfShards, numberOfReplicas, 2);

        assertThat(bulkShardCreationLimiter.test(shardedRequests)).isFalse();
    }
}
