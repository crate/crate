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

import io.crate.breaker.RamAccounting;
import io.crate.execution.dml.ShardRequest;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;


import static org.hamcrest.Matchers.is;

public class BulkShardCreationLimiterTest extends ESTestCase {

    private static class DummyShardRequest extends ShardRequest<DummyShardRequest, DummyRequestItem> {
    }

    private static class DummyRequestItem extends ShardRequest.Item {
        DummyRequestItem(String id) {
            super(id);
        }
    }

    private static final ShardedRequests<DummyShardRequest, DummyRequestItem> SHARED_REQUESTS = new ShardedRequests<>(
        (s) -> new DummyShardRequest(),
        RamAccounting.NO_ACCOUNTING
    );
    static {
        SHARED_REQUESTS.add(new DummyRequestItem("1"), 10, "dummy", null, RowSourceInfo.EMPTY_INSTANCE);
    }

    @Test
    public void testNumberOfShardsGreaterEqualThanLimit() throws Exception {
        int numberOfShards = BulkShardCreationLimiter.MAX_NEW_SHARDS_PER_NODE;
        int numberOfReplicas = 0;
        BulkShardCreationLimiter bulkShardCreationLimiter =
            new BulkShardCreationLimiter(numberOfShards, numberOfReplicas, 1);

        assertThat(bulkShardCreationLimiter.test(SHARED_REQUESTS), is(true));
    }

    @Test
    public void testNumberOfShardsLessThanLimit() throws Exception {
        int numberOfShards = BulkShardCreationLimiter.MAX_NEW_SHARDS_PER_NODE - 1;
        int numberOfReplicas = 0;
        BulkShardCreationLimiter bulkShardCreationLimiter =
            new BulkShardCreationLimiter(numberOfShards, numberOfReplicas, 1);

        assertThat(bulkShardCreationLimiter.test(SHARED_REQUESTS), is(false));
    }

    @Test
    public void testNumberOfShardsLessThanLimitWithTwoNodes() throws Exception {
        int numberOfShards = BulkShardCreationLimiter.MAX_NEW_SHARDS_PER_NODE - 1;
        int numberOfReplicas = 0;
        BulkShardCreationLimiter bulkShardCreationLimiter =
            new BulkShardCreationLimiter(numberOfShards, numberOfReplicas, 2);

        assertThat(bulkShardCreationLimiter.test(SHARED_REQUESTS), is(false));
    }
}
