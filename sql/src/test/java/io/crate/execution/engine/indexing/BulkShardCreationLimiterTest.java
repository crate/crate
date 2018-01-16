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

package io.crate.execution.engine.indexing;

import io.crate.execution.dml.ShardRequest;
import io.crate.test.integration.CrateUnitTest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.junit.Test;

import java.io.IOException;

import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.hamcrest.Matchers.is;

public class BulkShardCreationLimiterTest extends CrateUnitTest {

    private static class DummyShardRequest extends ShardRequest<DummyShardRequest, DummyRequestItem> {
        @Override
        protected DummyRequestItem readItem(StreamInput input) throws IOException {
            return null;
        }
    }

    private static class DummyRequestItem extends ShardRequest.Item {
        DummyRequestItem(String id) {
            super(id);
        }
    }

    private static final ShardedRequests<DummyShardRequest, DummyRequestItem> SHARED_REQUESTS = new ShardedRequests<>((s, i) -> new DummyShardRequest());
    static {
        SHARED_REQUESTS.add(new DummyRequestItem("1"), "dummy", null);
    }

    @Test
    public void testNumberOfShardsGreaterEqualThanLimit() throws Exception {
        Settings settings = Settings.builder()
            .put(SETTING_NUMBER_OF_SHARDS, BulkShardCreationLimiter.MAX_NEW_SHARDS_PER_NODE)
            .put(SETTING_NUMBER_OF_REPLICAS, 0).build();
        BulkShardCreationLimiter<DummyShardRequest, DummyRequestItem> bulkShardCreationLimiter =
            new BulkShardCreationLimiter<>(settings, 1);

        assertThat(bulkShardCreationLimiter.test(SHARED_REQUESTS), is(true));
    }

    @Test
    public void testNumberOfShardsLessThanLimit() throws Exception {
        Settings settings = Settings.builder()
            .put(SETTING_NUMBER_OF_SHARDS, BulkShardCreationLimiter.MAX_NEW_SHARDS_PER_NODE - 1)
            .put(SETTING_NUMBER_OF_REPLICAS, 0).build();
        BulkShardCreationLimiter<DummyShardRequest, DummyRequestItem> bulkShardCreationLimiter =
            new BulkShardCreationLimiter<>(settings, 1);

        assertThat(bulkShardCreationLimiter.test(SHARED_REQUESTS), is(false));
    }

    @Test
    public void testNumberOfShardsLessThanLimitWithTwoNodes() throws Exception {
        Settings settings = Settings.builder()
            .put(SETTING_NUMBER_OF_SHARDS, BulkShardCreationLimiter.MAX_NEW_SHARDS_PER_NODE)
            .put(SETTING_NUMBER_OF_REPLICAS, 0).build();
        BulkShardCreationLimiter<DummyShardRequest, DummyRequestItem> bulkShardCreationLimiter =
            new BulkShardCreationLimiter<>(settings, 2);

        assertThat(bulkShardCreationLimiter.test(SHARED_REQUESTS), is(false));
    }
}
