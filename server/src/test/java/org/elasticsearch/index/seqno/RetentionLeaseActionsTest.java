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

package org.elasticsearch.index.seqno;

import static io.crate.testing.Asserts.assertThat;
import static org.mockito.Mockito.mock;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardsIterator;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.transport.TransportService;
import org.junit.Test;

import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;

public class RetentionLeaseActionsTest extends CrateDummyClusterServiceUnitTest {

    @Test
    public void test_bwc_can_resolve_shards_using_index_names_before_6_1() throws Exception {
        SQLExecutor.builder(clusterService)
            .build()
            .addTable("CREATE TABLE doc.t1 (id INT) CLUSTERED INTO 1 SHARDS")
            .startShards("doc.t1");

        ShardId shardId = new ShardId(new Index("t1", "unknown-uuid"), 0);

        RetentionLeaseActions.Add.TransportAction retentionLeaseActionAdd =
            new RetentionLeaseActions.Add.TransportAction(
                THREAD_POOL,
                clusterService,
                mock(TransportService.class),
                mock(IndicesService.class)
            );
        RetentionLeaseActions.AddOrRenewRequest request = new RetentionLeaseActions.AddOrRenewRequest(
            shardId,
            "lease-id",
            1,
            "lease source"
        );

        try (var out = new BytesStreamOutput()) {
            out.setVersion(Version.V_6_0_0);
            request.writeTo(out);
            try (var in = out.bytes().streamInput()) {
                in.setVersion(Version.V_6_0_0);
                RetentionLeaseActions.AddOrRenewRequest request1 = new RetentionLeaseActions.AddOrRenewRequest(in);

                ShardsIterator shardsIt = retentionLeaseActionAdd.shards(clusterService.state(), request1);
                assertThat(shardsIt).isNotNull();
                ShardRouting shardRouting = shardsIt.nextOrNull();
                assertThat(shardRouting).isNotNull();
                assertThat(shardRouting.id()).isEqualTo(0);
            }
        }
    }
}
