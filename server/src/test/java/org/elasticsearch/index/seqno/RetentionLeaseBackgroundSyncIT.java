/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.seqno;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.IntegTestCase;
import org.junit.Test;

public class RetentionLeaseBackgroundSyncIT extends IntegTestCase {

    @Test
    public void testBackgroundRetentionLeaseSync() throws Exception {
        final int numberOfReplicas = 2 - scaledRandomIntBetween(0, 2);
        cluster().ensureAtLeastNumDataNodes(1 + numberOfReplicas);
        execute(
            "create table doc.tbl (x int) clustered into 1 shards " +
            "with (" +
            "   \"soft_deletes.retention_lease.sync_interval\" = '1s', " +
            "   \"soft_deletes.enabled\" = true, " +
            "   number_of_replicas = ?)",
            new Object[] { numberOfReplicas }
        );
        ensureGreen("tbl");
        final String primaryShardNodeId = clusterService().state().routingTable().index("tbl").shard(0).primaryShard().currentNodeId();
        final String primaryShardNodeName = clusterService().state().nodes().get(primaryShardNodeId).getName();
        final IndexShard primary = cluster()
                .getInstance(IndicesService.class, primaryShardNodeName)
                .getShardOrNull(new ShardId(resolveIndex("tbl"), 0));
        // we will add multiple retention leases and expect to see them synced to all replicas
        final int length = randomIntBetween(1, 8);
        final Map<String, RetentionLease> currentRetentionLeases = new HashMap<>(length);
        final List<String> ids = new ArrayList<>(length);
        for (int i = 0; i < length; i++) {
            final String id = randomValueOtherThanMany(currentRetentionLeases.keySet()::contains, () -> randomAlphaOfLength(8));
            ids.add(id);
            final long retainingSequenceNumber = randomLongBetween(0, Long.MAX_VALUE);
            final String source = randomAlphaOfLength(8);
            final CountDownLatch latch = new CountDownLatch(1);
            // put a new lease
            currentRetentionLeases.put(
                    id,
                    primary.addRetentionLease(id, retainingSequenceNumber, source, ActionListener.wrap(latch::countDown)));
            latch.await();
            // now renew all existing leases; we expect to see these synced to the replicas
            for (int j = 0; j <= i; j++) {
                currentRetentionLeases.put(
                    ids.get(j),
                    primary.renewRetentionLease(
                        ids.get(j),
                        randomLongBetween(currentRetentionLeases.get(ids.get(j)).retainingSequenceNumber(), Long.MAX_VALUE),
                        source));
            }
            assertBusy(() -> {
                // check all retention leases have been synced to all replicas
                for (final ShardRouting replicaShard : clusterService().state().routingTable().index("tbl").shard(0).replicaShards()) {
                    final String replicaShardNodeId = replicaShard.currentNodeId();
                    final String replicaShardNodeName = clusterService().state().nodes().get(replicaShardNodeId).getName();
                    final IndexShard replica = cluster()
                        .getInstance(IndicesService.class, replicaShardNodeName)
                        .getShardOrNull(new ShardId(resolveIndex("tbl"), 0));
                    assertThat(replica.getRetentionLeases()).isEqualTo(primary.getRetentionLeases());
                }
            });
        }
    }

}
