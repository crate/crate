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

import static org.hamcrest.Matchers.equalTo;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.junit.Test;

import io.crate.integrationtests.SQLTransportIntegrationTest;

public class RetentionLeaseSyncIT extends SQLTransportIntegrationTest  {

    @Test
    public void testRetentionLeasesSyncedOnAdd() throws Exception {
        final int numberOfReplicas = 2 - scaledRandomIntBetween(0, 2);
        internalCluster().ensureAtLeastNumDataNodes(1 + numberOfReplicas);
        execute(
            "create table doc.tbl (x int) clustered into 1 shards " +
            "with (number_of_replicas = ?, \"soft_deletes.enabled\" = true)",
            new Object[] { numberOfReplicas }
        );
        ensureGreen("tbl");
        final String primaryShardNodeId = clusterService().state().routingTable().index("tbl").shard(0).primaryShard().currentNodeId();
        final String primaryShardNodeName = clusterService().state().nodes().get(primaryShardNodeId).getName();
        final IndexShard primary = internalCluster()
            .getInstance(IndicesService.class, primaryShardNodeName)
            .getShardOrNull(new ShardId(resolveIndex("tbl"), 0));
        // we will add multiple retention leases and expect to see them synced to all replicas
        final int length = randomIntBetween(1, 8);
        final Map<String, RetentionLease> currentRetentionLeases = new HashMap<>();
        for (int i = 0; i < length; i++) {
            final String id = randomValueOtherThanMany(currentRetentionLeases.keySet()::contains, () -> randomAlphaOfLength(8));
            final long retainingSequenceNumber = randomLongBetween(SequenceNumbers.NO_OPS_PERFORMED, Long.MAX_VALUE);
            final String source = randomAlphaOfLength(8);
            final CountDownLatch latch = new CountDownLatch(1);
            final ActionListener<ReplicationResponse> listener = ActionListener.wrap(r -> latch.countDown(), e -> fail(e.toString()));
            currentRetentionLeases.put(id, primary.addRetentionLease(id, retainingSequenceNumber, source, listener));
            latch.await();

            // check retention leases have been committed on the primary
            final Collection<RetentionLease> primaryCommittedRetentionLeases = RetentionLease.decodeRetentionLeases(
                primary.acquireLastIndexCommit(false)
                    .getIndexCommit()
                    .getUserData()
                    .get(Engine.RETENTION_LEASES)
            );
            assertThat(currentRetentionLeases, equalTo(toMap(primaryCommittedRetentionLeases)));

            // check current retention leases have been synced to all replicas
            for (final ShardRouting replicaShard : clusterService().state().routingTable().index("tbl").shard(0).replicaShards()) {
                final String replicaShardNodeId = replicaShard.currentNodeId();
                final String replicaShardNodeName = clusterService().state().nodes().get(replicaShardNodeId).getName();
                final IndexShard replica = internalCluster()
                    .getInstance(IndicesService.class, replicaShardNodeName)
                    .getShardOrNull(new ShardId(resolveIndex("tbl"), 0));
                final Map<String, RetentionLease> retentionLeasesOnReplica = toMap(replica.getRetentionLeases());
                assertThat(retentionLeasesOnReplica, equalTo(currentRetentionLeases));

                // check retention leases have been committed on the replica
                final Collection<RetentionLease> replicaCommittedRetentionLeases = RetentionLease.decodeRetentionLeases(
                        replica.acquireLastIndexCommit(false).getIndexCommit().getUserData().get(Engine.RETENTION_LEASES));
                assertThat(currentRetentionLeases, equalTo(toMap(replicaCommittedRetentionLeases)));
            }
        }
    }

    private static Map<String, RetentionLease> toMap(final Collection<RetentionLease> replicaCommittedRetentionLeases) {
        return replicaCommittedRetentionLeases.stream().collect(Collectors.toMap(RetentionLease::id, Function.identity()));
    }

}
