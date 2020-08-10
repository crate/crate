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

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;

import java.io.Closeable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Ignore;
import org.junit.Test;

import io.crate.common.unit.TimeValue;
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
            final long retainingSequenceNumber = randomLongBetween(0, Long.MAX_VALUE);
            final String source = randomAlphaOfLength(8);
            final CountDownLatch latch = new CountDownLatch(1);
            final ActionListener<ReplicationResponse> listener = ActionListener.wrap(r -> latch.countDown(), e -> fail(e.toString()));
            // simulate a peer recovery which locks the soft deletes policy on the primary
            final Closeable retentionLock = randomBoolean() ? primary.acquireRetentionLock() : () -> {};
            currentRetentionLeases.put(id, primary.addRetentionLease(id, retainingSequenceNumber, source, listener));
            latch.await();
            retentionLock.close();

            // check retention leases have been committed on the primary
            final RetentionLeases primaryCommittedRetentionLeases = RetentionLeases.decodeRetentionLeases(
                primary.acquireLastIndexCommit(false)
                    .getIndexCommit()
                    .getUserData()
                    .get(Engine.RETENTION_LEASES)
            );
            assertThat(currentRetentionLeases, equalTo(RetentionLeases.toMap(primaryCommittedRetentionLeases)));

            // check current retention leases have been synced to all replicas
            for (final ShardRouting replicaShard : clusterService().state().routingTable().index("tbl").shard(0).replicaShards()) {
                final String replicaShardNodeId = replicaShard.currentNodeId();
                final String replicaShardNodeName = clusterService().state().nodes().get(replicaShardNodeId).getName();
                final IndexShard replica = internalCluster()
                    .getInstance(IndicesService.class, replicaShardNodeName)
                    .getShardOrNull(new ShardId(resolveIndex("tbl"), 0));
                final Map<String, RetentionLease> retentionLeasesOnReplica = RetentionLeases.toMap(replica.getRetentionLeases());
                assertThat(retentionLeasesOnReplica, equalTo(currentRetentionLeases));

                // check retention leases have been committed on the replica
                RetentionLeases replicaCommittedRetentionLeases = RetentionLeases.decodeRetentionLeases(
                    replica.acquireLastIndexCommit(false).getIndexCommit().getUserData().get(Engine.RETENTION_LEASES));
                assertThat(currentRetentionLeases, equalTo(RetentionLeases.toMap(replicaCommittedRetentionLeases)));
            }
        }
    }

    @Test
    public void testRetentionLeasesSyncOnExpiration() throws Exception {
        final int numberOfReplicas = 2 - scaledRandomIntBetween(0, 2);
        internalCluster().ensureAtLeastNumDataNodes(1 + numberOfReplicas);
        final long estimatedTimeIntervalMillis = ThreadPool.ESTIMATED_TIME_INTERVAL_SETTING.get(Settings.EMPTY).millis();
        final TimeValue retentionLeaseTimeToLive =
                TimeValue.timeValueMillis(randomLongBetween(estimatedTimeIntervalMillis, 2 * estimatedTimeIntervalMillis));
        execute(
            "create table doc.tbl (x int) clustered into 1 shards " +
            "with (" +
            "   number_of_replicas = ?, " +
            "   \"soft_deletes.retention_lease.sync_interval\" = ?)",
            new Object[] {
                numberOfReplicas,
                retentionLeaseTimeToLive.getStringRep()
            }
        );
        ensureGreen("tbl");
        final String primaryShardNodeId = clusterService().state().routingTable().index("tbl").shard(0).primaryShard().currentNodeId();
        final String primaryShardNodeName = clusterService().state().nodes().get(primaryShardNodeId).getName();
        final IndexShard primary = internalCluster()
                .getInstance(IndicesService.class, primaryShardNodeName)
                .getShardOrNull(new ShardId(resolveIndex("tbl"), 0));
        // we will add multiple retention leases, wait for some to expire, and assert a consistent view between the primary and the replicas
        final int length = randomIntBetween(1, 8);
        for (int i = 0; i < length; i++) {
            // update the index for retention leases to live a long time
            final AcknowledgedResponse longTtlResponse = client().admin()
                .indices()
                .prepareUpdateSettings("tbl")
                .setSettings(Settings.builder()
                    .putNull(IndexSettings.INDEX_SOFT_DELETES_RETENTION_LEASE_SETTING.getKey())
                    .build())
                .get();
            assertTrue(longTtlResponse.isAcknowledged());

            final String id = randomAlphaOfLength(8);
            final long retainingSequenceNumber = randomLongBetween(0, Long.MAX_VALUE);
            final String source = randomAlphaOfLength(8);
            final CountDownLatch latch = new CountDownLatch(1);
            final ActionListener<ReplicationResponse> listener = ActionListener.wrap(r -> latch.countDown(), e -> fail(e.toString()));
            final RetentionLease currentRetentionLease = primary.addRetentionLease(id, retainingSequenceNumber, source, listener);
            final long now = System.nanoTime();
            latch.await();

            // check current retention leases have been synced to all replicas
            for (final ShardRouting replicaShard : clusterService().state().routingTable().index("tbl").shard(0).replicaShards()) {
                final String replicaShardNodeId = replicaShard.currentNodeId();
                final String replicaShardNodeName = clusterService().state().nodes().get(replicaShardNodeId).getName();
                final IndexShard replica = internalCluster()
                        .getInstance(IndicesService.class, replicaShardNodeName)
                        .getShardOrNull(new ShardId(resolveIndex("tbl"), 0));
                assertThat(replica.getRetentionLeases().leases(), anyOf(empty(), contains(currentRetentionLease)));
            }

            // update the index for retention leases to short a long time, to force expiration
            final AcknowledgedResponse shortTtlResponse = client().admin()
                .indices()
                .prepareUpdateSettings("tbl")
                .setSettings(
                    Settings.builder()
                        .put(IndexSettings.INDEX_SOFT_DELETES_RETENTION_LEASE_SETTING.getKey(), retentionLeaseTimeToLive)
                        .build())
                .get();
            assertTrue(shortTtlResponse.isAcknowledged());

            // sleep long enough that the current retention lease has expired
            final long later = System.nanoTime();
            Thread.sleep(Math.max(0, retentionLeaseTimeToLive.millis() - TimeUnit.NANOSECONDS.toMillis(later - now)));
            assertBusy(() -> assertThat(primary.getRetentionLeases().leases(), empty()));

            // now that all retention leases are expired should have been synced to all replicas
            assertBusy(() -> {
                for (final ShardRouting replicaShard : clusterService().state().routingTable().index("tbl").shard(0).replicaShards()) {
                    final String replicaShardNodeId = replicaShard.currentNodeId();
                    final String replicaShardNodeName = clusterService().state().nodes().get(replicaShardNodeId).getName();
                    final IndexShard replica = internalCluster()
                        .getInstance(IndicesService.class, replicaShardNodeName)
                        .getShardOrNull(new ShardId(resolveIndex("tbl"), 0));

                    assertThat(replica.getRetentionLeases().leases(), empty());
                }
            });
        }
    }

    @Test
    @Ignore("Flaky, later ES patches should fix it")
    public void testRetentionLeasesSyncOnRecovery() throws Exception {
        /*
         * We effectively disable the background sync to ensure that the retention leases are not synced in the background so that the only
         * source of retention leases on the replicas would be from the commit point and recovery.
         */
        execute(
            "create table doc.tbl (x int) clustered into 1 shards " +
            "with (" +
            "   number_of_replicas = 0, " +
            "   \"soft_deletes.enabled\" = true, " +
            "   \"soft_deletes.retention_lease.sync_interval\" = ?)",
            new Object[] {
                TimeValue.timeValueHours(24).getStringRep()
            }
        );
        ensureYellow("tbl");
        // exclude the replicas from being allocated
        allowNodes("tbl", 1);
        final int numberOfReplicas = 1;
        execute("alter table doc.tbl set (number_of_replicas = 1)");
        final String primaryShardNodeId = clusterService().state().routingTable().index("tbl").shard(0).primaryShard().currentNodeId();
        final String primaryShardNodeName = clusterService().state().nodes().get(primaryShardNodeId).getName();
        final IndexShard primary = internalCluster()
            .getInstance(IndicesService.class, primaryShardNodeName)
            .getShardOrNull(new ShardId(resolveIndex("tbl"), 0));
        final int length = randomIntBetween(1, 8);
        final Map<String, RetentionLease> currentRetentionLeases = new HashMap<>();
        for (int i = 0; i < length; i++) {
            final String id = randomValueOtherThanMany(currentRetentionLeases.keySet()::contains, () -> randomAlphaOfLength(8));
            final long retainingSequenceNumber = randomLongBetween(0, Long.MAX_VALUE);
            final String source = randomAlphaOfLength(8);
            final CountDownLatch latch = new CountDownLatch(1);
            final ActionListener<ReplicationResponse> listener = ActionListener.wrap(r -> latch.countDown(), e -> fail(e.toString()));
            currentRetentionLeases.put(id, primary.addRetentionLease(id, retainingSequenceNumber, source, listener));
            latch.await();
            /*
             * Now renew the leases; since we do not flush immediately on renewal, this means that the latest retention leases will not be
             * in the latest commit point and therefore not transferred during the file-copy phase of recovery.
             */
            currentRetentionLeases.put(id, primary.renewRetentionLease(id, retainingSequenceNumber, source));
        }

        // now allow the replicas to be allocated and wait for recovery to finalize
        allowNodes("tbl", 1 + numberOfReplicas);
        ensureGreen("tbl");

        // check current retention leases have been synced to all replicas
        for (final ShardRouting replicaShard : clusterService().state().routingTable().index("tbl").shard(0).replicaShards()) {
            final String replicaShardNodeId = replicaShard.currentNodeId();
            final String replicaShardNodeName = clusterService().state().nodes().get(replicaShardNodeId).getName();
            final IndexShard replica = internalCluster()
                .getInstance(IndicesService.class, replicaShardNodeName)
                .getShardOrNull(new ShardId(resolveIndex("tbl"), 0));
            final Map<String, RetentionLease> retentionLeasesOnReplica = RetentionLeases.toMap(replica.getRetentionLeases());
            assertThat(retentionLeasesOnReplica, equalTo(currentRetentionLeases));

            // check retention leases have been committed on the replica
            final RetentionLeases replicaCommittedRetentionLeases = RetentionLeases.decodeRetentionLeases(
                replica.acquireLastIndexCommit(false).getIndexCommit().getUserData().get(Engine.RETENTION_LEASES));
            assertThat(currentRetentionLeases, equalTo(RetentionLeases.toMap(replicaCommittedRetentionLeases)));
        }
    }
}
