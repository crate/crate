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
import static org.assertj.core.api.Assertions.fail;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.recovery.PeerRecoveryTargetService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.IntegTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.AbstractSimpleTransportTestCase;
import org.elasticsearch.transport.TransportService;
import org.junit.After;
import org.junit.Test;

import io.crate.common.collections.Lists;
import io.crate.common.unit.TimeValue;

public class RetentionLeaseIT extends IntegTestCase  {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Lists.concat(super.nodePlugins(), MockTransportService.TestPlugin.class);
    }

    @After
    public void resetSettings() {
        execute("reset global \"indices.recovery.retry_delay_network\"");
    }

    @Test
    public void testRetentionLeasesSyncedOnAdd() throws Exception {
        final int numberOfReplicas = 2 - scaledRandomIntBetween(0, 2);
        cluster().ensureAtLeastNumDataNodes(1 + numberOfReplicas);
        execute(
            "create table doc.tbl (x int) clustered into 1 shards " +
            "with (number_of_replicas = ?, \"soft_deletes.enabled\" = true)",
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
        final Map<String, RetentionLease> currentRetentionLeases = new HashMap<>();
        for (int i = 0; i < length; i++) {
            final String id = randomValueOtherThanMany(currentRetentionLeases.keySet()::contains, () -> randomAlphaOfLength(8));
            final long retainingSequenceNumber = randomLongBetween(0, Long.MAX_VALUE);
            final String source = randomAlphaOfLength(8);
            final CountDownLatch latch = new CountDownLatch(1);
            final ActionListener<ReplicationResponse> listener = ActionListener.wrap(r -> latch.countDown(), e -> fail(e.toString()));
            // simulate a peer recovery which locks the soft deletes policy on the primary
            final Closeable retentionLock = randomBoolean() ? primary.acquireHistoryRetentionLock(Engine.HistorySource.INDEX) : () -> {};
            currentRetentionLeases.put(id, primary.addRetentionLease(id, retainingSequenceNumber, source, listener));
            latch.await();
            retentionLock.close();

            // check retention leases have been written on the primary
            assertThat(currentRetentionLeases).isEqualTo(RetentionLeaseUtils.toMapExcludingPeerRecoveryRetentionLeases(primary.loadRetentionLeases()));

            // check current retention leases have been synced to all replicas
            for (final ShardRouting replicaShard : clusterService().state().routingTable().index("tbl").shard(0).replicaShards()) {
                final String replicaShardNodeId = replicaShard.currentNodeId();
                final String replicaShardNodeName = clusterService().state().nodes().get(replicaShardNodeId).getName();
                final IndexShard replica = cluster()
                    .getInstance(IndicesService.class, replicaShardNodeName)
                    .getShardOrNull(new ShardId(resolveIndex("tbl"), 0));
                final Map<String, RetentionLease> retentionLeasesOnReplica =
                    RetentionLeaseUtils.toMapExcludingPeerRecoveryRetentionLeases(replica.getRetentionLeases());
                assertThat(retentionLeasesOnReplica).isEqualTo(currentRetentionLeases);

                // check retention leases have been written on the replica
                assertThat(currentRetentionLeases).isEqualTo(RetentionLeaseUtils.toMapExcludingPeerRecoveryRetentionLeases(replica.loadRetentionLeases()));
            }
        }
    }

    @Test
    public void testRetentionLeaseSyncedOnRemove() throws Exception {
        final int numberOfReplicas = 2 - scaledRandomIntBetween(0, 2);
        cluster().ensureAtLeastNumDataNodes(1 + numberOfReplicas);

        execute("create table doc.tbl (x int) clustered into 1 shards with (number_of_replicas = ?)",
                new Object[]{numberOfReplicas});

        ensureGreen("tbl");
        final String primaryShardNodeId = clusterService().state().routingTable().index("tbl").shard(0).primaryShard().currentNodeId();
        final String primaryShardNodeName = clusterService().state().nodes().get(primaryShardNodeId).getName();
        final IndexShard primary = cluster()
            .getInstance(IndicesService.class, primaryShardNodeName)
            .getShardOrNull(new ShardId(resolveIndex("tbl"), 0));
        final int length = randomIntBetween(1, 8);
        final Map<String, RetentionLease> currentRetentionLeases = new LinkedHashMap<>();
        for (int i = 0; i < length; i++) {
            final String id = randomValueOtherThanMany(currentRetentionLeases.keySet()::contains, () -> randomAlphaOfLength(8));
            final long retainingSequenceNumber = randomLongBetween(0, Long.MAX_VALUE);
            final String source = randomAlphaOfLength(8);
            final CountDownLatch latch = new CountDownLatch(1);
            final ActionListener<ReplicationResponse> listener = countDownLatchListener(latch);
            // simulate a peer recovery which locks the soft deletes policy on the primary
            final Closeable retentionLock = randomBoolean() ? primary.acquireHistoryRetentionLock(Engine.HistorySource.INDEX) : () -> {};
            currentRetentionLeases.put(id, primary.addRetentionLease(id, retainingSequenceNumber, source, listener));
            latch.await();
            retentionLock.close();
        }

        for (int i = 0; i < length; i++) {
            final String id = randomFrom(currentRetentionLeases.keySet());
            final CountDownLatch latch = new CountDownLatch(1);
            primary.removeRetentionLease(id, countDownLatchListener(latch));
            // simulate a peer recovery which locks the soft deletes policy on the primary
            final Closeable retentionLock = randomBoolean() ? primary.acquireHistoryRetentionLock(Engine.HistorySource.INDEX) : () -> {};
            currentRetentionLeases.remove(id);
            latch.await();
            retentionLock.close();

            // check retention leases have been written on the primary
            assertThat(currentRetentionLeases).isEqualTo(RetentionLeaseUtils.toMapExcludingPeerRecoveryRetentionLeases(primary.loadRetentionLeases()));

            // check current retention leases have been synced to all replicas
            for (final ShardRouting replicaShard : clusterService().state().routingTable().index("tbl").shard(0).replicaShards()) {
                final String replicaShardNodeId = replicaShard.currentNodeId();
                final String replicaShardNodeName = clusterService().state().nodes().get(replicaShardNodeId).getName();
                final IndexShard replica = cluster()
                    .getInstance(IndicesService.class, replicaShardNodeName)
                    .getShardOrNull(new ShardId(resolveIndex("tbl"), 0));
                final Map<String, RetentionLease> retentionLeasesOnReplica =
                    RetentionLeaseUtils.toMapExcludingPeerRecoveryRetentionLeases(replica.getRetentionLeases());
                assertThat(retentionLeasesOnReplica).isEqualTo(currentRetentionLeases);

                // check retention leases have been written on the replica
                assertThat(currentRetentionLeases).isEqualTo(RetentionLeaseUtils.toMapExcludingPeerRecoveryRetentionLeases(replica.loadRetentionLeases()));
            }
        }
    }

    @Test
    public void testRetentionLeasesSyncOnExpiration() throws Exception {
        final int numberOfReplicas = 2 - scaledRandomIntBetween(0, 2);
        cluster().ensureAtLeastNumDataNodes(1 + numberOfReplicas);
        final long estimatedTimeIntervalMillis = ThreadPool.ESTIMATED_TIME_INTERVAL_SETTING.get(Settings.EMPTY).millis();
        final TimeValue retentionLeaseTimeToLive =
                TimeValue.timeValueMillis(randomLongBetween(estimatedTimeIntervalMillis, 2 * estimatedTimeIntervalMillis));
        execute(
            "create table doc.tbl (x int) clustered into 1 shards " +
            "with (" +
            "   number_of_replicas = ?, " +
            "   \"soft_deletes.enabled\" = true, " +
            "   \"soft_deletes.retention_lease.sync_interval\" = ?)",
            new Object[] {
                numberOfReplicas,
                retentionLeaseTimeToLive.getStringRep()
            }
        );
        ensureGreen("tbl");
        final String primaryShardNodeId = clusterService().state().routingTable().index("tbl").shard(0).primaryShard().currentNodeId();
        final String primaryShardNodeName = clusterService().state().nodes().get(primaryShardNodeId).getName();
        final IndexShard primary = cluster()
                .getInstance(IndicesService.class, primaryShardNodeName)
                .getShardOrNull(new ShardId(resolveIndex("tbl"), 0));
        // we will add multiple retention leases, wait for some to expire, and assert a consistent view between the primary and the replicas
        final int length = randomIntBetween(1, 8);
        for (int i = 0; i < length; i++) {
            // update the index for retention leases to live a long time
            execute("alter table doc.tbl reset (\"soft_deletes.retention_lease.period\")");

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
                final IndexShard replica = cluster()
                        .getInstance(IndicesService.class, replicaShardNodeName)
                        .getShardOrNull(new ShardId(resolveIndex("tbl"), 0));
                assertThat(RetentionLeaseUtils.toMapExcludingPeerRecoveryRetentionLeases(replica.getRetentionLeases()))
                    .satisfiesAnyOf(
                        l -> assertThat(l).containsValue(currentRetentionLease),
                        l -> assertThat(l).isEmpty());
            }

            // update the index for retention leases to short a long time, to force expiration
            execute("alter table doc.tbl set (\"soft_deletes.retention_lease.period\" = ?)", new Object[] { retentionLeaseTimeToLive.getStringRep() });

            // sleep long enough that the current retention lease has expired
            final long later = System.nanoTime();
            Thread.sleep(Math.max(0, retentionLeaseTimeToLive.millis() - TimeUnit.NANOSECONDS.toMillis(later - now)));
            assertBusy(() -> assertThat(
                RetentionLeaseUtils.toMapExcludingPeerRecoveryRetentionLeases(primary.getRetentionLeases()).entrySet())
                    .isEmpty());

            // now that all retention leases are expired should have been synced to all replicas
            assertBusy(() -> {
                for (final ShardRouting replicaShard : clusterService().state().routingTable().index("tbl").shard(0).replicaShards()) {
                    final String replicaShardNodeId = replicaShard.currentNodeId();
                    final String replicaShardNodeName = clusterService().state().nodes().get(replicaShardNodeId).getName();
                    final IndexShard replica = cluster()
                        .getInstance(IndicesService.class, replicaShardNodeName)
                        .getShardOrNull(new ShardId(resolveIndex("tbl"), 0));

                    assertThat(
                        RetentionLeaseUtils.toMapExcludingPeerRecoveryRetentionLeases(replica.getRetentionLeases()).entrySet())
                            .isEmpty();
                }
            });
        }
    }

    @Test
    public void testBackgroundRetentionLeaseSync() throws Exception {
        final int numberOfReplicas = 2 - scaledRandomIntBetween(0, 2);
        cluster().ensureAtLeastNumDataNodes(1 + numberOfReplicas);
        execute(
            "create table doc.tbl (x int) clustered into 1 shards " +
            "with (" +
            "   number_of_replicas = ?, " +
            "   \"soft_deletes.retention_lease.sync_interval\" = '1s')",
            new Object[] {
                numberOfReplicas,
            }
        );

        ensureGreen("tbl");
        final String primaryShardNodeId = clusterService().state().routingTable().index("tbl").shard(0).primaryShard().currentNodeId();
        final String primaryShardNodeName = clusterService().state().nodes().get(primaryShardNodeId).getName();
        final IndexShard primary = cluster()
                .getInstance(IndicesService.class, primaryShardNodeName)
                .getShardOrNull(new ShardId(resolveIndex("tbl"), 0));
        // we will add multiple retention leases and expect to see them synced to all replicas
        final int length = randomIntBetween(1, 8);
        final Map<String, RetentionLease> currentRetentionLeases = new LinkedHashMap<>(length);
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

    @Test
    public void testRetentionLeasesSyncOnRecovery() throws Exception {
        final int numberOfReplicas = 2 - scaledRandomIntBetween(0, 2);
        cluster().ensureAtLeastNumDataNodes(1 + numberOfReplicas);
        /*
         * We effectively disable the background sync to ensure that the retention leases are not synced in the background so that the only
         * source of retention leases on the replicas would be from recovery.
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
        allowNodes("tbl", 1);
        ensureYellow("tbl");
        execute("alter table doc.tbl set (number_of_replicas = ?)", new Object[] { numberOfReplicas });

        final String primaryShardNodeId = clusterService().state().routingTable().index("tbl").shard(0).primaryShard().currentNodeId();
        final String primaryShardNodeName = clusterService().state().nodes().get(primaryShardNodeId).getName();
        final IndexShard primary = cluster()
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
        }

        // Cause some recoveries to fail to ensure that retention leases are handled properly when retrying a recovery
        //
        execute("set global persistent \"indices.recovery.retry_delay_network\" = '100ms'");
        final Semaphore recoveriesToDisrupt = new Semaphore(scaledRandomIntBetween(0, 4));
        final MockTransportService primaryTransportService
            = (MockTransportService) cluster().getInstance(TransportService.class, primaryShardNodeName);
        primaryTransportService.addSendBehavior((connection, requestId, action, request, options) -> {
            if (action.equals(PeerRecoveryTargetService.Actions.FINALIZE) && recoveriesToDisrupt.tryAcquire()) {
                if (randomBoolean()) {
                    // return a ConnectTransportException to the START_RECOVERY action
                    final TransportService replicaTransportService
                        = cluster().getInstance(TransportService.class, connection.getNode().getName());
                    final DiscoveryNode primaryNode = primaryTransportService.getLocalNode();
                    replicaTransportService.disconnectFromNode(primaryNode);
                    AbstractSimpleTransportTestCase.connectToNode(replicaTransportService, primaryNode);
                } else {
                    // return an exception to the FINALIZE action
                    throw new ElasticsearchException("failing recovery for test purposes");
                }
            }
            connection.sendRequest(requestId, action, request, options);
        });

        // now allow the replicas to be allocated and wait for recovery to finalize
        allowNodes("tbl", 1 + numberOfReplicas);
        ensureGreen("tbl");

        // check current retention leases have been synced to all replicas
        for (final ShardRouting replicaShard : clusterService().state().routingTable().index("tbl").shard(0).replicaShards()) {
            final String replicaShardNodeId = replicaShard.currentNodeId();
            final String replicaShardNodeName = clusterService().state().nodes().get(replicaShardNodeId).getName();
            final IndexShard replica = cluster()
                .getInstance(IndicesService.class, replicaShardNodeName)
                .getShardOrNull(new ShardId(resolveIndex("tbl"), 0));
            final Map<String, RetentionLease> retentionLeasesOnReplica
                = RetentionLeaseUtils.toMapExcludingPeerRecoveryRetentionLeases(replica.getRetentionLeases());
            assertThat(retentionLeasesOnReplica).isEqualTo(currentRetentionLeases);

            // check retention leases have been written on the replica; see RecoveryTarget#finalizeRecovery
            assertThat(currentRetentionLeases).isEqualTo(RetentionLeaseUtils.toMapExcludingPeerRecoveryRetentionLeases(replica.loadRetentionLeases()));
        }
    }

    @Test
    public void testCanAddRetentionLeaseUnderBlock() throws InterruptedException {
        final String idForInitialRetentionLease = randomAlphaOfLength(8);
        runUnderBlockTest(
                idForInitialRetentionLease,
                randomLongBetween(0, Long.MAX_VALUE),
                (primary, listener) -> {
                    final String nextId = randomValueOtherThan(idForInitialRetentionLease, () -> randomAlphaOfLength(8));
                    final long nextRetainingSequenceNumber = randomLongBetween(0, Long.MAX_VALUE);
                    final String nextSource = randomAlphaOfLength(8);
                    primary.addRetentionLease(nextId, nextRetainingSequenceNumber, nextSource, listener);
                },
                primary -> {});
    }

    @Test
    public void testCanRenewRetentionLeaseUnderBlock() throws InterruptedException {
        final String idForInitialRetentionLease = randomAlphaOfLength(8);
        final long initialRetainingSequenceNumber = randomLongBetween(0, Long.MAX_VALUE);
        final AtomicReference<RetentionLease> retentionLease = new AtomicReference<>();
        runUnderBlockTest(
                idForInitialRetentionLease,
                initialRetainingSequenceNumber,
                (primary, listener) -> {
                    final long nextRetainingSequenceNumber = randomLongBetween(initialRetainingSequenceNumber, Long.MAX_VALUE);
                    final String nextSource = randomAlphaOfLength(8);
                    retentionLease.set(primary.renewRetentionLease(idForInitialRetentionLease, nextRetainingSequenceNumber, nextSource));
                    listener.onResponse(new ReplicationResponse());
                },
                primary -> {
                    try {
                        /*
                         * If the background renew was able to execute, then the retention leases were persisted to disk. There is no other
                         * way for the current retention leases to end up written to disk so we assume that if they are written to disk, it
                         * implies that the background sync was able to execute under a block.
                         */
                        assertBusy(() -> assertThat(
                            RetentionLeaseUtils.toMapExcludingPeerRecoveryRetentionLeases(primary.loadRetentionLeases()).values())
                                .containsExactly(retentionLease.get()));
                    } catch (final Exception e) {
                        fail(e.toString());
                    }
                });

    }

    public void testCanRemoveRetentionLeasesUnderBlock() throws InterruptedException {
        final String idForInitialRetentionLease = randomAlphaOfLength(8);
        runUnderBlockTest(
                idForInitialRetentionLease,
                randomLongBetween(0, Long.MAX_VALUE),
                (primary, listener) -> primary.removeRetentionLease(idForInitialRetentionLease, listener),
                indexShard -> {});
    }

    private void runUnderBlockTest(
            final String idForInitialRetentionLease,
            final long initialRetainingSequenceNumber,
            final BiConsumer<IndexShard, ActionListener<ReplicationResponse>> primaryConsumer,
            final Consumer<IndexShard> afterSync) throws InterruptedException {
        execute(
            "create table doc.tbl (x int) clustered into 1 shards " +
            "with (" +
            "   number_of_replicas = 0, " +
            "   \"soft_deletes.enabled\" = true, " +
            "   \"soft_deletes.retention_lease.sync_interval\" = '1s' " +
            ")"
        );
        ensureGreen("tbl");

        final String primaryShardNodeId = clusterService().state().routingTable().index("tbl").shard(0).primaryShard().currentNodeId();
        final String primaryShardNodeName = clusterService().state().nodes().get(primaryShardNodeId).getName();
        final IndexShard primary = cluster()
            .getInstance(IndicesService.class, primaryShardNodeName)
            .getShardOrNull(new ShardId(resolveIndex("tbl"), 0));

        final String source = randomAlphaOfLength(8);
        final CountDownLatch latch = new CountDownLatch(1);
        final ActionListener<ReplicationResponse> listener = ActionListener.wrap(r -> latch.countDown(), e -> fail(e.toString()));
        primary.addRetentionLease(idForInitialRetentionLease, initialRetainingSequenceNumber, source, listener);
        latch.await();

        final String block = randomFrom("read_only", "read_only_allow_delete", "read", "write", "metadata");

        execute("alter table doc.tbl set (\"blocks." + block + "\" = true)");
        try {
            final CountDownLatch actionLatch = new CountDownLatch(1);
            final AtomicBoolean success = new AtomicBoolean();

            primaryConsumer.accept(
                primary,
                new ActionListener<ReplicationResponse>() {

                    @Override
                    public void onResponse(final ReplicationResponse replicationResponse) {
                        success.set(true);
                        actionLatch.countDown();
                    }

                    @Override
                    public void onFailure(final Exception e) {
                        fail(e.toString());
                    }

                }
            );
            actionLatch.await();
            assertThat(success.get()).isTrue();
            afterSync.accept(primary);
        } finally {
            execute("alter table doc.tbl reset (\"blocks." + block + "\")");
        }
    }

    @Test
    public void testCanAddRetentionLeaseWithoutWaitingForShards() throws InterruptedException {
        final String idForInitialRetentionLease = randomAlphaOfLength(8);
        runWaitForShardsTest(
                idForInitialRetentionLease,
                randomLongBetween(0, Long.MAX_VALUE),
                (primary, listener) -> {
                    final String nextId = randomValueOtherThan(idForInitialRetentionLease, () -> randomAlphaOfLength(8));
                    final long nextRetainingSequenceNumber = randomLongBetween(0, Long.MAX_VALUE);
                    final String nextSource = randomAlphaOfLength(8);
                    primary.addRetentionLease(nextId, nextRetainingSequenceNumber, nextSource, listener);
                },
                primary -> {});
    }

    @Test
    public void testCanRenewRetentionLeaseWithoutWaitingForShards() throws InterruptedException {
        final String idForInitialRetentionLease = randomAlphaOfLength(8);
        final long initialRetainingSequenceNumber = randomLongBetween(0, Long.MAX_VALUE);
        final AtomicReference<RetentionLease> retentionLease = new AtomicReference<>();
        runWaitForShardsTest(
                idForInitialRetentionLease,
                initialRetainingSequenceNumber,
                (primary, listener) -> {
                    final long nextRetainingSequenceNumber = randomLongBetween(initialRetainingSequenceNumber, Long.MAX_VALUE);
                    final String nextSource = randomAlphaOfLength(8);
                    retentionLease.set(primary.renewRetentionLease(idForInitialRetentionLease, nextRetainingSequenceNumber, nextSource));
                    listener.onResponse(new ReplicationResponse());
                },
                primary -> {
                    try {
                        /*
                         * If the background renew was able to execute, then the retention leases were persisted to disk. There is no other
                         * way for the current retention leases to end up written to disk so we assume that if they are written to disk, it
                         * implies that the background sync was able to execute despite wait for shards being set on the index.
                         */
                        assertBusy(() -> assertThat(
                            RetentionLeaseUtils.toMapExcludingPeerRecoveryRetentionLeases(primary.loadRetentionLeases()).values())
                                .contains(retentionLease.get()));
                    } catch (final Exception e) {
                        fail(e.toString());
                    }
                });

    }

    @Test
    public void testCanRemoveRetentionLeasesWithoutWaitingForShards() throws InterruptedException {
        final String idForInitialRetentionLease = randomAlphaOfLength(8);
        runWaitForShardsTest(
                idForInitialRetentionLease,
                randomLongBetween(0, Long.MAX_VALUE),
                (primary, listener) -> primary.removeRetentionLease(idForInitialRetentionLease, listener),
                primary -> {});
    }

    private void runWaitForShardsTest(
            final String idForInitialRetentionLease,
            final long initialRetainingSequenceNumber,
            final BiConsumer<IndexShard, ActionListener<ReplicationResponse>> primaryConsumer,
            final Consumer<IndexShard> afterSync) throws InterruptedException {
        final int numDataNodes = cluster().numDataNodes();
        execute(
            "create table doc.tbl (x int) clustered into 1 shards " +
            "with (" +
            "   number_of_replicas = ?, " +
            "   \"soft_deletes.enabled\" = true," +
            "   \"soft_deletes.retention_lease.sync_interval\" = ?)",
            new Object[] {
                numDataNodes == 1 ? 0 : numDataNodes - 1,
                TimeValue.timeValueSeconds(1).getStringRep()
            }
        );
        ensureYellowAndNoInitializingShards("tbl");
        var clusterHealthResponse = FutureUtils.get(
            client().admin().cluster().health(new ClusterHealthRequest("tbl").waitForActiveShards(numDataNodes)));
        assertThat(clusterHealthResponse.isTimedOut()).isFalse();

        final String primaryShardNodeId = clusterService().state().routingTable().index("tbl").shard(0).primaryShard().currentNodeId();
        final String primaryShardNodeName = clusterService().state().nodes().get(primaryShardNodeId).getName();
        final IndexShard primary = cluster()
                .getInstance(IndicesService.class, primaryShardNodeName)
                .getShardOrNull(new ShardId(resolveIndex("tbl"), 0));

        final String source = randomAlphaOfLength(8);
        final CountDownLatch latch = new CountDownLatch(1);
        final ActionListener<ReplicationResponse> listener = ActionListener.wrap(r -> latch.countDown(), e -> fail(e.toString()));
        primary.addRetentionLease(idForInitialRetentionLease, initialRetainingSequenceNumber, source, listener);
        latch.await();

        final String waitForActiveValue = randomBoolean() ? "all" : Integer.toString(numDataNodes);

        execute("alter table doc.tbl set (\"write.wait_for_active_shards\" = ?)", new Object[] { waitForActiveValue });
        final CountDownLatch actionLatch = new CountDownLatch(1);
        final AtomicBoolean success = new AtomicBoolean();

        primaryConsumer.accept(
                primary,
                new ActionListener<ReplicationResponse>() {

                    @Override
                    public void onResponse(final ReplicationResponse replicationResponse) {
                        success.set(true);
                        actionLatch.countDown();
                    }

                    @Override
                    public void onFailure(final Exception e) {
                        fail(e.toString());
                    }

                });
        actionLatch.await();
        assertThat(success.get()).isTrue();
        afterSync.accept(primary);
    }

    private static void failWithException(Exception e) {
        throw new AssertionError("unexpected", e);
    }

    private static ActionListener<ReplicationResponse> countDownLatchListener(CountDownLatch latch) {
        return ActionListener.wrap(r -> latch.countDown(), RetentionLeaseIT::failWithException);
    }

}
