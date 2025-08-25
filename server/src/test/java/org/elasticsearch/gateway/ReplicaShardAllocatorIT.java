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

package org.elasticsearch.gateway;

import static io.crate.testing.Asserts.assertThat;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.seqno.ReplicationTracker;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.indices.recovery.PeerRecoveryTargetService;
import org.elasticsearch.indices.recovery.RecoveryCleanFilesRequest;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.IntegTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.test.TestCluster;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TransportService;
import org.junit.Test;

import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.types.DataTypes;

@IntegTestCase.ClusterScope(scope = IntegTestCase.Scope.TEST, numDataNodes = 0)
public class ReplicaShardAllocatorIT extends IntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var nodePlugins = new ArrayList<>(super.nodePlugins());
        nodePlugins.add(MockTransportService.TestPlugin.class);
        nodePlugins.add(InternalSettingsPlugin.class);
        return nodePlugins;
    }

    /**
     * Verify that if we found a new copy where it can perform a no-op recovery,
     * then we will cancel the current recovery and allocate replica to the new copy.
     */
    @Test
    public void testPreferCopyCanPerformNoopRecovery() throws Exception {
        String nodeWithPrimary = cluster().startNode();
        execute("""
            create table doc.test (x int)
            clustered into 1 shards with (
                number_of_replicas = 1,
                "recovery.file_based_threshold" = 1.0,
                "global_checkpoint_sync.interval" = '100ms',
                "soft_deletes.retention_lease.sync_interval" = '100ms',
                "unassigned.node_left.delayed_timeout" = '1ms'
            )
        """);

        String indexUUID = resolveIndex("doc.test").getUUID();

        String nodeWithReplica = cluster().startDataOnlyNode();
        Settings nodeWithReplicaSettings = cluster().dataPathSettings(nodeWithReplica);

        execute("insert into doc.test (x) values (?)", new Object[]{randomIntBetween(100, 500)});

        if (randomBoolean()) {
            execute("insert into doc.test (x) values (?)", new Object[]{randomIntBetween(0, 80)});
        }

        ensureActivePeerRecoveryRetentionLeasesAdvanced("test", indexUUID);
        cluster().stopRandomNode(TestCluster.nameFilter(nodeWithReplica));
        if (randomBoolean()) {
            execute("optimize table doc.test with(flush = true)");
        }
        CountDownLatch blockRecovery = new CountDownLatch(1);
        CountDownLatch recoveryStarted = new CountDownLatch(1);
        MockTransportService transportServiceOnPrimary
            = (MockTransportService) cluster().getInstance(TransportService.class, nodeWithPrimary);
        transportServiceOnPrimary.addSendBehavior((connection, requestId, action, request, options) -> {
            if (PeerRecoveryTargetService.Actions.FILES_INFO.equals(action)) {
                recoveryStarted.countDown();
                try {
                    blockRecovery.await(10, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    fail(e);
                }
            }
            connection.sendRequest(requestId, action, request, options);
        });
        try {
            cluster().startDataOnlyNode();
            recoveryStarted.await();
            String newNodeWithReplica = cluster().startDataOnlyNode(nodeWithReplicaSettings);
            // AllocationService only calls GatewayAllocator if there're unassigned shards
            execute("""
                create table doc.dummy (x int)
                with (
                    "number_of_replicas" = 1,
                    "write.wait_for_active_shards" = 0
                )
            """);
            assertBusy(() -> {
                execute("select health from sys.health where table_name = 'test'");
                assertThat(response).hasRows("GREEN");
                assertThat(cluster().nodesInclude(indexUUID)).contains(newNodeWithReplica);
            });
            assertNoOpRecoveries("test");
            blockRecovery.countDown();
        } finally {
            transportServiceOnPrimary.clearAllRules();
        }
    }

    /**
     * Ensure that we fetch the latest shard store from the primary when a new node joins so we won't cancel the current recovery
     * for the copy on the newly joined node unless we can perform a noop recovery with that node.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testRecentPrimaryInformation() throws Exception {
        String nodeWithPrimary = cluster().startNode();

        execute("""
            create table doc.test (x int)
            clustered into 1 shards with (
                number_of_replicas = 1,
                "recovery.file_based_threshold" = 1.0,
                "global_checkpoint_sync.interval" = '100ms',
                "soft_deletes.retention_lease.sync_interval" = '100ms',
                "unassigned.node_left.delayed_timeout" = '1ms'
            )
        """);
        String nodeWithReplica = cluster().startDataOnlyNode();
        DiscoveryNode discoNodeWithReplica = cluster().getInstance(ClusterService.class, nodeWithReplica).localNode();
        Settings nodeWithReplicaSettings = cluster().dataPathSettings(nodeWithReplica);
        ensureGreen();

        String indexUUID = resolveIndex("doc.test").getUUID();

        execute("insert into doc.test (x) values (?)", new Object[][] {
            new Object[] { randomIntBetween(10, 100) },
            new Object[] { randomIntBetween(10, 100) },
        });
        cluster().stopRandomNode(TestCluster.nameFilter(nodeWithReplica));
        if (randomBoolean()) {
            execute("insert into doc.test (x) values (?)", new Object[][] {
                new Object[] { randomIntBetween(10, 100) },
                new Object[] { randomIntBetween(10, 100) },
            });
        }
        CountDownLatch blockRecovery = new CountDownLatch(1);
        CountDownLatch recoveryStarted = new CountDownLatch(1);
        MockTransportService transportServiceOnPrimary
            = (MockTransportService) cluster().getInstance(TransportService.class, nodeWithPrimary);
        transportServiceOnPrimary.addSendBehavior((connection, requestId, action, request, options) -> {
            if (PeerRecoveryTargetService.Actions.FILES_INFO.equals(action)) {
                recoveryStarted.countDown();
                try {
                    blockRecovery.await(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            connection.sendRequest(requestId, action, request, options);
        });
        try {
            String newNode = cluster().startDataOnlyNode();
            recoveryStarted.await(5, TimeUnit.SECONDS);
            // Index more documents and flush to destroy sync_id and remove the retention lease (as file_based_recovery_threshold reached).
            execute("insert into doc.test (x) values (?)", new Object[][] {
                new Object[] { randomIntBetween(10, 100) },
                new Object[] { randomIntBetween(10, 100) },
            });
            execute("optimize table doc.test with (flush = true, max_num_segments = 1)");
            assertBusy(() -> {
                execute("select unnest(retention_leases['leases']['id']) from sys.shards where table_name = 'test'");
                for (var row : response.rows()) {
                    assertThat(row[0]).isNotEqualTo((ReplicationTracker.getPeerRecoveryRetentionLeaseId(discoNodeWithReplica.getId())));
                }
            });
            // AllocationService only calls GatewayAllocator if there are unassigned shards
            // Excluding the new node for allocation to avoid rebalance of the `doc.test` table.
            execute("""
                create table doc.dummy (x int)
                with (
                    "number_of_replicas" = 1,
                    "write.wait_for_active_shards" = 0,
                    "routing.allocation.exclude._name" = ?
                )
            """, new Object[] { newNode });
            cluster().startDataOnlyNode(nodeWithReplicaSettings);

            // need to wait for events to ensure the reroute has happened since we perform it async when a new node joins.
            ensureYellow();
            assertBusy(() -> {
                execute("""
                    select
                        routing_state,
                        count(*)
                    from
                        sys.shards
                    where
                        table_name = 'test'
                    group by 1
                    """
                );
                assertThat(response).hasRows(
                    "STARTED| 2"
                );
            });
            blockRecovery.countDown();
            assertBusy(() -> {
                execute("select health from sys.health where table_name = 'test'");
                assertThat(response).hasRows("GREEN");
                assertThat(cluster().nodesInclude(indexUUID)).contains(newNode);
            });

            execute("select recovery['files'] from sys.shards where table_name = 'test'");
            for (var row : response.rows()) {
                assertThat((Map<String, Object>) row[0]).isNotEmpty();
            }
        } finally {
            transportServiceOnPrimary.clearAllRules();
        }
    }

    @Test
    public void testFullClusterRestartPerformNoopRecovery() throws Exception {
        int numOfReplicas = randomIntBetween(1, 2);
        cluster().ensureAtLeastNumDataNodes(numOfReplicas + 2);

        execute("""
            create table doc.test (x int)
            clustered into 1 shards with (
                number_of_replicas = ?,
                "translog.flush_threshold_size" = ?,
                "recovery.file_based_threshold" = '0.5',
                "global_checkpoint_sync.interval" = '100ms',
                "soft_deletes.retention_lease.sync_interval" = '100ms'
            )
            """, new Object[] {numOfReplicas, randomIntBetween(10, 100) + "kb",  });

        ensureGreen();

        String indexUUID = resolveIndex("doc.test").getUUID();

        execute("insert into doc.test (x) values (?)", new Object[]{randomIntBetween(200, 500)});

        execute("refresh table doc.test");

        execute("insert into doc.test (x) values (?)", new Object[]{randomIntBetween(0, 80)});

        if (randomBoolean()) {
            execute("optimize table doc.test with (max_num_segments = 1)");
        }
        ensureActivePeerRecoveryRetentionLeasesAdvanced("test", indexUUID);
        if (randomBoolean()) {
            execute("alter table doc.test close");
        }
        execute("set global \"cluster.routing.allocation.enable\" = 'primaries'");
        cluster().fullRestart();
        ensureYellow();
        execute("reset global \"cluster.routing.allocation.enable\"");
        ensureGreen();
        assertNoOpRecoveries("test");
    }

    /**
     * If the recovery source is on an old node (before <pre>{@link org.elasticsearch.Version#V_4_2_0}</pre>) then the recovery target
     * won't have the safe commit after phase1 because the recovery source does not send the global checkpoint in the clean_files
     * step. And if the recovery fails and retries, then the recovery stage might not transition properly. This test simulates
     * this behavior by changing the global checkpoint in phase1 to unassigned.
     */
    @Test
    public void testSimulateRecoverySourceOnOldNode() throws Exception {
        cluster().startMasterOnlyNode();
        String source = cluster().startDataOnlyNode();

        execute("""
            create table doc.test (x int)
            clustered into 1 shards with (
                number_of_replicas = 0
            )
            """, new Object[] {});

        ensureGreen();

        if (randomBoolean()) {
            execute("insert into doc.test (x) values (?)", new Object[]{randomIntBetween(200, 500)});
        }
        if (randomBoolean()) {
            execute("optimize table doc.test");
        }

        cluster().startDataOnlyNode();
        MockTransportService transportService = (MockTransportService) cluster().getInstance(TransportService.class, source);
        Semaphore failRecovery = new Semaphore(1);
        transportService.addSendBehavior((connection, requestId, action, request, options) -> {
            if (action.equals(PeerRecoveryTargetService.Actions.CLEAN_FILES)) {
                RecoveryCleanFilesRequest cleanFilesRequest = (RecoveryCleanFilesRequest) request;
                request = new RecoveryCleanFilesRequest(cleanFilesRequest.recoveryId(),
                                                        cleanFilesRequest.requestSeqNo(), cleanFilesRequest.shardId(), cleanFilesRequest.sourceMetaSnapshot(),
                                                        cleanFilesRequest.totalTranslogOps(), SequenceNumbers.UNASSIGNED_SEQ_NO);
            }
            if (action.equals(PeerRecoveryTargetService.Actions.FINALIZE)) {
                if (failRecovery.tryAcquire()) {
                    throw new IllegalStateException("simulated");
                }
            }
            connection.sendRequest(requestId, action, request, options);
        });

        execute("alter table doc.test set (number_of_replicas=1)");
        ensureGreen();
        transportService.clearAllRules();
    }

    /**
     * Make sure that we do not repeatedly cancel an ongoing recovery for a noop copy on a broken node.
     */
    @Test
    public void testDoNotCancelRecoveryForBrokenNode() throws Exception {
        cluster().startMasterOnlyNode();
        String nodeWithPrimary = cluster().startDataOnlyNode();

        execute("""
            create table doc.test (x int)
            clustered into 1 shards with (
                number_of_replicas = 0,
                "global_checkpoint_sync.interval" = '100ms',
                "soft_deletes.retention_lease.sync_interval" = '100ms'
            )
            """);

        ensureGreen();
        execute("insert into doc.test (x) values (?)", new Object[]{randomIntBetween(200, 500)});

        String brokenNode = cluster().startDataOnlyNode();
        MockTransportService transportService =
            (MockTransportService) cluster().getInstance(TransportService.class, nodeWithPrimary);
        CountDownLatch newNodeStarted = new CountDownLatch(1);
        transportService.addSendBehavior((connection, requestId, action, request, options) -> {
            if (action.equals(PeerRecoveryTargetService.Actions.TRANSLOG_OPS)) {
                if (brokenNode.equals(connection.getNode().getName())) {
                    try {
                        newNodeStarted.await();
                    } catch (InterruptedException e) {
                        throw new AssertionError(e);
                    }
                    throw new CircuitBreakingException(100, 150, 120, "dummy");
                }
            }
            connection.sendRequest(requestId, action, request, options);
        });
        execute("alter table doc.test set (number_of_replicas=1)");
        cluster().startDataOnlyNode();
        newNodeStarted.countDown();
        ensureGreen();
        transportService.clearAllRules();
    }

    @SuppressWarnings("unchecked")
    private void ensureActivePeerRecoveryRetentionLeasesAdvanced(String tableName, String indexUUID) throws Exception {
        assertBusy(() -> {
            Set<String> activeRetentionLeaseIds = clusterService().state().routingTable().index(indexUUID).shard(0).shards().stream()
                .map(shardRouting -> ReplicationTracker.getPeerRecoveryRetentionLeaseId(shardRouting.currentNodeId()))
                .collect(Collectors.toSet());
            execute(
                "select seq_no_stats['global_checkpoint'], seq_no_stats['max_seq_no'], retention_leases['leases'] from sys.shards where table_name = ?",
                new Object[]{tableName});
            assertThat(response.rowCount()).isGreaterThan(0);
            long globalCheckPoint = (long) response.rows()[0][0];
            long maxSeqNo = (long) response.rows()[0][1];
            assertThat(globalCheckPoint).isEqualTo(maxSeqNo);
            List<Map<String, Object>> rentetionLease = (List<Map<String, Object>>) response.rows()[0][2];
            assertThat(rentetionLease).hasSize(activeRetentionLeaseIds.size());
            for (var activeRetentionLease : rentetionLease) {
                assertThat(
                    DataTypes.LONG.explicitCast(activeRetentionLease.get("retaining_seq_no"), CoordinatorTxnCtx.systemTransactionContext().sessionSettings()))
                    .isEqualTo(globalCheckPoint + 1L);
            }
        });
    }

    private void assertNoOpRecoveries(String tableName) {
        execute("select recovery['files'] from sys.shards where table_name = ?", new Object[]{tableName});
        for (var row : response.rows()) {
            assertThat((Map<String, Object>) row[0]).isNotEmpty();
        }
    }
}
