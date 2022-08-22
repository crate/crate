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

import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.indices.flush.SyncedFlushAction;
import org.elasticsearch.action.admin.indices.flush.SyncedFlushRequest;
import org.elasticsearch.action.admin.indices.flush.SyncedFlushResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.index.seqno.ReplicationTracker;
import org.elasticsearch.indices.recovery.PeerRecoveryTargetService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.IntegTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.test.TestCluster;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TransportService;
import org.hamcrest.Matchers;
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
        String indexName = "test";
        String nodeWithPrimary = internalCluster().startNode();
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

        String nodeWithReplica = internalCluster().startDataOnlyNode();
        Settings nodeWithReplicaSettings = internalCluster().dataPathSettings(nodeWithReplica);
        ensureGreen(indexName);

        execute("insert into doc.test (x) values (?)", new Object[]{randomIntBetween(100, 500)});

        if (randomBoolean()) {
            execute("insert into doc.test (x) values (?)", new Object[]{randomIntBetween(0, 80)});
        }

        ensureActivePeerRecoveryRetentionLeasesAdvanced(indexName);
        internalCluster().stopRandomNode(TestCluster.nameFilter(nodeWithReplica));
        if (randomBoolean()) {
            execute("optimize table doc.test with(flush = true)");
        }
        CountDownLatch blockRecovery = new CountDownLatch(1);
        CountDownLatch recoveryStarted = new CountDownLatch(1);
        MockTransportService transportServiceOnPrimary
            = (MockTransportService) internalCluster().getInstance(TransportService.class, nodeWithPrimary);
        transportServiceOnPrimary.addSendBehavior((connection, requestId, action, request, options) -> {
            if (PeerRecoveryTargetService.Actions.FILES_INFO.equals(action)) {
                recoveryStarted.countDown();
                try {
                    blockRecovery.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            connection.sendRequest(requestId, action, request, options);
        });
        try {
            internalCluster().startDataOnlyNode();
            recoveryStarted.await();
            nodeWithReplica = internalCluster().startDataOnlyNode(nodeWithReplicaSettings);
            // AllocationService only calls GatewayAllocator if there're unassigned shards
            execute("""
                create table doc.dummy (x int)
                with ("number_of_replicas" = 1, "write.wait_for_active_shards" = 0)
            """);
            ensureGreen(indexName);
            assertThat(internalCluster().nodesInclude(indexName), hasItem(nodeWithReplica));
            assertNoOpRecoveries(indexName);
            blockRecovery.countDown();
        } finally {
            transportServiceOnPrimary.clearAllRules();
        }
    }

    /**
     * Ensure that we fetch the latest shard store from the primary when a new node joins so we won't cancel the current recovery
     * for the copy on the newly joined node unless we can perform a noop recovery with that node.
     */
    @Test
    public void testRecentPrimaryInformation() throws Exception {
        String indexName = "test";
        String nodeWithPrimary = internalCluster().startNode();

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
        String nodeWithReplica = internalCluster().startDataOnlyNode();
        DiscoveryNode discoNodeWithReplica = internalCluster().getInstance(ClusterService.class, nodeWithReplica).localNode();
        Settings nodeWithReplicaSettings = internalCluster().dataPathSettings(nodeWithReplica);
        ensureGreen(indexName);
        execute("insert into doc.test (x) values (?)", new Object[][] {
            new Object[] { randomIntBetween(10, 100) },
            new Object[] { randomIntBetween(10, 100) },
        });
        assertBusy(() -> {
            SyncedFlushResponse syncedFlushResponse = client()
                .execute(SyncedFlushAction.INSTANCE, new SyncedFlushRequest(indexName))
                .get(5, TimeUnit.SECONDS);
            assertThat(syncedFlushResponse.successfulShards(), equalTo(2));
        });
        internalCluster().stopRandomNode(TestCluster.nameFilter(nodeWithReplica));
        if (randomBoolean()) {
            execute("insert into doc.test (x) values (?)", new Object[][] {
                new Object[] { randomIntBetween(10, 100) },
                new Object[] { randomIntBetween(10, 100) },
            });
        }
        CountDownLatch blockRecovery = new CountDownLatch(1);
        CountDownLatch recoveryStarted = new CountDownLatch(1);
        MockTransportService transportServiceOnPrimary
            = (MockTransportService) internalCluster().getInstance(TransportService.class, nodeWithPrimary);
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
            String newNode = internalCluster().startDataOnlyNode();
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
                    assertThat(row[0], not(equalTo((ReplicationTracker.getPeerRecoveryRetentionLeaseId(discoNodeWithReplica.getId())))));
                }
            });
            // AllocationService only calls GatewayAllocator if there are unassigned shards
            execute("""
                create table doc.dummy (x int)
                with ("number_of_replicas" = 1, "write.wait_for_active_shards" = 0)
            """);
            internalCluster().startDataOnlyNode(nodeWithReplicaSettings);
            // need to wait for events to ensure the reroute has happened since we perform it async when a new node joins.
            FutureUtils.get(
                client().admin().cluster().health(
                    new ClusterHealthRequest(indexName)
                        .waitForYellowStatus()
                        .waitForEvents(Priority.LANGUID)
                ),
                5,
                TimeUnit.SECONDS
            );
            blockRecovery.countDown();
            ensureGreen(indexName);
            assertThat(internalCluster().nodesInclude(indexName), hasItem(newNode));

            execute("select recovery['files'] from sys.shards where table_name = 'test'");
            for (var row : response.rows()) {
                assertThat((Map<String, Object>) row[0], not(Matchers.anEmptyMap()));
            }
        } finally {
            transportServiceOnPrimary.clearAllRules();
        }
    }

    @Test
    public void testFullClusterRestartPerformNoopRecovery() throws Exception {
        int numOfReplicas = randomIntBetween(1, 2);
        internalCluster().ensureAtLeastNumDataNodes(numOfReplicas + 2);
        String indexName = "test";

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

        ensureGreen(indexName);

        execute("insert into doc.test (x) values (?)", new Object[]{randomIntBetween(200, 500)});

        execute("refresh table doc.test");

        execute("insert into doc.test (x) values (?)", new Object[]{randomIntBetween(0, 80)});

        if (randomBoolean()) {
            execute("optimize table doc.test with (max_num_segments = 1)");
        }
        ensureActivePeerRecoveryRetentionLeasesAdvanced(indexName);
        if (randomBoolean()) {
            execute("alter table doc.test close");
        }
        execute("set global \"cluster.routing.allocation.enable\" = 'primaries'");
        internalCluster().fullRestart();
        ensureYellow(indexName);
        execute("reset global \"cluster.routing.allocation.enable\"");
        ensureGreen(indexName);
        assertNoOpRecoveries(indexName);
    }

    /**
     * Make sure that we do not repeatedly cancel an ongoing recovery for a noop copy on a broken node.
     */
    @Test
    public void testDoNotCancelRecoveryForBrokenNode() throws Exception {
        internalCluster().startMasterOnlyNode();
        String nodeWithPrimary = internalCluster().startDataOnlyNode();
        String indexName = "test";

        execute("""
            create table doc.test (x int)
            clustered into 1 shards with (
                number_of_replicas = 0,
                "global_checkpoint_sync.interval" = '100ms',
                "soft_deletes.retention_lease.sync_interval" = '100ms'
            )
                """);

        ensureGreen(indexName);
        execute("insert into doc.test (x) values (?)", new Object[]{randomIntBetween(200, 500)});

        String brokenNode = internalCluster().startDataOnlyNode();
        MockTransportService transportService =
            (MockTransportService) internalCluster().getInstance(TransportService.class, nodeWithPrimary);
        CountDownLatch newNodeStarted = new CountDownLatch(1);
        transportService.addSendBehavior((connection, requestId, action, request, options) -> {
            if (action.equals(PeerRecoveryTargetService.Actions.TRANSLOG_OPS)) {
                if (brokenNode.equals(connection.getNode().getName())) {
                    try {
                        newNodeStarted.await();
                    } catch (InterruptedException e) {
                        throw new AssertionError(e);
                    }
                    throw new CircuitBreakingException("not enough memory for indexing", 100, 50);
                }
            }
            connection.sendRequest(requestId, action, request, options);
        });
        execute("alter table doc.test set (number_of_replicas=1)");
        internalCluster().startDataOnlyNode();
        newNodeStarted.countDown();
        ensureGreen(indexName);
        transportService.clearAllRules();
    }

    private void ensureActivePeerRecoveryRetentionLeasesAdvanced(String indexName) throws Exception {
        assertBusy(() -> {
            Set<String> activeRetentionLeaseIds = clusterService().state().routingTable().index(indexName).shard(0).shards().stream()
                .map(shardRouting -> ReplicationTracker.getPeerRecoveryRetentionLeaseId(shardRouting.currentNodeId()))
                .collect(Collectors.toSet());
            execute(
                "select seq_no_stats['global_checkpoint'], seq_no_stats['max_seq_no'], retention_leases['leases'] from sys.shards where table_name = ?",
                new Object[]{indexName});
            long globalCheckPoint = (long) response.rows()[0][0];
            long maxSeqNo = (long) response.rows()[0][1];
            assertThat(globalCheckPoint, equalTo(maxSeqNo));
            List<Map<String, Object>> rentetionLease = (List<Map<String, Object>>) response.rows()[0][2];
            assertThat(rentetionLease.size(), is(activeRetentionLeaseIds.size()));
            for (var activeRetentionLease : rentetionLease) {
                assertThat(
                    DataTypes.LONG.explicitCast(activeRetentionLease.get("retaining_seq_no"), CoordinatorTxnCtx.systemTransactionContext().sessionSettings()),
                    is(globalCheckPoint + 1L)
                );
            }
        });
    }

    private void assertNoOpRecoveries(String indexName) {
        execute("select recovery['files'] from sys.shards where table_name = ?", new Object[]{indexName});
        for (var row : response.rows()) {
            assertThat((Map<String, Object>) row[0], not(Matchers.anEmptyMap()));
        }
    }
}
