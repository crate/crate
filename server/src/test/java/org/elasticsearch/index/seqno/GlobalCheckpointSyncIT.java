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
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.IntegTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TransportService;
import org.junit.Test;

import io.crate.common.unit.TimeValue;
import io.crate.metadata.RelationName;

public class GlobalCheckpointSyncIT extends IntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Stream.concat(
                super.nodePlugins().stream(),
                Stream.of(InternalSettingsPlugin.class, MockTransportService.TestPlugin.class))
                .collect(Collectors.toList());
    }

    @Test
    public void testGlobalCheckpointSyncWithAsyncDurability() throws Exception {
        cluster().ensureAtLeastNumDataNodes(2);
        execute(
            "create table test(id integer) clustered into 1 shards with" +
            "(\"global_checkpoint_sync.interval\" = '1s', \"translog.durability\" = 'ASYNC', \"translog.sync_interval\" = '1s', number_of_replicas = 1)");

        for (int j = 0; j < 10; j++) {
            execute("insert into test(id) values(?)", new Object[]{j});
        }

        assertBusy(() -> {
            execute("select seq_no_stats['global_checkpoint'], seq_no_stats['max_seq_no'] from sys.shards where table_name = 'test'");
            assertThat(response.rowCount()).isGreaterThan(0L);
            for (var row : response.rows()) {
                assertThat(row[0])
                    .as("global checkpoint and max seq# must be in sync")
                    .isEqualTo(row[1]);
            }
        });
    }

    @Test
    public void testPostOperationGlobalCheckpointSync() throws Exception {
        // set the sync interval high so it does not execute during this test. This only allows the global checkpoint to catch up
        // on a post-operation background sync if translog durability is set to sync. Async durability relies on a scheduled global
        // checkpoint sync to allow the information about persisted local checkpoints to be transferred to the primary.
        //
        runGlobalCheckpointSyncTest(
            TimeValue.timeValueHours(24),
            (indexName, client) -> {
                RelationName relationName = RelationName.fromIndexName(indexName);
                String stmt = String.format(
                    Locale.ENGLISH,
                    "alter table \"%s\".\"%s\" set (\"translog.durability\" = ?)",
                    relationName.schema(),
                    relationName.name()
                );
                execute(stmt, new Object[] { Translog.Durability.REQUEST.name() });
            },
            (indexName, client) -> {}
        );
    }

    /*
     * This test swallows the post-operation global checkpoint syncs, and then restores the ability to send these requests at the end of the
     * test so that a background sync can fire and sync the global checkpoint.
     */
    public void testBackgroundGlobalCheckpointSync() throws Exception {
        runGlobalCheckpointSyncTest(
                TimeValue.timeValueSeconds(randomIntBetween(1, 3)),
                (indexName, client) -> {
                    // prevent global checkpoint syncs between all nodes
                    final DiscoveryNodes nodes = FutureUtils.get(client.admin().cluster()
                        .state(new ClusterStateRequest())).getState().nodes();
                    for (final DiscoveryNode node : nodes) {
                        for (final DiscoveryNode other : nodes) {
                            if (node == other) {
                                continue;
                            }
                            final MockTransportService senderTransportService =
                                    (MockTransportService) cluster().getInstance(TransportService.class, node.getName());
                            final MockTransportService receiverTransportService =
                                    (MockTransportService) cluster().getInstance(TransportService.class, other.getName());
                            senderTransportService.addSendBehavior(receiverTransportService,
                                (connection, requestId, action, request, options) -> {
                                    if ("indices:admin/seq_no/global_checkpoint_sync[r]".equals(action)) {
                                        throw new IllegalStateException("blocking indices:admin/seq_no/global_checkpoint_sync[r]");
                                    } else {
                                        connection.sendRequest(requestId, action, request, options);
                                    }
                                });
                        }
                    }
                },
                (indexName, client) -> {
                    // restore global checkpoint syncs between all nodes
                    final DiscoveryNodes nodes = FutureUtils
                        .get(client.admin().cluster().state(new ClusterStateRequest()))
                        .getState()
                        .nodes();
                    for (final DiscoveryNode node : nodes) {
                        for (final DiscoveryNode other : nodes) {
                            if (node == other) {
                                continue;
                            }
                            final MockTransportService senderTransportService =
                                    (MockTransportService) cluster().getInstance(TransportService.class, node.getName());
                            final MockTransportService receiverTransportService =
                                    (MockTransportService) cluster().getInstance(TransportService.class, other.getName());
                            senderTransportService.clearOutboundRules(receiverTransportService);
                        }
                    }
                });
    }

    private void runGlobalCheckpointSyncTest(
        final TimeValue globalCheckpointSyncInterval,
        final BiConsumer<String, Client> beforeIndexing,
        final BiConsumer<String, Client> afterIndexing) throws Exception {
        final int numberOfReplicas = randomIntBetween(1, 4);
        cluster().ensureAtLeastNumDataNodes(1 + numberOfReplicas);
        execute(
            "create table test(id integer) clustered into 1 shards with" +
            "(\"global_checkpoint_sync.interval\" = ?, number_of_replicas = ?)",
            new Object[]{globalCheckpointSyncInterval.toString(), numberOfReplicas}
            );

        var indexName = getFqn("test");

        if (randomBoolean()) {
            ensureGreen(indexName);
        }

        beforeIndexing.accept(indexName, client());

        final int numberOfDocuments = randomIntBetween(0, 256);

        final int numberOfThreads = randomIntBetween(1, 4);
        final CyclicBarrier barrier = new CyclicBarrier(1 + numberOfThreads);

        // start concurrent indexing threads
        final List<Thread> threads = new ArrayList<>(numberOfThreads);
        for (int i = 0; i < numberOfThreads; i++) {
            final int index = i;
            final Thread thread = new Thread(() -> {
                try {
                    barrier.await();
                } catch (BrokenBarrierException | InterruptedException e) {
                    throw new RuntimeException(e);
                }
                for (int j = 0; j < numberOfDocuments; j++) {
                    final String id = Integer.toString(index * numberOfDocuments + j);
                    sqlExecutor.execute("insert into test(id) values(?)", new Object[]{id});
                }
                try {
                    barrier.await();
                } catch (BrokenBarrierException | InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });
            threads.add(thread);
            thread.start();
        }

        // synchronize the start of the threads
        barrier.await();

        // wait for the threads to finish
        barrier.await();

        afterIndexing.accept(indexName, client());

        assertBusy(() -> {
            execute("select seq_no_stats['global_checkpoint'], seq_no_stats['max_seq_no'] from " + "sys.shards where table_name='test' and primary=true");
            assertThat(response.rowCount()).isGreaterThan(0L);
            for(var row : response.rows()) {
                var globalCheckpoint = row[0];
                var maxSeqNo = row[1];
                assertThat(globalCheckpoint).isEqualTo(maxSeqNo);
            }
        });

        for (final Thread thread : threads) {
            thread.join();
        }
    }

    public void testPersistGlobalCheckpoint() throws Exception {
        cluster().ensureAtLeastNumDataNodes(2);
        if (randomBoolean()) {
            execute(
                "create table test(id integer) clustered into 1 shards with" +
                "(\"global_checkpoint_sync.interval\" = ?, \"translog.durability\" = 'ASYNC', number_of_replicas = ?, \"translog.sync_interval\" = ?)",
                new Object[]{randomTimeValue(100, 1000, "ms"), randomIntBetween(0, 1), randomTimeValue(100, 1000, "ms")}
            );
        } else {
            execute(
                "create table test(id integer) clustered into 1 shards with" +
                "(\"global_checkpoint_sync.interval\" = ?, number_of_replicas = ?)",
                new Object[]{randomTimeValue(100, 1000, "ms"), randomIntBetween(0, 1)}
            );
        }
        if (randomBoolean()) {
            ensureGreen(getFqn("test"));
        }
        int numDocs = randomIntBetween(1, 20);
        for (int i = 0; i < numDocs; i++) {
            execute("insert into test(id) values(?)", new Object[]{i});
        }
        ensureGreen(getFqn("test"));
        assertBusy(() -> {
            for (IndicesService indicesService : cluster().getDataNodeInstances(IndicesService.class)) {
                for (IndexService indexService : indicesService) {
                    for (IndexShard shard : indexService) {
                        final SeqNoStats seqNoStats = shard.seqNoStats();
                        assertThat(seqNoStats.getLocalCheckpoint()).isEqualTo(seqNoStats.getMaxSeqNo());
                        assertThat(shard.getLastKnownGlobalCheckpoint()).isEqualTo(seqNoStats.getMaxSeqNo());
                        assertThat(shard.getLastSyncedGlobalCheckpoint()).isEqualTo(seqNoStats.getMaxSeqNo());
                    }
                }
            }
        });
    }

    public void testPersistLocalCheckpoint() {
        cluster().ensureAtLeastNumDataNodes(2);
        execute(
            "create table test(id integer) clustered into 1 shards with" +
            "(\"global_checkpoint_sync.interval\" = ?, \"translog.durability\" = ?, number_of_replicas = ?)",
            new Object[]{"10ms", Translog.Durability.REQUEST.toString(), randomIntBetween(0, 1)}
        );

        var indexName = getFqn("test");
        ensureGreen(indexName);
        int numDocs = randomIntBetween(1, 20);
        logger.info("numDocs {}", numDocs);
        long maxSeqNo = 0;
        for (int i = 0; i < numDocs; i++) {
            maxSeqNo = (long) execute("insert into test(id) values(?) returning _seq_no", new Object[]{i}).rows()[0][0];
            logger.info("got {}", maxSeqNo);
        }
        for (IndicesService indicesService : cluster().getDataNodeInstances(IndicesService.class)) {
            for (IndexService indexService : indicesService) {
                for (IndexShard shard : indexService) {
                    final SeqNoStats seqNoStats = shard.seqNoStats();
                    assertThat(maxSeqNo).isEqualTo(seqNoStats.getMaxSeqNo());
                    assertThat(seqNoStats.getLocalCheckpoint()).isEqualTo(seqNoStats.getMaxSeqNo());
                }
            }
        }
    }
}
