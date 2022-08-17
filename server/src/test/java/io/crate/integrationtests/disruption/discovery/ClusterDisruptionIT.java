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

package io.crate.integrationtests.disruption.discovery;

import static io.crate.metadata.IndexParts.toIndexName;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.index.CorruptIndexException;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.routing.Murmur3HashFunction;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardTestCase;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.IntegTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.test.TestCluster;
import org.elasticsearch.test.disruption.NetworkDisruption;
import org.elasticsearch.test.disruption.NetworkDisruption.Bridge;
import org.elasticsearch.test.disruption.NetworkDisruption.NetworkLinkDisruptionType;
import org.elasticsearch.test.disruption.ServiceDisruptionScheme;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.junit.Test;

import io.crate.common.unit.TimeValue;
import io.crate.exceptions.DuplicateKeyException;

/**
 * Tests various cluster operations (e.g., indexing) during disruptions.
 */
@TestLogging("_root:DEBUG,org.elasticsearch.cluster.service:TRACE")
@IntegTestCase.ClusterScope(scope = IntegTestCase.Scope.TEST, numDataNodes = 0)
@IntegTestCase.Slow
public class ClusterDisruptionIT extends AbstractDisruptionTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(InternalSettingsPlugin.class);
        return plugins;
    }

    /**
     * Test that we do not loose document whose indexing request was successful, under a randomly selected disruption scheme
     * We also collect &amp; report the type of indexing failures that occur.
     * <p>
     * This test is a superset of tests run in the Jepsen test suite, with the exception of versioned updates
     */
    @TestLogging("_root:DEBUG,org.elasticsearch.action.bulk:TRACE,org.elasticsearch.action.get:TRACE," +
                 "org.elasticsearch.discovery:TRACE,org.elasticsearch.action.support.replication:TRACE," +
                 "org.elasticsearch.cluster.service:TRACE,org.elasticsearch.indices.recovery:TRACE," +
                 "org.elasticsearch.indices.cluster:TRACE,org.elasticsearch.index.shard:TRACE")
    @Test
    public void testAckedIndexing() throws Exception {
        final List<String> nodes = startCluster(3);

        int numberOfShards = 1 + randomInt(2);
        int replicas = randomInt(2);

        logger.info("creating table t clustered into {} shards with {} replicas", numberOfShards, replicas);
        execute("create table t (id int primary key, x string) clustered into " + numberOfShards + " shards " +
                "with (number_of_replicas = " + replicas + ", \"write.wait_for_active_shards\" = 1, \"global_checkpoint_sync.interval\"='1s')");
        ensureGreen();

        ServiceDisruptionScheme disruptionScheme = addRandomDisruptionScheme();
        logger.info("disruption scheme [{}] added", disruptionScheme);

        final ConcurrentHashMap<String, String> ackedDocs = new ConcurrentHashMap<>(); // id -> node sent.

        final AtomicBoolean stop = new AtomicBoolean(false);
        List<Thread> indexers = new ArrayList<>(nodes.size());
        List<Semaphore> semaphores = new ArrayList<>(nodes.size());
        final AtomicInteger idGenerator = new AtomicInteger(0);
        final AtomicReference<CountDownLatch> countDownLatchRef = new AtomicReference<>();
        final List<Exception> exceptedExceptions = new CopyOnWriteArrayList<>();

        logger.info("starting indexers");
        try {
            for (final String node : nodes) {
                final Semaphore semaphore = new Semaphore(0);
                semaphores.add(semaphore);
                final String name = "indexer_" + indexers.size();
                Thread thread = new Thread(() -> {
                    while (!stop.get()) {
                        String id = null;
                        try {
                            if (!semaphore.tryAcquire(10, TimeUnit.SECONDS)) {
                                continue;
                            }
                            logger.info("[{}] Acquired semaphore and it has {} permits left",
                                        name,
                                        semaphore.availablePermits());
                            try {
                                id = String.valueOf(idGenerator.incrementAndGet());
                                int shard = Math.floorMod(Murmur3HashFunction.hash(id), numberOfShards);
                                logger.trace("[{}] indexing id [{}] through node [{}] targeting shard [{}]",
                                             name,
                                             id,
                                             node,
                                             shard);

                                execute("insert into t (id, x) values (?, ?)",
                                        new Object[]{id, randomInt(100)},
                                        node,
                                        TimeValue.timeValueSeconds(1L));
                                ackedDocs.put(id, node);
                                logger.trace("[{}] indexed id [{}] through node [{}], response [{}]",
                                             name,
                                             id,
                                             node,
                                             response);
                            } catch (ElasticsearchException | DuplicateKeyException e) {
                                exceptedExceptions.add(e);
                                final String rowId = id;
                                logger.trace(() -> new ParameterizedMessage("[{}] failed id [{}] through node [{}]",
                                                                            name,
                                                                            rowId,
                                                                            node), e);
                            } finally {
                                countDownLatchRef.get().countDown();
                                logger.trace("[{}] decreased counter : {}", name, countDownLatchRef.get().getCount());
                            }
                        } catch (InterruptedException e) {
                            // fine - semaphore interrupt
                        } catch (AssertionError | Exception e) {
                            logger.trace(() -> new ParameterizedMessage(
                                "unexpected exception in background thread of [{}]",
                                node), e);
                        }
                    }
                });

                thread.setName(name);
                thread.start();
                indexers.add(thread);
            }

            int docsPerIndexer = randomInt(3);
            logger.info("indexing {} docs per indexer before partition", docsPerIndexer);
            countDownLatchRef.set(new CountDownLatch(docsPerIndexer * indexers.size()));
            for (Semaphore semaphore : semaphores) {
                semaphore.release(docsPerIndexer);
            }
            assertTrue(countDownLatchRef.get().await(1, TimeUnit.MINUTES));

            for (int iter = 1 + randomInt(1); iter > 0; iter--) {
                logger.info("starting disruptions & indexing (iteration [{}])", iter);
                disruptionScheme.startDisrupting();

                docsPerIndexer = randomIntBetween(1, 4);
                logger.info("indexing {} docs per indexer during partition", docsPerIndexer);

                countDownLatchRef.set(new CountDownLatch(docsPerIndexer * indexers.size()));
                Collections.shuffle(semaphores, random());
                for (Semaphore semaphore : semaphores) {
                    assertThat(semaphore.availablePermits(), equalTo(0));
                    semaphore.release(docsPerIndexer);
                }
                logger.info("waiting for indexing requests to complete");
                assertThat("indexing requests must complete", countDownLatchRef.get().await(20, TimeUnit.SECONDS), is(true));

                logger.info("stopping disruption");
                disruptionScheme.stopDisrupting();
                for (String node : internalCluster().getNodeNames()) {
                    ensureStableCluster(nodes.size(), TimeValue.timeValueMillis(disruptionScheme.expectedTimeToHeal().millis() +
                                                                                DISRUPTION_HEALING_OVERHEAD.millis()), true, node);
                }
                // in case of a bridge partition, shard allocation can fail "index.allocation.max_retries" times if the master
                // is the super-connected node and recovery source and target are on opposite sides of the bridge
                if (disruptionScheme instanceof NetworkDisruption &&
                    ((NetworkDisruption) disruptionScheme).getDisruptedLinks() instanceof Bridge) {
                    logger.warn("retrying failed allocations in case of a bridge partition");
                    execute("ALTER CLUSTER REROUTE RETRY FAILED");
                }
                ensureGreen();

                logger.info("validating successful docs");
                assertBusy(() -> {
                    for (String node : nodes) {
                        try {
                            logger.debug("validating through node [{}] ([{}] acked docs)", node, ackedDocs.size());
                            for (String id : ackedDocs.keySet()) {
                                execute("select * from t where id = ?", new Object[]{id}, node);
                                assertThat("doc [" + id + "] indexed via node [" + ackedDocs.get(id) + "] not found",
                                    response.rowCount(), is(1L));
                            }
                        } catch (AssertionError | NoShardAvailableActionException e) {
                            throw new AssertionError(e.getMessage() + " (checked via node [" + node + "]", e);
                        }
                    }
                }, 30, TimeUnit.SECONDS);

                logger.info("done validating (iteration [{}])", iter);
            }
        } finally {
            logger.info("shutting down indexers");
            stop.set(true);
            for (Thread indexer : indexers) {
                indexer.interrupt();
                indexer.join(60000);
            }
            if (exceptedExceptions.size() > 0) {
                StringBuilder sb = new StringBuilder();
                for (Exception e : exceptedExceptions) {
                    sb.append("\n").append(e.getMessage());
                }
                logger.debug("Indexing exceptions during disruption: {}", sb);
            }
        }
    }

    /**
     *  Test that a document which is indexed on the majority side of a partition, is available from the minority side,
     *  once the partition is healed
     */
    @Test
    public void testRejoinDocumentExistsInAllShardCopies() throws Exception {
        List<String> nodes = startCluster(3);

        execute("create table t (id int primary key, x string) clustered into 1 shards with (number_of_replicas = 2, " +
                "\"write.wait_for_active_shards\" = 1)");
        ensureGreen();

        nodes = new ArrayList<>(nodes);
        Collections.shuffle(nodes, random());
        String isolatedNode = nodes.get(0);
        String notIsolatedNode = nodes.get(1);

        NetworkDisruption.TwoPartitions partitions = isolateNode(isolatedNode);
        NetworkDisruption scheme = addRandomDisruptionType(partitions);
        scheme.startDisrupting();
        ensureStableCluster(2, notIsolatedNode);
        String indexName = toIndexName(sqlExecutor.getCurrentSchema(), "t", null);
        assertFalse(FutureUtils.get(
            client(notIsolatedNode).admin().cluster().health(
                new ClusterHealthRequest(indexName).waitForYellowStatus()
            )).isTimedOut());

        execute("insert into t (id, x) values (1, 10)", null, notIsolatedNode);

        logger.info("Verifying if document exists via node[{}]", notIsolatedNode);

        execute("select * from t where id = '1'", null, notIsolatedNode);
        assertThat(response.rowCount(), is(1L));

        scheme.stopDisrupting();

        ensureStableCluster(3);
        ensureGreen();

        for (String node : nodes) {
            logger.info("Verifying if document exists after isolating node[{}] via node[{}]", isolatedNode, node);
            execute("select * from t where id = '1'", null, node);
            assertThat(response.rowCount(), is(1L));
        }
    }

    // simulate handling of sending shard failure during an isolation
    @Test
    public void testSendingShardFailure() throws Exception {
        List<String> nodes = startCluster(3);
        String masterNode = internalCluster().getMasterName();
        List<String> nonMasterNodes = nodes.stream().filter(node -> !node.equals(masterNode)).collect(Collectors.toList());
        String nonMasterNode = randomFrom(nonMasterNodes);
        execute("create table t (id int primary key, x string) clustered into 3 shards with (number_of_replicas = 2)");
        ensureGreen();
        String nonMasterNodeId = internalCluster().clusterService(nonMasterNode).localNode().getId();

        // fail a random shard
        ShardRouting failedShard =
            randomFrom(clusterService().state().getRoutingNodes().node(nonMasterNodeId).shardsWithState(
                ShardRoutingState.STARTED));
        ShardStateAction service = internalCluster().getInstance(ShardStateAction.class, nonMasterNode);
        CountDownLatch latch = new CountDownLatch(1);
        AtomicBoolean success = new AtomicBoolean();

        String isolatedNode = randomBoolean() ? masterNode : nonMasterNode;
        NetworkDisruption.TwoPartitions partitions = isolateNode(isolatedNode);
        // we cannot use the NetworkUnresponsive disruption type here as it will swallow the "shard failed" request, calling neither
        // onSuccess nor onFailure on the provided listener.
        NetworkLinkDisruptionType disruptionType = new NetworkDisruption.NetworkDisconnect();
        NetworkDisruption networkDisruption = new NetworkDisruption(partitions, disruptionType);
        setDisruptionScheme(networkDisruption);
        networkDisruption.startDisrupting();

        service.localShardFailed(failedShard, "simulated", new CorruptIndexException("simulated", (String) null),
            new ActionListener<>() {
                @Override
                public void onResponse(Void aVoid) {
                    success.set(true);
                    latch.countDown();
                }

                @Override
                public void onFailure(Exception e) {
                    success.set(false);
                    latch.countDown();
                    assert false;
                }
            });

        if (isolatedNode.equals(nonMasterNode)) {
            assertNoMaster(nonMasterNode);
        } else {
            ensureStableCluster(2, nonMasterNode);
        }

        // heal the partition
        networkDisruption.removeAndEnsureHealthy(internalCluster());

        // the cluster should stabilize
        ensureStableCluster(3);

        latch.await();

        // the listener should be notified
        assertTrue(success.get());

        // the failed shard should be gone
        List<ShardRouting> shards = clusterService().state().getRoutingTable()
            .allShards(toIndexName(sqlExecutor.getCurrentSchema(), "t", null));
        for (ShardRouting shard : shards) {
            assertThat(shard.allocationId(), not(equalTo(failedShard.allocationId())));
        }
    }

    @Test
    public void testRestartNodeWhileIndexing() throws Exception {
        startCluster(3);
        String index = toIndexName(sqlExecutor.getCurrentSchema(), "t", null);

        int numberOfReplicas = between(1, 2);
        logger.info("creating table with {} shards and {} replicas", 1, numberOfReplicas);
        execute("create table t (id int primary key, x string) clustered into 1 shards with (number_of_replicas = 2, " +
                "\"write.wait_for_active_shards\" = 1)");
        AtomicBoolean stopped = new AtomicBoolean();
        Thread[] threads = new Thread[between(1, 4)];
        AtomicInteger docID = new AtomicInteger();
        Set<String> ackedDocs = ConcurrentCollections.newConcurrentSet();
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(() -> {
                while (stopped.get() == false && docID.get() < 5000) {
                    String id = Integer.toString(docID.incrementAndGet());
                    try {
                        execute("insert into t (id, x) values (?, ?)", new Object[]{id, randomInt(5000)});
                        logger.info("--> index id={}", id);
                        ackedDocs.add(id);
                    } catch (Exception ignore) {
                        logger.info("--> fail to index id={}", id);
                    }
                }
            });
            threads[i].start();
        }
        ensureGreen();
        assertBusy(() -> assertThat(docID.get(), greaterThanOrEqualTo(100)));
        internalCluster().restartRandomDataNode(new TestCluster.RestartCallback());
        ensureGreen();
        assertBusy(() -> assertThat(docID.get(), greaterThanOrEqualTo(200)));
        stopped.set(true);
        for (Thread thread : threads) {
            thread.join();
        }
        ClusterState clusterState = internalCluster().clusterService().state();
        for (ShardRouting shardRouting : clusterState.routingTable().allShards(index)) {
            String nodeName = clusterState.nodes().get(shardRouting.currentNodeId()).getName();
            IndicesService indicesService = internalCluster().getInstance(IndicesService.class, nodeName);
            IndexShard shard = indicesService.getShardOrNull(shardRouting.shardId());
            Set<String> docs = IndexShardTestCase.getShardDocUIDs(shard);
            assertThat("shard [" + shard.routingEntry() + "] docIds [" + docs + "] vs " + " acked docIds [" + ackedDocs + "]",
                ackedDocs, everyItem(is(in(docs))));
        }
    }
}
