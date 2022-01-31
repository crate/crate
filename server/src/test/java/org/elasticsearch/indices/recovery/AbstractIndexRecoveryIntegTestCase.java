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

package org.elasticsearch.indices.recovery;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.cluster.NodeConnectionsService;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.index.MockEngineFactoryPlugin;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.node.RecoverySettingsChunkSizePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.test.store.MockFSIndexStore;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.test.transport.StubbableTransport;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

import io.crate.common.unit.TimeValue;
import io.crate.integrationtests.SQLIntegrationTestCase;
import io.crate.testing.SQLResponse;

public abstract class AbstractIndexRecoveryIntegTestCase extends SQLIntegrationTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(
            MockTransportService.TestPlugin.class,
            MockFSIndexStore.TestPlugin.class,
            RecoverySettingsChunkSizePlugin.class,
            InternalSettingsPlugin.class,
            MockEngineFactoryPlugin.class
        );
    }

    @Override
    protected void beforeIndexDeletion() throws Exception {
        super.beforeIndexDeletion();
        internalCluster().assertConsistentHistoryBetweenTranslogAndLuceneIndex();
        internalCluster().assertSeqNos();
        internalCluster().assertSameDocIdsOnShards();
    }

    protected void checkTransientErrorsDuringRecoveryAreRetried(String recoveryActionToBlock) throws Exception {

        final Settings nodeSettings = Settings.builder()
            .put(RecoverySettings.INDICES_RECOVERY_RETRY_DELAY_NETWORK_SETTING.getKey(), "100ms")
            .put(NodeConnectionsService.CLUSTER_NODE_RECONNECT_INTERVAL_SETTING.getKey(), "500ms")
            .put(RecoverySettings.INDICES_RECOVERY_INTERNAL_ACTION_TIMEOUT_SETTING.getKey(), "10s")
            .build();
        // start a master node
        internalCluster().startNode(nodeSettings);

        final String blueNodeName = internalCluster().startNode(
            Settings.builder().put("node.attr.color", "blue").put(nodeSettings).build()
        );
        final String redNodeName = internalCluster().startNode(Settings.builder().put("node.attr.color", "red").put(nodeSettings).build());

        ClusterHealthResponse response = client().admin().cluster().prepareHealth().setWaitForNodes(">=3").get();
        assertThat(response.isTimedOut(), is(false));

        execute("create table doc.test (x integer) clustered into 1 shards with (number_of_replicas=0, routing.allocation.include.color='blue')");

        int numDocs = scaledRandomIntBetween(100, 8000);
        // Index 3/4 of the documents and flush. And then index the rest. This attempts to ensure that there
        // is a mix of file chunks and translog ops

        int threeFourths = (int) (numDocs * 0.75);

        var input = new Object[threeFourths];

        for (int i = 0; i < threeFourths; i++) {
            input[i] = i;
        }

        execute("insert into doc.test1 (x) (select * from unnest(?))", input);
        execute("optimize doc.test1");

        input = new Object[numDocs - threeFourths];

        for (int i = threeFourths; i < numDocs; i++) {
            input[i] = i;
        }

        execute("insert into doc.test1 (x) (select * from unnest(?))", input);

        ClusterStateResponse stateResponse = client().admin().cluster().prepareState().get();
        final String blueNodeId = internalCluster().getInstance(ClusterService.class, blueNodeName).localNode().getId();

        assertFalse(stateResponse.getState().getRoutingNodes().node(blueNodeId).isEmpty());

        SQLResponse result = execute("select count(*) from doc.test1");
        assertThat(result.rows()[0][0], is(numDocs));


        logger.info("--> will temporarily interrupt recovery action between blue & red on [{}]", recoveryActionToBlock);


        MockTransportService blueTransportService = (MockTransportService) internalCluster().getInstance(
            TransportService.class,
            blueNodeName
        );
        MockTransportService redTransportService = (MockTransportService) internalCluster().getInstance(
            TransportService.class,
            redNodeName
        );

        final AtomicBoolean recoveryStarted = new AtomicBoolean(false);
        final AtomicBoolean finalizeReceived = new AtomicBoolean(false);

        final SingleStartEnforcer validator = new SingleStartEnforcer("test", recoveryStarted, finalizeReceived);
        redTransportService.addSendBehavior(blueTransportService, (connection, requestId, action, request, options) -> {
            validator.accept(action, request);
            connection.sendRequest(requestId, action, request, options);
        });
        Runnable connectionBreaker = () -> {
            // Always break connection from source to remote to ensure that actions are retried
            logger.info("--> closing connections from source node to target node");
            blueTransportService.disconnectFromNode(redTransportService.getLocalDiscoNode());
            if (randomBoolean()) {
                // Sometimes break connection from remote to source to ensure that recovery is re-established
                logger.info("--> closing connections from target node to source node");
                redTransportService.disconnectFromNode(blueTransportService.getLocalDiscoNode());
            }
        };
        TransientReceiveRejected handlingBehavior = new TransientReceiveRejected(
            recoveryActionToBlock,
            finalizeReceived,
            recoveryStarted,
            connectionBreaker
        );
        redTransportService.addRequestHandlingBehavior(recoveryActionToBlock, handlingBehavior);

        try {

            execute("alter table doc.test set (number_of_replicas=1, routing.allocation.include.color='red,blue')");
            ensureGreen();

            result = execute("select count(*) from doc.test1");
            assertThat(result.rows()[0][0], is(numDocs));

        } finally {
            blueTransportService.clearAllRules();
            redTransportService.clearAllRules();
        }
    }

    public void checkDisconnectsWhileRecovering(String recoveryActionToBlock) throws Exception {
        final Settings nodeSettings = Settings.builder()
            .put(RecoverySettings.INDICES_RECOVERY_RETRY_DELAY_NETWORK_SETTING.getKey(), "100ms")
            .put(RecoverySettings.INDICES_RECOVERY_INTERNAL_ACTION_TIMEOUT_SETTING.getKey(), "1s")
            .put(NodeConnectionsService.CLUSTER_NODE_RECONNECT_INTERVAL_SETTING.getKey(), "1s")
            .build();
        // start a master node
        internalCluster().startNode(nodeSettings);

        final String blueNodeName = internalCluster().startNode(
            Settings.builder().put("node.attr.color", "blue").put(nodeSettings).build()
        );
        final String redNodeName = internalCluster().startNode(Settings.builder().put("node.attr.color", "red").put(nodeSettings).build());

        ClusterHealthResponse response = client().admin().cluster().prepareHealth().setWaitForNodes(">=3").get();
        assertThat(response.isTimedOut(), is(false));

        execute("create table doc.test (x integer) clustered into 1 shards with (number_of_replicas=0, routing.allocation.include.color='blue')");

        int numDocs = scaledRandomIntBetween(25, 250);
        var input = new Object[numDocs];
        for (int i = 0; i < numDocs; i++) {
            input[i] = i;
        }

        execute("insert into doc.test1 (x) (select * from unnest(?))", input);

        ClusterStateResponse stateResponse = client().admin().cluster().prepareState().get();
        final String blueNodeId = internalCluster().getInstance(ClusterService.class, blueNodeName).localNode().getId();

        assertFalse(stateResponse.getState().getRoutingNodes().node(blueNodeId).isEmpty());

        var result = execute("select count(*) from doc.test1");
        assertThat(result.rows()[0][0], is(numDocs));

        final boolean dropRequests = randomBoolean();
        logger.info("--> will {} between blue & red on [{}]", dropRequests ? "drop requests" : "break connection", recoveryActionToBlock);


        MockTransportService blueMockTransportService = (MockTransportService) internalCluster().getInstance(
            TransportService.class,
            blueNodeName
        );
        MockTransportService redMockTransportService = (MockTransportService) internalCluster().getInstance(
            TransportService.class,
            redNodeName
        );
        TransportService redTransportService = internalCluster().getInstance(TransportService.class, redNodeName);
        TransportService blueTransportService = internalCluster().getInstance(TransportService.class, blueNodeName);
        final CountDownLatch requestFailed = new CountDownLatch(1);

        if (randomBoolean()) {
            final StubbableTransport.SendRequestBehavior sendRequestBehavior = (connection, requestId, action, request, options) -> {
                if (recoveryActionToBlock.equals(action) || requestFailed.getCount() == 0) {
                    requestFailed.countDown();
                    logger.info("--> preventing {} request by throwing ConnectTransportException", action);
                    throw new ConnectTransportException(connection.getNode(), "DISCONNECT: prevented " + action + " request");
                }
                connection.sendRequest(requestId, action, request, options);
            };
            // Fail on the sending side
            blueMockTransportService.addSendBehavior(redTransportService, sendRequestBehavior);
            redMockTransportService.addSendBehavior(blueTransportService, sendRequestBehavior);
        } else {
            // Fail on the receiving side.
            blueMockTransportService.addRequestHandlingBehavior(recoveryActionToBlock, (handler, request, channel) -> {
                logger.info("--> preventing {} response by closing response channel", recoveryActionToBlock);
                requestFailed.countDown();
                redMockTransportService.disconnectFromNode(blueMockTransportService.getLocalDiscoNode());
                handler.messageReceived(request, channel);
            });
            redMockTransportService.addRequestHandlingBehavior(recoveryActionToBlock, (handler, request, channel) -> {
                logger.info("--> preventing {} response by closing response channel", recoveryActionToBlock);
                requestFailed.countDown();
                blueMockTransportService.disconnectFromNode(redMockTransportService.getLocalDiscoNode());
                handler.messageReceived(request, channel);
            });
        }

        logger.info("--> starting recovery from blue to red");

        execute("alter table doc.test set (number_of_replicas=1, routing.allocation.include.color='red,blue')");

        requestFailed.await();

        logger.info("--> clearing rules to allow recovery to proceed");
        blueMockTransportService.clearAllRules();
        redMockTransportService.clearAllRules();


    }

    public void checkDisconnectsDuringRecovery(boolean useSnapshotBasedRecoveries) throws Exception {
        boolean primaryRelocation = randomBoolean();
        final String indexName = "test";
        final Settings nodeSettings = Settings.builder()
            .put(
                RecoverySettings.INDICES_RECOVERY_RETRY_DELAY_NETWORK_SETTING.getKey(),
                TimeValue.timeValueMillis(randomIntBetween(0, 100))
            )
            .build();
        TimeValue disconnectAfterDelay = TimeValue.timeValueMillis(randomIntBetween(0, 100));
        // start a master node
        String masterNodeName = internalCluster().startMasterOnlyNode(nodeSettings);

        final String blueNodeName = internalCluster().startNode(
            Settings.builder().put("node.attr.color", "blue").put(nodeSettings).build()
        );
        final String redNodeName = internalCluster().startNode(Settings.builder().put("node.attr.color", "red").put(nodeSettings).build());


        execute("create table doc.test (x integer) clustered into 1 shards with (number_of_replicas=0, routing.allocation.include.color='blue')");

        int numDocs = scaledRandomIntBetween(25, 250);
        var input = new Object[numDocs];
        for (int i = 0; i < numDocs; i++) {
            input[i] = i;
        }


        execute("insert into doc.test1 (x) (select * from unnest(?))", input);

        ensureGreen();
        var result = execute("select count(*) from doc.test1");
        assertThat(result.rows()[0][0], is(numDocs));

        MockTransportService masterTransportService = (MockTransportService) internalCluster().getInstance(
            TransportService.class,
            masterNodeName
        );
        MockTransportService blueMockTransportService = (MockTransportService) internalCluster().getInstance(
            TransportService.class,
            blueNodeName
        );
        MockTransportService redMockTransportService = (MockTransportService) internalCluster().getInstance(
            TransportService.class,
            redNodeName
        );

        redMockTransportService.addSendBehavior(blueMockTransportService, new StubbableTransport.SendRequestBehavior() {
            private final AtomicInteger count = new AtomicInteger();

            @Override
            public void sendRequest(
                Transport.Connection connection,
                long requestId,
                String action,
                TransportRequest request,
                TransportRequestOptions options
            ) throws IOException {
                logger.info("--> sending request {} on {}", action, connection.getNode());
                if (PeerRecoverySourceService.Actions.START_RECOVERY.equals(action) && count.incrementAndGet() == 1) {
                    // ensures that it's considered as valid recovery attempt by source
                    try {
                        assertBusy(
                            () -> assertThat(
                                "Expected there to be some initializing shards",
                                client(blueNodeName).admin()
                                    .cluster()
                                    .prepareState()
                                    .setLocal(true)
                                    .get()
                                    .getState()
                                    .getRoutingTable()
                                    .index("test")
                                    .shard(0)
                                    .getAllInitializingShards(),
                                not(empty())
                            )
                        );
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                    connection.sendRequest(requestId, action, request, options);
                    try {
                        Thread.sleep(disconnectAfterDelay.millis());
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    throw new ConnectTransportException(
                        connection.getNode(),
                        "DISCONNECT: simulation disconnect after successfully sending " + action + " request"
                    );
                } else {
                    connection.sendRequest(requestId, action, request, options);
                }
            }
        });

        final AtomicBoolean finalized = new AtomicBoolean();
        blueMockTransportService.addSendBehavior(redMockTransportService, (connection, requestId, action, request, options) -> {
            logger.info("--> sending request {} on {}", action, connection.getNode());
            if (action.equals(PeerRecoveryTargetService.Actions.FINALIZE)) {
                finalized.set(true);
            }
            connection.sendRequest(requestId, action, request, options);
        });

        for (MockTransportService mockTransportService : Arrays.asList(redMockTransportService, blueMockTransportService)) {
            mockTransportService.addSendBehavior(masterTransportService, (connection, requestId, action, request, options) -> {
                logger.info("--> sending request {} on {}", action, connection.getNode());
                if ((primaryRelocation && finalized.get()) == false) {
                    assertNotEquals(action, ShardStateAction.SHARD_FAILED_ACTION_NAME);
                }
                connection.sendRequest(requestId, action, request, options);
            });
        }

        if (primaryRelocation) {
            logger.info("--> starting primary relocation recovery from blue to red");
            client().admin()
                .indices()
                .prepareUpdateSettings(indexName)
                .setSettings(Settings.builder().put(IndexMetadata.INDEX_ROUTING_INCLUDE_GROUP_SETTING.getKey() + "color", "red"))
                .get();

            ensureGreen(); // also waits for relocation / recovery to complete
            // if a primary relocation fails after the source shard has been marked as relocated, both source and target are failed. If the
            // source shard is moved back to started because the target fails first, it's possible that there is a cluster state where the
            // shard is marked as started again (and ensureGreen returns), but while applying the cluster state the primary is failed and
            // will be reallocated. The cluster will thus become green, then red, then green again. Triggering a refresh here before
            // searching helps, as in contrast to search actions, refresh waits for the closed shard to be reallocated.
            client().admin().indices().prepareRefresh(indexName).get();
        } else {
            logger.info("--> starting replica recovery from blue to red");
            execute("alter table doc.test set (number_of_replicas=1, routing.allocation.include.color='red,blue')");

            ensureGreen();
        }

        for (int i = 0; i < 10; i++) {
            result = execute("select count(*) from doc.test1");
            assertThat(result.rows()[0][0], is(numDocs));
        }
    }


    private class SingleStartEnforcer implements BiConsumer<String, TransportRequest> {

        private final AtomicBoolean recoveryStarted;
        private final AtomicBoolean finalizeReceived;
        private final String indexName;

        private SingleStartEnforcer(String indexName, AtomicBoolean recoveryStarted, AtomicBoolean finalizeReceived) {
            this.indexName = indexName;
            this.recoveryStarted = recoveryStarted;
            this.finalizeReceived = finalizeReceived;
        }

        @Override
        public void accept(String action, TransportRequest request) {
            // The cluster state applier will immediately attempt to retry the recovery on a cluster state
            // update. We want to assert that the first and only recovery attempt succeeds
            if (PeerRecoverySourceService.Actions.START_RECOVERY.equals(action)) {
                StartRecoveryRequest startRecoveryRequest = (StartRecoveryRequest) request;
                ShardId shardId = startRecoveryRequest.shardId();
                logger.info("--> attempting to send start_recovery request for shard: " + shardId);
                if (indexName.equals(shardId.getIndexName()) && recoveryStarted.get() && finalizeReceived.get() == false) {
                    throw new IllegalStateException("Recovery cannot be started twice");
                }
            }
        }
    }

    private class TransientReceiveRejected implements StubbableTransport.RequestHandlingBehavior<TransportRequest> {

        private final String actionName;
        private final AtomicBoolean recoveryStarted;
        private final AtomicBoolean finalizeReceived;
        private final Runnable connectionBreaker;
        private final AtomicInteger blocksRemaining;

        private TransientReceiveRejected(
            String actionName,
            AtomicBoolean recoveryStarted,
            AtomicBoolean finalizeReceived,
            Runnable connectionBreaker
        ) {
            this.actionName = actionName;
            this.recoveryStarted = recoveryStarted;
            this.finalizeReceived = finalizeReceived;
            this.connectionBreaker = connectionBreaker;
            this.blocksRemaining = new AtomicInteger(randomIntBetween(1, 3));
        }

        @Override
        public void messageReceived(TransportRequestHandler<TransportRequest> handler,
                                    TransportRequest request,
                                    TransportChannel channel) throws Exception {
            recoveryStarted.set(true);
            if (actionName.equals(PeerRecoveryTargetService.Actions.FINALIZE)) {
                finalizeReceived.set(true);
            }
            if (blocksRemaining.getAndUpdate(i -> i == 0 ? 0 : i - 1) != 0) {
                String rejected = "rejected";
                String circuit = "circuit";
                String network = "network";
                String reason = randomFrom(rejected, circuit, network);
                if (reason.equals(rejected)) {
                    logger.info("--> preventing {} response by throwing exception", actionName);
                    throw new EsRejectedExecutionException("", false);
                } else if (reason.equals(circuit)) {
                    logger.info("--> preventing {} response by throwing exception", actionName);
                    throw new CircuitBreakingException("Broken");
                } else if (reason.equals(network)) {
                    logger.info("--> preventing {} response by breaking connection", actionName);
                    connectionBreaker.run();
                } else {
                    throw new AssertionError("Unknown failure reason: " + reason);
                }
            }
            handler.messageReceived(request, channel);
        }
    }
}
