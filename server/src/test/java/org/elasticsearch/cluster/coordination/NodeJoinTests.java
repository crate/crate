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
package org.elasticsearch.cluster.coordination;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.coordination.CoordinationMetaData.VotingConfiguration;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.cluster.service.MasterServiceTests;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.BaseFuture;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.indices.cluster.FakeThreadPoolMasterService;
import org.elasticsearch.node.Node;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.test.transport.CapturingTransport;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RequestHandlerRegistry;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportResponseOptions;
import org.elasticsearch.transport.TransportService;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static org.elasticsearch.transport.TransportService.HANDSHAKE_ACTION_NAME;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

@TestLogging("org.elasticsearch.cluster.service:TRACE,org.elasticsearch.cluster.coordination:TRACE")
public class NodeJoinTests extends ESTestCase {

    private static ThreadPool threadPool;

    private MasterService masterService;
    private Coordinator coordinator;
    private DeterministicTaskQueue deterministicTaskQueue;
    private Transport transport;

    @BeforeClass
    public static void beforeClass() {
        threadPool = new TestThreadPool(NodeJoinTests.getTestClass().getName());
    }

    @AfterClass
    public static void afterClass() {
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        threadPool = null;
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        masterService.close();
    }

    private static ClusterState initialState(DiscoveryNode localNode, long term, long version,
                                             VotingConfiguration config) {
        return ClusterState.builder(new ClusterName(ClusterServiceUtils.class.getSimpleName()))
            .nodes(DiscoveryNodes.builder()
                .add(localNode)
                .localNodeId(localNode.getId()))
            .metaData(MetaData.builder()
                    .coordinationMetaData(
                        CoordinationMetaData.builder()
                        .term(term)
                        .lastAcceptedConfiguration(config)
                        .lastCommittedConfiguration(config)
                    .build()))
            .version(version)
            .blocks(ClusterBlocks.EMPTY_CLUSTER_BLOCK).build();
    }

    private void setupFakeMasterServiceAndCoordinator(long term, ClusterState initialState) {
        deterministicTaskQueue
            = new DeterministicTaskQueue(Settings.builder().put(Node.NODE_NAME_SETTING.getKey(), "test").build(), random());
        FakeThreadPoolMasterService fakeMasterService = new FakeThreadPoolMasterService("test_node","test",
                deterministicTaskQueue::scheduleNow);
        setupMasterServiceAndCoordinator(term, initialState, fakeMasterService, deterministicTaskQueue.getThreadPool(), Randomness.get());
        fakeMasterService.setClusterStatePublisher((event, publishListener, ackListener) -> {
            coordinator.handlePublishRequest(new PublishRequest(event.state()));
            publishListener.onResponse(null);
        });
        fakeMasterService.start();
    }

    private void setupRealMasterServiceAndCoordinator(long term, ClusterState initialState) {
        MasterService masterService = new MasterService("test_node", Settings.EMPTY, threadPool);
        AtomicReference<ClusterState> clusterStateRef = new AtomicReference<>(initialState);
        masterService.setClusterStatePublisher((event, publishListener, ackListener) -> {
            clusterStateRef.set(event.state());
            publishListener.onResponse(null);
        });
        setupMasterServiceAndCoordinator(term, initialState, masterService, threadPool, new Random(Randomness.get().nextLong()));
        masterService.setClusterStateSupplier(clusterStateRef::get);
        masterService.start();
    }

    private void setupMasterServiceAndCoordinator(long term, ClusterState initialState, MasterService masterService,
                                                  ThreadPool threadPool, Random random) {
        if (this.masterService != null || coordinator != null) {
            throw new IllegalStateException("method setupMasterServiceAndCoordinator can only be called once");
        }
        this.masterService = masterService;
        CapturingTransport capturingTransport = new CapturingTransport() {
            @Override
            protected void onSendRequest(long requestId, String action, TransportRequest request, DiscoveryNode destination) {
                if (action.equals(HANDSHAKE_ACTION_NAME)) {
                    handleResponse(requestId, new TransportService.HandshakeResponse(destination, initialState.getClusterName(),
                        destination.getVersion()));
                } else if (action.equals(JoinHelper.VALIDATE_JOIN_ACTION_NAME)) {
                    handleResponse(requestId, new TransportResponse.Empty());
                } else {
                    super.onSendRequest(requestId, action, request, destination);
                }
            }
        };
        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        TransportService transportService = capturingTransport.createTransportService(
            Settings.EMPTY,
            threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            x -> initialState.nodes().getLocalNode(),
            clusterSettings
        );
        coordinator = new Coordinator("test_node", Settings.EMPTY, clusterSettings,
            transportService, writableRegistry(),
            ESAllocationTestCase.createAllocationService(Settings.EMPTY),
            masterService,
            () -> new InMemoryPersistedState(term, initialState), r -> emptyList(),
            new NoOpClusterApplier(),
            Collections.emptyList(),
            random);
        transportService.start();
        transportService.acceptIncomingRequests();
        transport = capturingTransport;
        coordinator.start();
        coordinator.startInitialJoin();
    }

    protected DiscoveryNode newNode(int i) {
        return newNode(i, randomBoolean());
    }

    protected DiscoveryNode newNode(int i, boolean master) {
        Set<DiscoveryNode.Role> roles = new HashSet<>();
        if (master) {
            roles.add(DiscoveryNode.Role.MASTER);
        }
        final String prefix = master ? "master_" : "data_";
        return new DiscoveryNode(prefix + i, i + "", buildNewFakeTransportAddress(), emptyMap(), roles, Version.CURRENT);
    }

    static class SimpleFuture extends BaseFuture<Void> {
        final String description;

        SimpleFuture(String description) {
            this.description = description;
        }

        public void markAsDone() {
            set(null);
        }

        public void markAsFailed(Throwable t) {
            setException(t);
        }

        @Override
        public String toString() {
            return "future [" + description + "]";
        }
    }

    private SimpleFuture joinNodeAsync(final JoinRequest joinRequest) {
        final SimpleFuture future = new SimpleFuture("join of " + joinRequest + "]");
        logger.debug("starting {}", future);
        // clone the node before submitting to simulate an incoming join, which is guaranteed to have a new
        // disco node object serialized off the network
        try {
            final RequestHandlerRegistry<JoinRequest> joinHandler = (RequestHandlerRegistry<JoinRequest>)
                transport.getRequestHandler(JoinHelper.JOIN_ACTION_NAME);
            joinHandler.processMessageReceived(joinRequest, new TransportChannel() {
                @Override
                public String getProfileName() {
                    return "dummy";
                }

                @Override
                public String getChannelType() {
                    return "dummy";
                }

                @Override
                public void sendResponse(TransportResponse response) {
                    logger.debug("{} completed", future);
                    future.markAsDone();
                }

                @Override
                public void sendResponse(TransportResponse response,
                                         TransportResponseOptions options) throws IOException {
                    sendResponse(response);
                }

                @Override
                public void sendResponse(Exception e) {
                    logger.error(() -> new ParameterizedMessage("unexpected error for {}", future), e);
                    future.markAsFailed(e);
                }
            });
        } catch (Exception e) {
            logger.error(() -> new ParameterizedMessage("unexpected error for {}", future), e);
            future.markAsFailed(e);
        }
        return future;
    }

    private void joinNode(final JoinRequest joinRequest) {
        FutureUtils.get(joinNodeAsync(joinRequest));
    }

    private void joinNodeAndRun(final JoinRequest joinRequest) {
        SimpleFuture fut = joinNodeAsync(joinRequest);
        deterministicTaskQueue.runAllRunnableTasks();
        assertTrue(fut.isDone());
        FutureUtils.get(fut);
    }

    public void testJoinWithHigherTermElectsLeader() {
        DiscoveryNode node0 = newNode(0, true);
        DiscoveryNode node1 = newNode(1, true);
        long initialTerm = randomLongBetween(1, 10);
        long initialVersion = randomLongBetween(1, 10);
        setupFakeMasterServiceAndCoordinator(initialTerm, initialState(node0, initialTerm, initialVersion,
            new VotingConfiguration(Collections.singleton(randomFrom(node0, node1).getId()))));
        assertFalse(isLocalNodeElectedMaster());
        assertNull(coordinator.getStateForMasterService().nodes().getMasterNodeId());
        long newTerm = initialTerm + randomLongBetween(1, 10);
        SimpleFuture fut = joinNodeAsync(new JoinRequest(node1, Optional.of(new Join(node1, node0, newTerm, initialTerm, initialVersion))));
        assertEquals(Coordinator.Mode.LEADER, coordinator.getMode());
        assertNull(coordinator.getStateForMasterService().nodes().getMasterNodeId());
        deterministicTaskQueue.runAllRunnableTasks();
        assertTrue(fut.isDone());
        assertTrue(isLocalNodeElectedMaster());
        assertTrue(coordinator.getStateForMasterService().nodes().isLocalNodeElectedMaster());
    }

    public void testJoinWithHigherTermButBetterStateGetsRejected() {
        DiscoveryNode node0 = newNode(0, true);
        DiscoveryNode node1 = newNode(1, true);
        long initialTerm = randomLongBetween(1, 10);
        long initialVersion = randomLongBetween(1, 10);
        setupFakeMasterServiceAndCoordinator(initialTerm, initialState(node0, initialTerm, initialVersion,
            new VotingConfiguration(Collections.singleton(node1.getId()))));
        assertFalse(isLocalNodeElectedMaster());
        long newTerm = initialTerm + randomLongBetween(1, 10);
        long higherVersion = initialVersion + randomLongBetween(1, 10);
        expectThrows(CoordinationStateRejectedException.class,
            () -> joinNodeAndRun(new JoinRequest(node1, Optional.of(new Join(node1, node0, newTerm, initialTerm, higherVersion)))));
        assertFalse(isLocalNodeElectedMaster());
    }

    public void testJoinWithHigherTermButBetterStateStillElectsMasterThroughSelfJoin() {
        DiscoveryNode node0 = newNode(0, true);
        DiscoveryNode node1 = newNode(1, true);
        long initialTerm = randomLongBetween(1, 10);
        long initialVersion = randomLongBetween(1, 10);
        setupFakeMasterServiceAndCoordinator(initialTerm, initialState(node0, initialTerm, initialVersion,
            new VotingConfiguration(Collections.singleton(node0.getId()))));
        assertFalse(isLocalNodeElectedMaster());
        long newTerm = initialTerm + randomLongBetween(1, 10);
        long higherVersion = initialVersion + randomLongBetween(1, 10);
        joinNodeAndRun(new JoinRequest(node1, Optional.of(new Join(node1, node0, newTerm, initialTerm, higherVersion))));
        assertTrue(isLocalNodeElectedMaster());
    }

    public void testJoinElectedLeader() {
        DiscoveryNode node0 = newNode(0, true);
        DiscoveryNode node1 = newNode(1, true);
        long initialTerm = randomLongBetween(1, 10);
        long initialVersion = randomLongBetween(1, 10);
        setupFakeMasterServiceAndCoordinator(initialTerm, initialState(node0, initialTerm, initialVersion,
            new VotingConfiguration(Collections.singleton(node0.getId()))));
        assertFalse(isLocalNodeElectedMaster());
        long newTerm = initialTerm + randomLongBetween(1, 10);
        joinNodeAndRun(new JoinRequest(node0, Optional.of(new Join(node0, node0, newTerm, initialTerm, initialVersion))));
        assertTrue(isLocalNodeElectedMaster());
        assertFalse(clusterStateHasNode(node1));
        joinNodeAndRun(new JoinRequest(node1, Optional.of(new Join(node1, node0, newTerm, initialTerm, initialVersion))));
        assertTrue(isLocalNodeElectedMaster());
        assertTrue(clusterStateHasNode(node1));
    }

    public void testJoinAccumulation() {
        DiscoveryNode node0 = newNode(0, true);
        DiscoveryNode node1 = newNode(1, true);
        DiscoveryNode node2 = newNode(2, true);
        long initialTerm = randomLongBetween(1, 10);
        long initialVersion = randomLongBetween(1, 10);
        setupFakeMasterServiceAndCoordinator(initialTerm, initialState(node0, initialTerm, initialVersion,
            new VotingConfiguration(Collections.singleton(node2.getId()))));
        assertFalse(isLocalNodeElectedMaster());
        long newTerm = initialTerm + randomLongBetween(1, 10);
        SimpleFuture futNode0 = joinNodeAsync(new JoinRequest(node0, Optional.of(
            new Join(node0, node0, newTerm, initialTerm, initialVersion))));
        deterministicTaskQueue.runAllRunnableTasks();
        assertFalse(futNode0.isDone());
        assertFalse(isLocalNodeElectedMaster());
        SimpleFuture futNode1 = joinNodeAsync(new JoinRequest(node1, Optional.of(
            new Join(node1, node0, newTerm, initialTerm, initialVersion))));
        deterministicTaskQueue.runAllRunnableTasks();
        assertFalse(futNode1.isDone());
        assertFalse(isLocalNodeElectedMaster());
        joinNodeAndRun(new JoinRequest(node2, Optional.of(new Join(node2, node0, newTerm, initialTerm, initialVersion))));
        assertTrue(isLocalNodeElectedMaster());
        assertTrue(clusterStateHasNode(node1));
        assertTrue(clusterStateHasNode(node2));
        FutureUtils.get(futNode0);
        FutureUtils.get(futNode1);
    }

    public void testJoinFollowerWithHigherTerm() throws Exception {
        DiscoveryNode node0 = newNode(0, true);
        DiscoveryNode node1 = newNode(1, true);
        long initialTerm = randomLongBetween(1, 10);
        long initialVersion = randomLongBetween(1, 10);
        setupFakeMasterServiceAndCoordinator(initialTerm, initialState(node0, initialTerm, initialVersion,
            new VotingConfiguration(Collections.singleton(node0.getId()))));
        long newTerm = initialTerm + randomLongBetween(1, 10);
        handleStartJoinFrom(node1, newTerm);
        handleFollowerCheckFrom(node1, newTerm);
        long newerTerm = newTerm + randomLongBetween(1, 10);
        joinNodeAndRun(new JoinRequest(node1,
            Optional.of(new Join(node1, node0, newerTerm, initialTerm, initialVersion))));
        assertTrue(isLocalNodeElectedMaster());
    }

    private void handleStartJoinFrom(DiscoveryNode node, long term) throws Exception {
        final RequestHandlerRegistry<StartJoinRequest> startJoinHandler = (RequestHandlerRegistry<StartJoinRequest>)
            transport.getRequestHandler(JoinHelper.START_JOIN_ACTION_NAME);
        startJoinHandler.processMessageReceived(new StartJoinRequest(node, term), new TransportChannel() {
            @Override
            public String getProfileName() {
                return "dummy";
            }

            @Override
            public String getChannelType() {
                return "dummy";
            }

            @Override
            public void sendResponse(TransportResponse response) {

            }

            @Override
            public void sendResponse(TransportResponse response, TransportResponseOptions options) throws IOException {

            }

            @Override
            public void sendResponse(Exception exception) {
                fail();
            }
        });
        deterministicTaskQueue.runAllRunnableTasks();
        assertFalse(isLocalNodeElectedMaster());
        assertThat(coordinator.getMode(), equalTo(Coordinator.Mode.CANDIDATE));
    }

    private void handleFollowerCheckFrom(DiscoveryNode node, long term) throws Exception {
        final RequestHandlerRegistry<FollowersChecker.FollowerCheckRequest> followerCheckHandler =
            (RequestHandlerRegistry<FollowersChecker.FollowerCheckRequest>)
            transport.getRequestHandler(FollowersChecker.FOLLOWER_CHECK_ACTION_NAME);
        followerCheckHandler.processMessageReceived(new FollowersChecker.FollowerCheckRequest(term, node), new TransportChannel() {
            @Override
            public String getProfileName() {
                return "dummy";
            }

            @Override
            public String getChannelType() {
                return "dummy";
            }

            @Override
            public void sendResponse(TransportResponse response) {
            }

            @Override
            public void sendResponse(TransportResponse response, TransportResponseOptions options) throws IOException {
            }

            @Override
            public void sendResponse(Exception exception) {
                fail();
            }
        });
        deterministicTaskQueue.runAllRunnableTasks();
        assertFalse(isLocalNodeElectedMaster());
        assertThat(coordinator.getMode(), equalTo(Coordinator.Mode.FOLLOWER));
    }

    public void testJoinFollowerFails() throws Exception {
        DiscoveryNode node0 = newNode(0, true);
        DiscoveryNode node1 = newNode(1, true);
        long initialTerm = randomLongBetween(1, 10);
        long initialVersion = randomLongBetween(1, 10);
        setupFakeMasterServiceAndCoordinator(initialTerm, initialState(node0, initialTerm, initialVersion,
            new VotingConfiguration(Collections.singleton(node0.getId()))));
        long newTerm = initialTerm + randomLongBetween(1, 10);
        handleStartJoinFrom(node1, newTerm);
        handleFollowerCheckFrom(node1, newTerm);
        assertThat(expectThrows(CoordinationStateRejectedException.class,
            () -> joinNodeAndRun(new JoinRequest(node1, Optional.empty()))).getMessage(),
            containsString("join target is a follower"));
        assertFalse(isLocalNodeElectedMaster());
    }

    public void testBecomeFollowerFailsPendingJoin() throws Exception {
        DiscoveryNode node0 = newNode(0, true);
        DiscoveryNode node1 = newNode(1, true);
        long initialTerm = randomLongBetween(1, 10);
        long initialVersion = randomLongBetween(1, 10);
        setupFakeMasterServiceAndCoordinator(initialTerm, initialState(node0, initialTerm, initialVersion,
            new VotingConfiguration(Collections.singleton(node1.getId()))));
        long newTerm = initialTerm + randomLongBetween(1, 10);
        SimpleFuture fut = joinNodeAsync(new JoinRequest(node0, Optional.of(new Join(node0, node0, newTerm, initialTerm, initialVersion))));
        deterministicTaskQueue.runAllRunnableTasks();
        assertFalse(fut.isDone());
        assertFalse(isLocalNodeElectedMaster());
        handleFollowerCheckFrom(node1, newTerm);
        assertFalse(isLocalNodeElectedMaster());
        assertThat(expectThrows(CoordinationStateRejectedException.class,
            () -> FutureUtils.get(fut)).getMessage(),
            containsString("became follower"));
        assertFalse(isLocalNodeElectedMaster());
    }

    public void testConcurrentJoining() {
        List<DiscoveryNode> nodes = IntStream.rangeClosed(1, randomIntBetween(2, 5))
            .mapToObj(nodeId -> newNode(nodeId, true)).collect(Collectors.toList());

        DiscoveryNode localNode = nodes.get(0);
        VotingConfiguration votingConfiguration = new VotingConfiguration(randomValueOtherThan(singletonList(localNode),
            () -> randomSubsetOf(randomIntBetween(1, nodes.size()), nodes)).stream().map(DiscoveryNode::getId).collect(Collectors.toSet()));

        logger.info("Voting configuration: {}", votingConfiguration);

        long initialTerm = randomLongBetween(1, 10);
        long initialVersion = randomLongBetween(1, 10);
        setupRealMasterServiceAndCoordinator(initialTerm, initialState(localNode, initialTerm, initialVersion, votingConfiguration));
        long newTerm = initialTerm + randomLongBetween(1, 10);

        // we need at least a quorum of voting nodes with a correct term and worse state
        List<DiscoveryNode> successfulNodes;
        do {
            successfulNodes = randomSubsetOf(nodes);
        } while (votingConfiguration.hasQuorum(successfulNodes.stream().map(DiscoveryNode::getId).collect(Collectors.toList()))
            == false);

        logger.info("Successful voting nodes: {}", successfulNodes);

        List<JoinRequest> correctJoinRequests = successfulNodes.stream().map(
            node -> new JoinRequest(node, Optional.of(new Join(node, localNode, newTerm, initialTerm, initialVersion))))
            .collect(Collectors.toList());

        List<DiscoveryNode> possiblyUnsuccessfulNodes = new ArrayList<>(nodes);
        possiblyUnsuccessfulNodes.removeAll(successfulNodes);

        logger.info("Possibly unsuccessful voting nodes: {}", possiblyUnsuccessfulNodes);

        List<JoinRequest> possiblyFailingJoinRequests = possiblyUnsuccessfulNodes.stream().map(node -> {
            if (randomBoolean()) {
                // a correct request
                return new JoinRequest(node, Optional.of(new Join(node, localNode,
                    newTerm, initialTerm, initialVersion)));
            } else if (randomBoolean()) {
                // term too low
                return new JoinRequest(node, Optional.of(new Join(node, localNode,
                    randomLongBetween(0, initialTerm), initialTerm, initialVersion)));
            } else {
                // better state
                return new JoinRequest(node, Optional.of(new Join(node, localNode,
                    newTerm, initialTerm, initialVersion + randomLongBetween(1, 10))));
            }
        }).collect(Collectors.toList());

        // duplicate some requests, which will be unsuccessful
        possiblyFailingJoinRequests.addAll(randomSubsetOf(possiblyFailingJoinRequests));

        CyclicBarrier barrier = new CyclicBarrier(correctJoinRequests.size() + possiblyFailingJoinRequests.size() + 1);
        final Runnable awaitBarrier = () -> {
            try {
                barrier.await();
            } catch (InterruptedException | BrokenBarrierException e) {
                throw new RuntimeException(e);
            }
        };

        final AtomicBoolean stopAsserting = new AtomicBoolean();
        final Thread assertionThread = new Thread(() -> {
            awaitBarrier.run();
            while (stopAsserting.get() == false) {
                coordinator.invariant();
            }
        }, "assert invariants");

        final List<Thread> joinThreads = Stream.concat(correctJoinRequests.stream().map(joinRequest ->
            new Thread(() -> {
                awaitBarrier.run();
                joinNode(joinRequest);
            }, "process " + joinRequest)), possiblyFailingJoinRequests.stream().map(joinRequest ->
            new Thread(() -> {
                awaitBarrier.run();
                try {
                    joinNode(joinRequest);
                } catch (CoordinationStateRejectedException e) {
                    // ignore - these requests are expected to fail
                }
            }, "process " + joinRequest))).collect(Collectors.toList());

        assertionThread.start();
        joinThreads.forEach(Thread::start);
        joinThreads.forEach(t -> {
            try {
                t.join();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
        stopAsserting.set(true);
        try {
            assertionThread.join();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        assertTrue(MasterServiceTests.discoveryState(masterService).nodes().isLocalNodeElectedMaster());
        for (DiscoveryNode successfulNode : successfulNodes) {
            assertTrue(successfulNode.toString(), clusterStateHasNode(successfulNode));
            assertTrue(successfulNode.toString(), coordinator.hasJoinVoteFrom(successfulNode));
        }
    }

    private boolean isLocalNodeElectedMaster() {
        return MasterServiceTests.discoveryState(masterService).nodes().isLocalNodeElectedMaster();
    }

    private boolean clusterStateHasNode(DiscoveryNode node) {
        return node.equals(MasterServiceTests.discoveryState(masterService).nodes().get(node.getId()));
    }
}
