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

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;
import static org.elasticsearch.monitor.StatusInfo.Status.HEALTHY;
import static org.elasticsearch.transport.TransportService.HANDSHAKE_ACTION_NAME;

import java.util.ArrayList;
import java.util.Collections;
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

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainFuture;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.coordination.CoordinationMetadata.VotingConfiguration;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.FakeThreadPoolMasterService;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.cluster.service.MasterServiceTests;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.monitor.NodeHealthService;
import org.elasticsearch.monitor.StatusInfo;
import org.elasticsearch.node.Node;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.CapturingTransport;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RequestHandlerRegistry;
import org.elasticsearch.transport.TestTransportChannel;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

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
            .metadata(Metadata.builder()
                    .coordinationMetadata(
                        CoordinationMetadata.builder()
                        .term(term)
                        .lastAcceptedConfiguration(config)
                        .lastCommittedConfiguration(config)
                    .build()))
            .version(version)
            .blocks(ClusterBlocks.EMPTY_CLUSTER_BLOCK).build();
    }

    private void setupFakeMasterServiceAndCoordinator(long term, ClusterState initialState, NodeHealthService nodeHealthService) {
        deterministicTaskQueue
            = new DeterministicTaskQueue(Settings.builder().put(Node.NODE_NAME_SETTING.getKey(), "test").build(), random());
        FakeThreadPoolMasterService fakeMasterService = new FakeThreadPoolMasterService("test_node","test", deterministicTaskQueue::scheduleNow);
        setupMasterServiceAndCoordinator(term, initialState, fakeMasterService,  deterministicTaskQueue.getThreadPool(), Randomness.get(), nodeHealthService);
        fakeMasterService.setClusterStatePublisher((event, publishListener, ackListener) -> {
            coordinator.handlePublishRequest(new PublishRequest(event.state()));
            publishListener.onResponse(null);
        });
        fakeMasterService.start();
    }

    private void setupRealMasterServiceAndCoordinator(long term, ClusterState initialState) {
        MasterService masterService = new MasterService(Settings.builder().put(Node.NODE_NAME_SETTING.getKey(), "test_node").build(),
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS), threadPool);
        AtomicReference<ClusterState> clusterStateRef = new AtomicReference<>(initialState);
        masterService.setClusterStatePublisher((event, publishListener, ackListener) -> {
            clusterStateRef.set(event.state());
            publishListener.onResponse(null);
        });
        setupMasterServiceAndCoordinator(term, initialState, masterService, threadPool, new Random(Randomness.get().nextLong()),
            () -> new StatusInfo(HEALTHY, "healthy-info"));
        masterService.setClusterStateSupplier(clusterStateRef::get);
        masterService.start();
    }

    private void setupMasterServiceAndCoordinator(long term, ClusterState initialState, MasterService masterService,
                                                  ThreadPool threadPool, Random random, NodeHealthService nodeHealthService) {
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
            x -> initialState.nodes().getLocalNode(),
            clusterSettings
        );
        coordinator = new Coordinator(
            "test_node",
            Settings.EMPTY,
            clusterSettings,
            transportService, writableRegistry(),
            ESAllocationTestCase.createAllocationService(Settings.EMPTY),
            masterService,
            () -> new InMemoryPersistedState(term, initialState), r -> emptyList(),
            new NoOpClusterApplier(),
            Collections.emptyList(),
            random, (s, p, r) -> {}, ElectionStrategy.DEFAULT_INSTANCE, nodeHealthService);
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
        final Set<DiscoveryNodeRole> roles;
        if (master) {
            roles = singleton(DiscoveryNodeRole.MASTER_ROLE);
        } else {
            roles = Set.of();
        }
        final String prefix = master ? "master_" : "data_";
        return new DiscoveryNode(prefix + i, i + "", buildNewFakeTransportAddress(), emptyMap(), roles, Version.CURRENT);
    }

    static class SimpleFuture extends PlainFuture<Void> {
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
            final RequestHandlerRegistry<JoinRequest> joinHandler = transport.getRequestHandlers().getHandler(JoinHelper.JOIN_ACTION_NAME);
            final ActionListener<TransportResponse> listener = new ActionListener<TransportResponse>() {

                @Override
                public void onResponse(TransportResponse transportResponse) {
                    logger.debug("{} completed", future);
                    future.markAsDone();
                }

                @Override
                public void onFailure(Exception e) {
                    logger.error(() -> new ParameterizedMessage("unexpected error for {}", future), e);
                    future.markAsFailed(e);
                }
            };

            joinHandler.processMessageReceived(joinRequest, new TestTransportChannel(listener));
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
        assertThat(fut.isDone()).isTrue();
        FutureUtils.get(fut);
    }

    @Test
    public void testJoinWithHigherTermElectsLeader() {
        DiscoveryNode node0 = newNode(0, true);
        DiscoveryNode node1 = newNode(1, true);
        long initialTerm = randomLongBetween(1, 10);
        long initialVersion = randomLongBetween(1, 10);
        setupFakeMasterServiceAndCoordinator(initialTerm, initialState(node0, initialTerm, initialVersion,
            VotingConfiguration.of(randomFrom(node0, node1))),
            () -> new StatusInfo(HEALTHY, "healthy-info"));
        assertThat(isLocalNodeElectedMaster()).isFalse();
        assertThat(coordinator.getStateForMasterService().nodes().getMasterNodeId()).isNull();
        long newTerm = initialTerm + randomLongBetween(1, 10);
        SimpleFuture fut = joinNodeAsync(new JoinRequest(node1, newTerm,
            Optional.of(new Join(node1, node0, newTerm, initialTerm, initialVersion))));
        assertThat(coordinator.getMode()).isEqualTo(Coordinator.Mode.LEADER);
        assertThat(coordinator.getStateForMasterService().nodes().getMasterNodeId()).isNull();
        deterministicTaskQueue.runAllRunnableTasks();
        assertThat(fut.isDone()).isTrue();
        assertThat(isLocalNodeElectedMaster()).isTrue();
        assertThat(coordinator.getStateForMasterService().nodes().isLocalNodeElectedMaster()).isTrue();
    }

    @Test
    public void testJoinWithHigherTermButBetterStateGetsRejected() {
        DiscoveryNode node0 = newNode(0, true);
        DiscoveryNode node1 = newNode(1, true);
        long initialTerm = randomLongBetween(1, 10);
        long initialVersion = randomLongBetween(1, 10);
        setupFakeMasterServiceAndCoordinator(initialTerm, initialState(node0, initialTerm, initialVersion,
            VotingConfiguration.of(node1)), () -> new StatusInfo(HEALTHY, "healthy-info"));
        assertThat(isLocalNodeElectedMaster()).isFalse();
        long newTerm = initialTerm + randomLongBetween(1, 10);
        long higherVersion = initialVersion + randomLongBetween(1, 10);
        assertThatThrownBy(() -> joinNodeAndRun(new JoinRequest(node1, newTerm,
                Optional.of(new Join(node1, node0, newTerm, initialTerm, higherVersion)))))
            .isExactlyInstanceOf(CoordinationStateRejectedException.class);
        assertThat(isLocalNodeElectedMaster()).isFalse();
    }

    @Test
    public void testJoinWithHigherTermButBetterStateStillElectsMasterThroughSelfJoin() {
        DiscoveryNode node0 = newNode(0, true);
        DiscoveryNode node1 = newNode(1, true);
        long initialTerm = randomLongBetween(1, 10);
        long initialVersion = randomLongBetween(1, 10);
        setupFakeMasterServiceAndCoordinator(initialTerm, initialState(node0, initialTerm, initialVersion,
            VotingConfiguration.of(node0)), () -> new StatusInfo(HEALTHY, "healthy-info"));
        assertThat(isLocalNodeElectedMaster()).isFalse();
        long newTerm = initialTerm + randomLongBetween(1, 10);
        long higherVersion = initialVersion + randomLongBetween(1, 10);
        joinNodeAndRun(new JoinRequest(node1, newTerm, Optional.of(new Join(node1, node0, newTerm, initialTerm, higherVersion))));
        assertThat(isLocalNodeElectedMaster()).isTrue();
    }

    @Test
    public void testJoinElectedLeader() {
        DiscoveryNode node0 = newNode(0, true);
        DiscoveryNode node1 = newNode(1, true);
        long initialTerm = randomLongBetween(1, 10);
        long initialVersion = randomLongBetween(1, 10);
        setupFakeMasterServiceAndCoordinator(initialTerm, initialState(node0, initialTerm, initialVersion,
            VotingConfiguration.of(node0)), () -> new StatusInfo(HEALTHY, "healthy-info"));
        assertThat(isLocalNodeElectedMaster()).isFalse();
        long newTerm = initialTerm + randomLongBetween(1, 10);
        joinNodeAndRun(new JoinRequest(node0, newTerm, Optional.of(new Join(node0, node0, newTerm, initialTerm, initialVersion))));
        assertThat(isLocalNodeElectedMaster()).isTrue();
        assertThat(clusterStateHasNode(node1)).isFalse();
        joinNodeAndRun(new JoinRequest(node1, newTerm, Optional.of(new Join(node1, node0, newTerm, initialTerm, initialVersion))));
        assertThat(isLocalNodeElectedMaster()).isTrue();
        assertThat(clusterStateHasNode(node1)).isTrue();
    }

    @Test
    public void testJoinElectedLeaderWithHigherTerm() {
        DiscoveryNode node0 = newNode(0, true);
        DiscoveryNode node1 = newNode(1, true);
        long initialTerm = randomLongBetween(1, 10);
        long initialVersion = randomLongBetween(1, 10);
        setupFakeMasterServiceAndCoordinator(initialTerm, initialState(node0, initialTerm, initialVersion,
            VotingConfiguration.of(node0)), () -> new StatusInfo(HEALTHY, "healthy-info"));
        long newTerm = initialTerm + randomLongBetween(1, 10);

        joinNodeAndRun(new JoinRequest(node0, newTerm, Optional.of(new Join(node0, node0, newTerm, initialTerm, initialVersion))));
        assertThat(isLocalNodeElectedMaster()).isTrue();

        long newerTerm = newTerm + randomLongBetween(1, 10);
        joinNodeAndRun(new JoinRequest(node1, newerTerm, Optional.empty()));
        assertThat(coordinator.getCurrentTerm()).isGreaterThanOrEqualTo(newerTerm);
        assertThat(isLocalNodeElectedMaster()).isTrue();
    }

    @Test
    public void testJoinAccumulation() {
        DiscoveryNode node0 = newNode(0, true);
        DiscoveryNode node1 = newNode(1, true);
        DiscoveryNode node2 = newNode(2, true);
        long initialTerm = randomLongBetween(1, 10);
        long initialVersion = randomLongBetween(1, 10);
        setupFakeMasterServiceAndCoordinator(initialTerm, initialState(node0, initialTerm, initialVersion,
            VotingConfiguration.of(node2)), () -> new StatusInfo(HEALTHY, "healthy-info"));
        assertThat(isLocalNodeElectedMaster()).isFalse();
        long newTerm = initialTerm + randomLongBetween(1, 10);
        SimpleFuture futNode0 = joinNodeAsync(new JoinRequest(node0, newTerm, Optional.of(
            new Join(node0, node0, newTerm, initialTerm, initialVersion))));
        deterministicTaskQueue.runAllRunnableTasks();
        assertThat(futNode0.isDone()).isFalse();
        assertThat(isLocalNodeElectedMaster()).isFalse();
        SimpleFuture futNode1 = joinNodeAsync(new JoinRequest(node1, newTerm, Optional.of(
            new Join(node1, node0, newTerm, initialTerm, initialVersion))));
        deterministicTaskQueue.runAllRunnableTasks();
        assertThat(futNode1.isDone()).isFalse();
        assertThat(isLocalNodeElectedMaster()).isFalse();
        joinNodeAndRun(new JoinRequest(node2, newTerm, Optional.of(new Join(node2, node0, newTerm, initialTerm, initialVersion))));
        assertThat(isLocalNodeElectedMaster()).isTrue();
        assertThat(clusterStateHasNode(node1)).isTrue();
        assertThat(clusterStateHasNode(node2)).isTrue();
        FutureUtils.get(futNode0);
        FutureUtils.get(futNode1);
    }

    @Test
    public void testJoinFollowerWithHigherTerm() throws Exception {
        DiscoveryNode node0 = newNode(0, true);
        DiscoveryNode node1 = newNode(1, true);
        long initialTerm = randomLongBetween(1, 10);
        long initialVersion = randomLongBetween(1, 10);
        setupFakeMasterServiceAndCoordinator(initialTerm, initialState(node0, initialTerm, initialVersion,
            VotingConfiguration.of(node0)), () -> new StatusInfo(HEALTHY, "healthy-info"));
        long newTerm = initialTerm + randomLongBetween(1, 10);
        handleStartJoinFrom(node1, newTerm);
        handleFollowerCheckFrom(node1, newTerm);
        long newerTerm = newTerm + randomLongBetween(1, 10);
        joinNodeAndRun(new JoinRequest(node1, newerTerm,
            Optional.of(new Join(node1, node0, newerTerm, initialTerm, initialVersion))));
        assertThat(isLocalNodeElectedMaster()).isTrue();
    }

    @Test
    public void testJoinUpdateVotingConfigExclusion() throws Exception {
        DiscoveryNode initialNode = newNode(0, true);
        long initialTerm = randomLongBetween(1, 10);
        long initialVersion = randomLongBetween(1, 10);

        CoordinationMetadata.VotingConfigExclusion votingConfigExclusion = new CoordinationMetadata.VotingConfigExclusion(
                                                        CoordinationMetadata.VotingConfigExclusion.MISSING_VALUE_MARKER, "knownNodeName");

        setupFakeMasterServiceAndCoordinator(initialTerm, buildStateWithVotingConfigExclusion(initialNode, initialTerm,
            initialVersion, votingConfigExclusion),
            () -> new StatusInfo(HEALTHY, "healthy-info"));

        DiscoveryNode knownJoiningNode = new DiscoveryNode("knownNodeName", "newNodeId", buildNewFakeTransportAddress(),
                                                            emptyMap(), singleton(DiscoveryNodeRole.MASTER_ROLE), Version.CURRENT);
        long newTerm = initialTerm + randomLongBetween(1, 10);
        long newerTerm = newTerm + randomLongBetween(1, 10);

        joinNodeAndRun(new JoinRequest(knownJoiningNode, initialTerm,
            Optional.of(new Join(knownJoiningNode, initialNode, newerTerm, initialTerm, initialVersion))));

        assertThat(MasterServiceTests.discoveryState(masterService).getVotingConfigExclusions().stream().anyMatch(exclusion -> {
            return "knownNodeName".equals(exclusion.getNodeName()) && "newNodeId".equals(exclusion.getNodeId());
        })).isTrue();
    }

    private ClusterState buildStateWithVotingConfigExclusion(DiscoveryNode initialNode,
                                                             long initialTerm,
                                                             long initialVersion,
                                                             CoordinationMetadata.VotingConfigExclusion votingConfigExclusion) {
        ClusterState initialState = initialState(initialNode, initialTerm, initialVersion,
                                                    new VotingConfiguration(singleton(initialNode.getId())));
        Metadata newMetadata = Metadata.builder(initialState.metadata())
                                        .coordinationMetadata(CoordinationMetadata.builder(initialState.coordinationMetadata())
                                                                                    .addVotingConfigExclusion(votingConfigExclusion)
                                                                                    .build())
                                        .build();

        return ClusterState.builder(initialState).metadata(newMetadata).build();
    }

    private void handleStartJoinFrom(DiscoveryNode node, long term) throws Exception {
        final RequestHandlerRegistry<StartJoinRequest> startJoinHandler = transport.getRequestHandlers()
            .getHandler(JoinHelper.START_JOIN_ACTION_NAME);
        startJoinHandler.processMessageReceived(new StartJoinRequest(node, term), new TestTransportChannel(
            new ActionListener<TransportResponse>() {
                @Override
                public void onResponse(TransportResponse transportResponse) {
                }

                @Override
                public void onFailure(Exception e) {
                fail();
            }
        }));
        deterministicTaskQueue.runAllRunnableTasks();
        assertThat(isLocalNodeElectedMaster()).isFalse();
        assertThat(coordinator.getMode()).isEqualTo(Coordinator.Mode.CANDIDATE);
    }

    private void handleFollowerCheckFrom(DiscoveryNode node, long term) throws Exception {
        final RequestHandlerRegistry<FollowersChecker.FollowerCheckRequest> followerCheckHandler = transport.getRequestHandlers()
            .getHandler(FollowersChecker.FOLLOWER_CHECK_ACTION_NAME);
        final TestTransportChannel channel = new TestTransportChannel(new ActionListener<TransportResponse>() {
            @Override
            public void onResponse(TransportResponse transportResponse) {

            }

            @Override
            public void onFailure(Exception e) {
                fail();
            }
        });
        followerCheckHandler.processMessageReceived(new FollowersChecker.FollowerCheckRequest(term, node), channel);
        // Will throw exception if failed
        deterministicTaskQueue.runAllRunnableTasks();
        assertThat(isLocalNodeElectedMaster()).isFalse();
        assertThat(coordinator.getMode()).isEqualTo(Coordinator.Mode.FOLLOWER);
    }

    @Test
    public void testJoinFollowerFails() throws Exception {
        DiscoveryNode node0 = newNode(0, true);
        DiscoveryNode node1 = newNode(1, true);
        long initialTerm = randomLongBetween(1, 10);
        long initialVersion = randomLongBetween(1, 10);
        setupFakeMasterServiceAndCoordinator(initialTerm, initialState(node0, initialTerm, initialVersion,
            VotingConfiguration.of(node0)), () -> new StatusInfo(HEALTHY, "healthy-info"));
        long newTerm = initialTerm + randomLongBetween(1, 10);
        handleStartJoinFrom(node1, newTerm);
        handleFollowerCheckFrom(node1, newTerm);
        assertThatThrownBy(() -> joinNodeAndRun(new JoinRequest(node1, newTerm, Optional.empty())))
            .isExactlyInstanceOf(CoordinationStateRejectedException.class)
            .hasMessageContaining("join target is a follower");
        assertThat(isLocalNodeElectedMaster()).isFalse();
    }

    @Test
    public void testBecomeFollowerFailsPendingJoin() throws Exception {
        DiscoveryNode node0 = newNode(0, true);
        DiscoveryNode node1 = newNode(1, true);
        long initialTerm = randomLongBetween(1, 10);
        long initialVersion = randomLongBetween(1, 10);
        setupFakeMasterServiceAndCoordinator(initialTerm, initialState(node0, initialTerm, initialVersion,
            VotingConfiguration.of(node1)), () -> new StatusInfo(HEALTHY, "healthy-info"));
        long newTerm = initialTerm + randomLongBetween(1, 10);
        SimpleFuture fut = joinNodeAsync(new JoinRequest(node0, newTerm,
            Optional.of(new Join(node0, node0, newTerm, initialTerm, initialVersion))));
        deterministicTaskQueue.runAllRunnableTasks();
        assertThat(fut.isDone()).isFalse();
        assertThat(isLocalNodeElectedMaster()).isFalse();
        handleFollowerCheckFrom(node1, newTerm);
        assertThat(isLocalNodeElectedMaster()).isFalse();
        assertThatThrownBy(() -> FutureUtils.get(fut))
            .isExactlyInstanceOf(CoordinationStateRejectedException.class)
            .hasMessageContaining("became follower");
        assertThat(isLocalNodeElectedMaster()).isFalse();
    }

    @Test
    public void testConcurrentJoining() {
        List<DiscoveryNode> masterNodes = IntStream.rangeClosed(1, randomIntBetween(2, 5))
            .mapToObj(nodeId -> newNode(nodeId, true)).toList();
        List<DiscoveryNode> otherNodes = IntStream.rangeClosed(masterNodes.size() + 1, masterNodes.size() + 1 + randomIntBetween(0, 5))
            .mapToObj(nodeId -> newNode(nodeId, false)).toList();
        List<DiscoveryNode> allNodes = Stream.concat(masterNodes.stream(), otherNodes.stream()).toList();

        DiscoveryNode localNode = masterNodes.getFirst();
        VotingConfiguration votingConfiguration = new VotingConfiguration(randomValueOtherThan(singletonList(localNode),
            () -> randomSubsetOf(randomIntBetween(1, masterNodes.size()), masterNodes)).stream()
            .map(DiscoveryNode::getId).collect(Collectors.toSet()));

        logger.info("Voting configuration: {}", votingConfiguration);

        long initialTerm = randomLongBetween(1, 10);
        long initialVersion = randomLongBetween(1, 10);
        setupRealMasterServiceAndCoordinator(initialTerm, initialState(localNode, initialTerm, initialVersion, votingConfiguration));
        long newTerm = initialTerm + randomLongBetween(1, 10);

        // we need at least a quorum of voting nodes with a correct term and worse state
        List<DiscoveryNode> successfulNodes;
        do {
            successfulNodes = randomSubsetOf(allNodes);
        } while (votingConfiguration.hasQuorum(successfulNodes.stream().map(DiscoveryNode::getId).toList())
            == false);

        logger.info("Successful voting nodes: {}", successfulNodes);

        List<JoinRequest> correctJoinRequests = successfulNodes.stream().map(
            node -> new JoinRequest(node, newTerm, Optional.of(new Join(node, localNode, newTerm, initialTerm, initialVersion))))
            .toList();

        List<DiscoveryNode> possiblyUnsuccessfulNodes = new ArrayList<>(allNodes);
        possiblyUnsuccessfulNodes.removeAll(successfulNodes);

        logger.info("Possibly unsuccessful voting nodes: {}", possiblyUnsuccessfulNodes);

        List<JoinRequest> possiblyFailingJoinRequests = possiblyUnsuccessfulNodes.stream().map(node -> {
            if (randomBoolean()) {
                // a correct request
                return new JoinRequest(node, newTerm, Optional.of(new Join(node, localNode,
                    newTerm, initialTerm, initialVersion)));
            } else if (randomBoolean()) {
                // term too low
                return new JoinRequest(node, newTerm, Optional.of(new Join(node, localNode,
                    randomLongBetween(0, initialTerm), initialTerm, initialVersion)));
            } else {
                // better state
                return new JoinRequest(node, newTerm, Optional.of(new Join(node, localNode,
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
            }, "process " + joinRequest))).toList();

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

        assertThat(MasterServiceTests.discoveryState(masterService).nodes().isLocalNodeElectedMaster()).isTrue();
        for (DiscoveryNode successfulNode : successfulNodes) {
            assertThat(clusterStateHasNode(successfulNode)).as(successfulNode + " joined cluster").isTrue();
            assertThat(coordinator.missingJoinVoteFrom(successfulNode)).as(successfulNode + " voted for master").isFalse();
        }
    }

    private boolean isLocalNodeElectedMaster() {
        return MasterServiceTests.discoveryState(masterService).nodes().isLocalNodeElectedMaster();
    }

    private boolean clusterStateHasNode(DiscoveryNode node) {
        return node.equals(MasterServiceTests.discoveryState(masterService).nodes().get(node.getId()));
    }
}
