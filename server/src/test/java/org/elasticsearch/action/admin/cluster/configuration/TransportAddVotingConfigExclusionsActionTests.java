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
package org.elasticsearch.action.admin.cluster.configuration;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static org.elasticsearch.action.admin.cluster.configuration.TransportAddVotingConfigExclusionsAction.MAXIMUM_VOTING_CONFIG_EXCLUSIONS_SETTING;
import static org.elasticsearch.cluster.ClusterState.builder;
import static org.elasticsearch.test.ClusterServiceUtils.createClusterService;
import static org.elasticsearch.test.ClusterServiceUtils.setState;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.sameInstance;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.ClusterStateObserver.Listener;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.coordination.CoordinationMetadata;
import org.elasticsearch.cluster.coordination.CoordinationMetadata.VotingConfigExclusion;
import org.elasticsearch.cluster.coordination.CoordinationMetadata.VotingConfiguration;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes.Builder;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.MockTransport;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPool.Names;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import io.crate.common.unit.TimeValue;

public class TransportAddVotingConfigExclusionsActionTests extends ESTestCase {

    private static ThreadPool threadPool;
    private static ClusterService clusterService;
    private static DiscoveryNode localNode, otherNode1, otherNode2, otherDataNode;
    private static VotingConfigExclusion localNodeExclusion, otherNode1Exclusion, otherNode2Exclusion;

    private TransportService transportService;
    private ClusterStateObserver clusterStateObserver;

    @BeforeClass
    public static void createThreadPoolAndClusterService() {
        threadPool = new TestThreadPool("test", Settings.EMPTY);
        localNode = makeDiscoveryNode("local");
        localNodeExclusion = new VotingConfigExclusion(localNode);
        otherNode1 = makeDiscoveryNode("other1");
        otherNode1Exclusion = new VotingConfigExclusion(otherNode1);
        otherNode2 = makeDiscoveryNode("other2");
        otherNode2Exclusion = new VotingConfigExclusion(otherNode2);
        otherDataNode = new DiscoveryNode("data", "data", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);
        clusterService = createClusterService(threadPool, localNode);
    }

    private static DiscoveryNode makeDiscoveryNode(String name) {
        return new DiscoveryNode(
                name,
                name,
                buildNewFakeTransportAddress(),
                emptyMap(),
                singleton(DiscoveryNodeRole.MASTER_ROLE),
                Version.CURRENT);
    }

    @AfterClass
    public static void shutdownThreadPoolAndClusterService() {
        clusterService.stop();
        threadPool.shutdown();
    }

    @Before
    public void resetDeprecationLogger() {
        AddVotingConfigExclusionsRequest.DEPRECATION_LOGGER.resetLRU();
    }

    @Before
    public void setupForTest() {
        final MockTransport transport = new MockTransport();
        transportService = transport.createTransportService(
            Settings.EMPTY,
            threadPool,
            boundTransportAddress -> localNode,
            null
        );

        new TransportAddVotingConfigExclusionsAction(
            transportService, clusterService, threadPool); // registers action

        transportService.start();
        transportService.acceptIncomingRequests();

        final VotingConfiguration allNodesConfig = VotingConfiguration.of(localNode, otherNode1, otherNode2);

        setState(clusterService, builder(new ClusterName("cluster"))
            .nodes(new Builder().add(localNode).add(otherNode1).add(otherNode2).add(otherDataNode)
                .localNodeId(localNode.getId()).masterNodeId(localNode.getId()))
            .metadata(Metadata.builder()
                .coordinationMetadata(CoordinationMetadata.builder().lastAcceptedConfiguration(allNodesConfig)
                    .lastCommittedConfiguration(allNodesConfig).build())));

        clusterStateObserver = new ClusterStateObserver(clusterService, null, logger);
    }

    public void testWithdrawsVoteFromANode() throws InterruptedException {
        final CountDownLatch countDownLatch = new CountDownLatch(2);

        clusterStateObserver.waitForNextChange(new AdjustConfigurationForExclusions(countDownLatch));
        transportService.sendRequest(localNode, AddVotingConfigExclusionsAction.NAME, new AddVotingConfigExclusionsRequest("other1"),
            expectSuccess(r -> {
                assertNotNull(r);
                countDownLatch.countDown();
            })
        );

        assertTrue(countDownLatch.await(30, TimeUnit.SECONDS));
        assertThat(clusterService.getClusterApplierService().state().getVotingConfigExclusions(), contains(otherNode1Exclusion));
    }

    public void testWithdrawsVotesFromMultipleNodes() throws InterruptedException {
        final CountDownLatch countDownLatch = new CountDownLatch(2);

        clusterStateObserver.waitForNextChange(new AdjustConfigurationForExclusions(countDownLatch));
        transportService.sendRequest(localNode, AddVotingConfigExclusionsAction.NAME,
            new AddVotingConfigExclusionsRequest("other1", "other2"),
            expectSuccess(r -> {
                assertNotNull(r);
                countDownLatch.countDown();
            })
        );

        assertTrue(countDownLatch.await(30, TimeUnit.SECONDS));
        assertThat(clusterService.getClusterApplierService().state().getVotingConfigExclusions(),
                containsInAnyOrder(otherNode1Exclusion, otherNode2Exclusion));
    }

    public void testWithdrawsVotesFromNodesMatchingWildcard() throws InterruptedException {
        final CountDownLatch countDownLatch = new CountDownLatch(2);

        clusterStateObserver.waitForNextChange(new AdjustConfigurationForExclusions(countDownLatch));
        transportService.sendRequest(localNode, AddVotingConfigExclusionsAction.NAME, makeRequestWithNodeDescriptions("other*"),
            expectSuccess(r -> {
                assertNotNull(r);
                countDownLatch.countDown();
            })
        );

        assertTrue(countDownLatch.await(30, TimeUnit.SECONDS));
        assertThat(clusterService.getClusterApplierService().state().getVotingConfigExclusions(),
                containsInAnyOrder(otherNode1Exclusion, otherNode2Exclusion));
        assertWarnings(AddVotingConfigExclusionsRequest.DEPRECATION_MESSAGE);
    }

    public void testWithdrawsVotesFromAllMasterEligibleNodes() throws InterruptedException {
        final CountDownLatch countDownLatch = new CountDownLatch(2);

        clusterStateObserver.waitForNextChange(new AdjustConfigurationForExclusions(countDownLatch));
        transportService.sendRequest(localNode, AddVotingConfigExclusionsAction.NAME, makeRequestWithNodeDescriptions("_all"),
            expectSuccess(r -> {
                assertNotNull(r);
                countDownLatch.countDown();
            })
        );

        assertTrue(countDownLatch.await(30, TimeUnit.SECONDS));
        assertThat(clusterService.getClusterApplierService().state().getVotingConfigExclusions(),
                containsInAnyOrder(localNodeExclusion, otherNode1Exclusion, otherNode2Exclusion));
        assertWarnings(AddVotingConfigExclusionsRequest.DEPRECATION_MESSAGE);
    }

    public void testWithdrawsVoteFromLocalNode() throws InterruptedException {
        final CountDownLatch countDownLatch = new CountDownLatch(2);

        clusterStateObserver.waitForNextChange(new AdjustConfigurationForExclusions(countDownLatch));
        transportService.sendRequest(localNode, AddVotingConfigExclusionsAction.NAME, makeRequestWithNodeDescriptions("_local"),
            expectSuccess(r -> {
                assertNotNull(r);
                countDownLatch.countDown();
            })
        );

        assertTrue(countDownLatch.await(30, TimeUnit.SECONDS));
        assertThat(clusterService.getClusterApplierService().state().getVotingConfigExclusions(),
                contains(localNodeExclusion));
        assertWarnings(AddVotingConfigExclusionsRequest.DEPRECATION_MESSAGE);
    }

    public void testReturnsImmediatelyIfVoteAlreadyWithdrawn() throws InterruptedException {
        final ClusterState state = clusterService.state();
        setState(clusterService, builder(state)
            .metadata(Metadata.builder(state.metadata())
                .coordinationMetadata(CoordinationMetadata.builder(state.coordinationMetadata())
                    .lastCommittedConfiguration(VotingConfiguration.of(localNode, otherNode2))
                    .lastAcceptedConfiguration(VotingConfiguration.of(localNode, otherNode2))
                .build())));

        final CountDownLatch countDownLatch = new CountDownLatch(1);

        // no observer to reconfigure
        transportService.sendRequest(localNode, AddVotingConfigExclusionsAction.NAME, new AddVotingConfigExclusionsRequest("other1"),
            expectSuccess(r -> {
                assertNotNull(r);
                countDownLatch.countDown();
            })
        );

        assertTrue(countDownLatch.await(30, TimeUnit.SECONDS));
        assertThat(clusterService.getClusterApplierService().state().getVotingConfigExclusions(),
                contains(otherNode1Exclusion));
    }

    public void testReturnsErrorIfNoMatchingNodeDescriptions() throws InterruptedException {
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        final SetOnce<TransportException> exceptionHolder = new SetOnce<>();

        transportService.sendRequest(localNode, AddVotingConfigExclusionsAction.NAME, makeRequestWithNodeDescriptions("not-a-node"),
            expectError(e -> {
                exceptionHolder.set(e);
                countDownLatch.countDown();
            })
        );

        assertTrue(countDownLatch.await(30, TimeUnit.SECONDS));
        final Throwable rootCause = exceptionHolder.get().getRootCause();
        assertThat(rootCause, instanceOf(IllegalArgumentException.class));
        assertThat(rootCause.getMessage(),
            equalTo("add voting config exclusions request for [not-a-node] matched no master-eligible nodes"));
        assertWarnings(AddVotingConfigExclusionsRequest.DEPRECATION_MESSAGE);
    }

    public void testOnlyMatchesMasterEligibleNodes() throws InterruptedException {
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        final SetOnce<TransportException> exceptionHolder = new SetOnce<>();

        transportService.sendRequest(localNode, AddVotingConfigExclusionsAction.NAME,
            makeRequestWithNodeDescriptions("_all", "master:false"),
            expectError(e -> {
                exceptionHolder.set(e);
                countDownLatch.countDown();
            })
        );

        assertTrue(countDownLatch.await(30, TimeUnit.SECONDS));
        final Throwable rootCause = exceptionHolder.get().getRootCause();
        assertThat(rootCause, instanceOf(IllegalArgumentException.class));
        assertThat(rootCause.getMessage(),
            equalTo("add voting config exclusions request for [_all, master:false] matched no master-eligible nodes"));
        assertWarnings(AddVotingConfigExclusionsRequest.DEPRECATION_MESSAGE);
    }

    public void testExcludeAbsentNodesByNodeIds() throws InterruptedException {
        final CountDownLatch countDownLatch = new CountDownLatch(2);

        clusterStateObserver.waitForNextChange(new AdjustConfigurationForExclusions(countDownLatch));
        transportService.sendRequest(localNode, AddVotingConfigExclusionsAction.NAME,
            new AddVotingConfigExclusionsRequest(Strings.EMPTY_ARRAY, new String[]{"absent_id"},
                                                    Strings.EMPTY_ARRAY, TimeValue.timeValueSeconds(30)),
            expectSuccess(e -> {
                countDownLatch.countDown();
            })
        );

        assertTrue(countDownLatch.await(30, TimeUnit.SECONDS));
        assertEquals(singleton(new VotingConfigExclusion("absent_id", VotingConfigExclusion.MISSING_VALUE_MARKER)),
            clusterService.getClusterApplierService().state().getVotingConfigExclusions());
    }

    public void testExcludeExistingNodesByNodeIds() throws InterruptedException {
        final CountDownLatch countDownLatch = new CountDownLatch(2);

        clusterStateObserver.waitForNextChange(new AdjustConfigurationForExclusions(countDownLatch));
        transportService.sendRequest(localNode, AddVotingConfigExclusionsAction.NAME,
            new AddVotingConfigExclusionsRequest(Strings.EMPTY_ARRAY, new String[]{"other1", "other2"},
                                                    Strings.EMPTY_ARRAY, TimeValue.timeValueSeconds(30)),
            expectSuccess(r -> {
                assertNotNull(r);
                countDownLatch.countDown();
            })
        );

        assertTrue(countDownLatch.await(30, TimeUnit.SECONDS));
        assertThat(clusterService.getClusterApplierService().state().getVotingConfigExclusions(),
            containsInAnyOrder(otherNode1Exclusion, otherNode2Exclusion));
    }

    public void testExcludeAbsentNodesByNodeNames() throws InterruptedException {
        final CountDownLatch countDownLatch = new CountDownLatch(2);

        clusterStateObserver.waitForNextChange(new AdjustConfigurationForExclusions(countDownLatch));
        transportService.sendRequest(localNode, AddVotingConfigExclusionsAction.NAME, new AddVotingConfigExclusionsRequest("absent_node"),
            expectSuccess(e -> {
                countDownLatch.countDown();
            })
        );

        assertTrue(countDownLatch.await(30, TimeUnit.SECONDS));
        assertEquals(singleton(new VotingConfigExclusion(VotingConfigExclusion.MISSING_VALUE_MARKER, "absent_node")),
                    clusterService.getClusterApplierService().state().getVotingConfigExclusions());
    }

    public void testExcludeExistingNodesByNodeNames() throws InterruptedException {
        final CountDownLatch countDownLatch = new CountDownLatch(2);

        clusterStateObserver.waitForNextChange(new AdjustConfigurationForExclusions(countDownLatch));
        transportService.sendRequest(localNode, AddVotingConfigExclusionsAction.NAME,
            new AddVotingConfigExclusionsRequest("other1", "other2"),
            expectSuccess(r -> {
                assertNotNull(r);
                countDownLatch.countDown();
            })
        );

        assertTrue(countDownLatch.await(30, TimeUnit.SECONDS));
        assertThat(clusterService.getClusterApplierService().state().getVotingConfigExclusions(),
            containsInAnyOrder(otherNode1Exclusion, otherNode2Exclusion));
    }

    public void testSucceedsEvenIfAllExclusionsAlreadyAdded() throws InterruptedException {
        final ClusterState state = clusterService.state();
        final ClusterState.Builder builder = builder(state);
        builder.metadata(Metadata.builder(state.metadata()).
                coordinationMetadata(
                        CoordinationMetadata.builder(state.coordinationMetadata())
                                .addVotingConfigExclusion(otherNode1Exclusion).
                build()));
        setState(clusterService, builder);

        final CountDownLatch countDownLatch = new CountDownLatch(1);

        transportService.sendRequest(localNode, AddVotingConfigExclusionsAction.NAME, new AddVotingConfigExclusionsRequest("other1"),
            expectSuccess(r -> {
                assertNotNull(r);
                countDownLatch.countDown();
            })
        );

        assertTrue(countDownLatch.await(30, TimeUnit.SECONDS));
        assertThat(clusterService.getClusterApplierService().state().getVotingConfigExclusions(),
                contains(otherNode1Exclusion));
    }

    public void testExcludeByNodeIdSucceedsEvenIfAllExclusionsAlreadyAdded() throws InterruptedException {
        final ClusterState state = clusterService.state();
        final ClusterState.Builder builder = builder(state);
        builder.metadata(Metadata.builder(state.metadata()).
            coordinationMetadata(
                CoordinationMetadata.builder(state.coordinationMetadata())
                    .addVotingConfigExclusion(otherNode1Exclusion).
                    build()));
        setState(clusterService, builder);

        final CountDownLatch countDownLatch = new CountDownLatch(1);

        transportService.sendRequest(localNode, AddVotingConfigExclusionsAction.NAME,
            new AddVotingConfigExclusionsRequest(Strings.EMPTY_ARRAY, new String[]{"other1"},
                                                    Strings.EMPTY_ARRAY, TimeValue.timeValueSeconds(30)),
            expectSuccess(r -> {
                assertNotNull(r);
                countDownLatch.countDown();
            })
        );

        assertTrue(countDownLatch.await(30, TimeUnit.SECONDS));
        assertThat(clusterService.getClusterApplierService().state().getVotingConfigExclusions(),
            contains(otherNode1Exclusion));
    }

    public void testExcludeByNodeNameSucceedsEvenIfAllExclusionsAlreadyAdded() throws InterruptedException {
        final ClusterState state = clusterService.state();
        final ClusterState.Builder builder = builder(state);
        builder.metadata(Metadata.builder(state.metadata()).
            coordinationMetadata(
                CoordinationMetadata.builder(state.coordinationMetadata())
                    .addVotingConfigExclusion(otherNode1Exclusion).
                    build()));
        setState(clusterService, builder);

        final CountDownLatch countDownLatch = new CountDownLatch(1);

        transportService.sendRequest(localNode, AddVotingConfigExclusionsAction.NAME, new AddVotingConfigExclusionsRequest("other1"),
            expectSuccess(r -> {
                assertNotNull(r);
                countDownLatch.countDown();
            })
        );

        assertTrue(countDownLatch.await(30, TimeUnit.SECONDS));
        assertThat(clusterService.getClusterApplierService().state().getVotingConfigExclusions(),
            contains(otherNode1Exclusion));
    }

    public void testReturnsErrorIfMaximumExclusionCountExceeded() throws InterruptedException {
        final Metadata.Builder metadataBuilder = Metadata.builder(clusterService.state().metadata()).persistentSettings(
                Settings.builder().put(clusterService.state().metadata().persistentSettings())
                        .put(MAXIMUM_VOTING_CONFIG_EXCLUSIONS_SETTING.getKey(), 2).build());
        CoordinationMetadata.Builder coordinationMetadataBuilder =
                CoordinationMetadata.builder(clusterService.state().coordinationMetadata())
                        .addVotingConfigExclusion(localNodeExclusion);

        final int existingCount, newCount;
        if (randomBoolean()) {
            coordinationMetadataBuilder.addVotingConfigExclusion(otherNode1Exclusion);
            existingCount = 2;
            newCount = 1;
        } else {
            existingCount = 1;
            newCount = 2;
        }

        metadataBuilder.coordinationMetadata(coordinationMetadataBuilder.build());

        final ClusterState.Builder builder = builder(clusterService.state()).metadata(metadataBuilder);
        setState(clusterService, builder);

        final CountDownLatch countDownLatch = new CountDownLatch(1);
        final SetOnce<TransportException> exceptionHolder = new SetOnce<>();

        transportService.sendRequest(localNode, AddVotingConfigExclusionsAction.NAME, makeRequestWithNodeDescriptions("other*"),
            expectError(e -> {
                exceptionHolder.set(e);
                countDownLatch.countDown();
            })
        );

        assertTrue(countDownLatch.await(30, TimeUnit.SECONDS));
        final Throwable rootCause = exceptionHolder.get().getRootCause();
        assertThat(rootCause, instanceOf(IllegalArgumentException.class));
        assertThat(rootCause.getMessage(), equalTo("add voting config exclusions request for [other*] would add [" + newCount +
            "] exclusions to the existing [" + existingCount +
            "] which would exceed the maximum of [2] set by [cluster.max_voting_config_exclusions]"));
        assertWarnings(AddVotingConfigExclusionsRequest.DEPRECATION_MESSAGE);
    }

    public void testTimesOut() throws InterruptedException {
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        final SetOnce<TransportException> exceptionHolder = new SetOnce<>();

        transportService.sendRequest(localNode, AddVotingConfigExclusionsAction.NAME,
            new AddVotingConfigExclusionsRequest(new String[]{"other1"}, Strings.EMPTY_ARRAY, Strings.EMPTY_ARRAY,
                TimeValue.timeValueMillis(100)),
            expectError(e -> {
                exceptionHolder.set(e);
                countDownLatch.countDown();
            })
        );

        assertTrue(countDownLatch.await(30, TimeUnit.SECONDS));
        final Throwable rootCause = exceptionHolder.get().getRootCause();
        assertThat(rootCause,instanceOf(ElasticsearchTimeoutException.class));
        assertThat(rootCause.getMessage(), startsWith("timed out waiting for voting config exclusions [{other1}"));
        assertWarnings(AddVotingConfigExclusionsRequest.DEPRECATION_MESSAGE);

    }

    private TransportResponseHandler<AddVotingConfigExclusionsResponse> expectSuccess(
        Consumer<AddVotingConfigExclusionsResponse> onResponse) {
        return responseHandler(onResponse, e -> {
            throw new AssertionError("unexpected", e);
        });
    }

    private TransportResponseHandler<AddVotingConfigExclusionsResponse> expectError(Consumer<TransportException> onException) {
        return responseHandler(r -> {
            assert false : r;
        }, onException);
    }

    private TransportResponseHandler<AddVotingConfigExclusionsResponse> responseHandler(
        Consumer<AddVotingConfigExclusionsResponse> onResponse, Consumer<TransportException> onException) {
        return new TransportResponseHandler<AddVotingConfigExclusionsResponse>() {
            @Override
            public void handleResponse(AddVotingConfigExclusionsResponse response) {
                onResponse.accept(response);
            }

            @Override
            public void handleException(TransportException exp) {
                onException.accept(exp);
            }

            @Override
            public String executor() {
                return Names.SAME;
            }

            @Override
            public AddVotingConfigExclusionsResponse read(StreamInput in) throws IOException {
                return new AddVotingConfigExclusionsResponse(in);
            }
        };
    }

    private static class AdjustConfigurationForExclusions implements Listener {

        final CountDownLatch doneLatch;

        AdjustConfigurationForExclusions(CountDownLatch latch) {
            this.doneLatch = latch;
        }

        @Override
        public void onNewClusterState(ClusterState state) {
            clusterService.getMasterService().submitStateUpdateTask("reconfiguration", new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    assertThat(currentState, sameInstance(state));
                    final Set<String> votingNodeIds = new HashSet<>();
                    currentState.nodes().forEach(n -> votingNodeIds.add(n.getId()));
                    currentState.getVotingConfigExclusions().forEach(t -> votingNodeIds.remove(t.getNodeId()));
                    final VotingConfiguration votingConfiguration = new VotingConfiguration(votingNodeIds);
                    return builder(currentState)
                        .metadata(Metadata.builder(currentState.metadata())
                            .coordinationMetadata(CoordinationMetadata.builder(currentState.coordinationMetadata())
                                .lastAcceptedConfiguration(votingConfiguration)
                                .lastCommittedConfiguration(votingConfiguration)
                                .build()))
                           .build();
                }

                @Override
                public void onFailure(String source, Exception e) {
                    throw new AssertionError("unexpected failure", e);
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    doneLatch.countDown();
                }
            });
        }

        @Override
        public void onClusterServiceClose() {
            throw new AssertionError("unexpected close");
        }

        @Override
        public void onTimeout(TimeValue timeout) {
            throw new AssertionError("unexpected timeout");
        }
    }

    private AddVotingConfigExclusionsRequest makeRequestWithNodeDescriptions(String... nodeDescriptions) {
        return new AddVotingConfigExclusionsRequest(nodeDescriptions, Strings.EMPTY_ARRAY,
                                                    Strings.EMPTY_ARRAY, TimeValue.timeValueSeconds(30));
    }

}
