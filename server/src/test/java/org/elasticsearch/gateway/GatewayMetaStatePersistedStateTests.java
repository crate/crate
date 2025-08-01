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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOError;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.store.MockDirectoryWrapper;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.coordination.CoordinationMetadata;
import org.elasticsearch.cluster.coordination.CoordinationMetadata.VotingConfigExclusion;
import org.elasticsearch.cluster.coordination.CoordinationState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Manifest;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.MockPageCacheRecycler;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.node.Node;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.junit.Test;

import io.crate.common.exceptions.Exceptions;
import io.crate.common.io.IOUtils;

public class GatewayMetaStatePersistedStateTests extends ESTestCase {
    private NodeEnvironment nodeEnvironment;
    private ClusterName clusterName;
    private Settings settings;
    private DiscoveryNode localNode;
    private BigArrays bigArrays;

    @Override
    public void setUp() throws Exception {
        bigArrays = new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());
        nodeEnvironment = newNodeEnvironment();
        localNode = new DiscoveryNode("node1", buildNewFakeTransportAddress(), Collections.emptyMap(),
                                      Set.of(DiscoveryNodeRole.MASTER_ROLE), Version.CURRENT);
        clusterName = new ClusterName(randomAlphaOfLength(10));
        settings = Settings.builder().put(ClusterName.CLUSTER_NAME_SETTING.getKey(), clusterName.value()).build();
        super.setUp();
    }

    @Override
    public void tearDown() throws Exception {
        nodeEnvironment.close();
        super.tearDown();
    }

    private CoordinationState.PersistedState newGatewayPersistedState() {
        final MockGatewayMetaState gateway = new MockGatewayMetaState(localNode, bigArrays);
        gateway.start(settings, nodeEnvironment, xContentRegistry(), writableRegistry());
        final CoordinationState.PersistedState persistedState = gateway.getPersistedState();
        assertThat(persistedState).isExactlyInstanceOf(GatewayMetaState.LucenePersistedState.class);
        return persistedState;
    }

    private CoordinationState.PersistedState maybeNew(CoordinationState.PersistedState persistedState) throws IOException {
        if (randomBoolean()) {
            persistedState.close();
            return newGatewayPersistedState();
        }
        return persistedState;
    }

    @Test
    public void testInitialState() throws IOException {
        CoordinationState.PersistedState gateway = null;
        try {
            gateway = newGatewayPersistedState();
            ClusterState state = gateway.getLastAcceptedState();
            assertThat(state.getClusterName()).isEqualTo(clusterName);
            assertThat(Metadata.isGlobalStateEquals(state.metadata(), Metadata.EMPTY_METADATA)).isTrue();
            assertThat(state.version()).isEqualTo(Manifest.empty().getClusterStateVersion());
            assertThat(state.nodes().getLocalNode()).isEqualTo(localNode);

            long currentTerm = gateway.getCurrentTerm();
            assertThat(currentTerm).isEqualTo(Manifest.empty().getCurrentTerm());
        } finally {
            IOUtils.close(gateway);
        }
    }

    public void testSetCurrentTerm() throws IOException {
        CoordinationState.PersistedState gateway = null;
        try {
            gateway = newGatewayPersistedState();

            for (int i = 0; i < randomIntBetween(1, 5); i++) {
                final long currentTerm = randomNonNegativeLong();
                gateway.setCurrentTerm(currentTerm);
                gateway = maybeNew(gateway);
                assertThat(gateway.getCurrentTerm()).isEqualTo(currentTerm);
            }
        } finally {
            IOUtils.close(gateway);
        }
    }

    private ClusterState createClusterState(long version, Metadata metadata) {
        return ClusterState.builder(clusterName).
                nodes(DiscoveryNodes.builder().add(localNode).localNodeId(localNode.getId()).build()).
                version(version).
                metadata(metadata).
                build();
    }

    private CoordinationMetadata createCoordinationMetadata(long term) {
        CoordinationMetadata.Builder builder = CoordinationMetadata.builder();
        builder.term(term);
        builder.lastAcceptedConfiguration(
                new CoordinationMetadata.VotingConfiguration(
                        Set.of(generateRandomStringArray(10, 10, false))));
        builder.lastCommittedConfiguration(
                new CoordinationMetadata.VotingConfiguration(
                        Set.of(generateRandomStringArray(10, 10, false))));
        for (int i = 0; i < randomIntBetween(0, 5); i++) {
            builder.addVotingConfigExclusion(new VotingConfigExclusion(randomAlphaOfLength(10), randomAlphaOfLength(10)));
        }

        return builder.build();
    }

    private IndexMetadata createIndexMetadata(String indexName, int numberOfShards, long version) {
        return IndexMetadata.builder(indexName).settings(
                Settings.builder()
                        .put(IndexMetadata.SETTING_INDEX_UUID, indexName)
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numberOfShards)
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                        .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                        .build()
        ).version(version).build();
    }

    private void assertClusterStateEqual(ClusterState expected, ClusterState actual) {
        assertThat(actual.version()).isEqualTo(expected.version());
        assertThat(Metadata.isGlobalStateEquals(actual.metadata(), expected.metadata())).isTrue();
        for (IndexMetadata indexMetadata : expected.metadata()) {
            assertThat(actual.metadata().index(indexMetadata.getIndex())).isEqualTo(indexMetadata);
        }
    }

    public void testSetLastAcceptedState() throws IOException {
        CoordinationState.PersistedState gateway = null;
        try {
            gateway = newGatewayPersistedState();
            final long term = randomNonNegativeLong();

            for (int i = 0; i < randomIntBetween(1, 5); i++) {
                final long version = randomNonNegativeLong();
                final String indexName = randomAlphaOfLength(10);
                final IndexMetadata indexMetadata = createIndexMetadata(indexName, randomIntBetween(1, 5), randomNonNegativeLong());
                final Metadata metadata = Metadata.builder().
                    persistentSettings(Settings.builder().put(randomAlphaOfLength(10), randomAlphaOfLength(10)).build()).
                    coordinationMetadata(createCoordinationMetadata(term)).
                    put(indexMetadata, false).
                    build();
                ClusterState state = createClusterState(version, metadata);

                gateway.setLastAcceptedState(state);
                gateway = maybeNew(gateway);

                ClusterState lastAcceptedState = gateway.getLastAcceptedState();
                assertClusterStateEqual(state, lastAcceptedState);
            }
        } finally {
            IOUtils.close(gateway);
        }
    }

    @Test
    public void testSetLastAcceptedStateTermChanged() throws IOException {
        CoordinationState.PersistedState gateway = null;
        try {
            gateway = newGatewayPersistedState();

            final String indexName = randomAlphaOfLength(10);
            final int numberOfShards = randomIntBetween(1, 5);
            final long version = randomNonNegativeLong();
            final long term = randomValueOtherThan(Long.MAX_VALUE, ESTestCase::randomNonNegativeLong);
            final IndexMetadata indexMetadata = createIndexMetadata(indexName, numberOfShards, version);
            final ClusterState state = createClusterState(randomNonNegativeLong(),
                                                          Metadata.builder().coordinationMetadata(createCoordinationMetadata(term)).put(indexMetadata, false).build());
            gateway.setLastAcceptedState(state);

            gateway = maybeNew(gateway);
            final long newTerm = randomLongBetween(term + 1, Long.MAX_VALUE);
            final int newNumberOfShards = randomValueOtherThan(numberOfShards, () -> randomIntBetween(1, 5));
            final IndexMetadata newIndexMetadata = createIndexMetadata(indexName, newNumberOfShards, version);
            final ClusterState newClusterState = createClusterState(randomNonNegativeLong(),
                                                                    Metadata.builder().coordinationMetadata(createCoordinationMetadata(newTerm)).put(newIndexMetadata, false).build());
            gateway.setLastAcceptedState(newClusterState);

            gateway = maybeNew(gateway);
            assertThat(gateway.getLastAcceptedState().metadata().index(indexName)).isEqualTo(newIndexMetadata);
        } finally {
            IOUtils.close(gateway);
        }
    }

    public void testCurrentTermAndTermAreDifferent() throws IOException {
        CoordinationState.PersistedState gateway = null;
        try {
            gateway = newGatewayPersistedState();

            long currentTerm = randomNonNegativeLong();
            long term = randomValueOtherThan(currentTerm, ESTestCase::randomNonNegativeLong);

            gateway.setCurrentTerm(currentTerm);
            gateway.setLastAcceptedState(createClusterState(randomNonNegativeLong(),
                                                            Metadata.builder().coordinationMetadata(CoordinationMetadata.builder().term(term).build()).build()));

            gateway = maybeNew(gateway);
            assertThat(gateway.getCurrentTerm()).isEqualTo(currentTerm);
            assertThat(gateway.getLastAcceptedState().coordinationMetadata().term()).isEqualTo(term);
        } finally {
            IOUtils.close(gateway);
        }
    }

    @Test
    public void testMarkAcceptedConfigAsCommitted() throws IOException {
        CoordinationState.PersistedState gateway = null;
        try {
            gateway = newGatewayPersistedState();

            //generate random coordinationMetadata with different lastAcceptedConfiguration and lastCommittedConfiguration
            CoordinationMetadata coordinationMetadata;
            do {
                coordinationMetadata = createCoordinationMetadata(randomNonNegativeLong());
            } while (coordinationMetadata.getLastAcceptedConfiguration().equals(coordinationMetadata.getLastCommittedConfiguration()));

            ClusterState state = createClusterState(randomNonNegativeLong(),
                                                    Metadata.builder().coordinationMetadata(coordinationMetadata)
                                                        .clusterUUID(randomAlphaOfLength(10)).build());
            gateway.setLastAcceptedState(state);

            gateway = maybeNew(gateway);
            assertThat(gateway.getLastAcceptedState().getLastAcceptedConfiguration()).isNotEqualTo(gateway.getLastAcceptedState().getLastCommittedConfiguration());
            gateway.markLastAcceptedStateAsCommitted();

            CoordinationMetadata expectedCoordinationMetadata = CoordinationMetadata.builder(coordinationMetadata)
                .lastCommittedConfiguration(coordinationMetadata.getLastAcceptedConfiguration()).build();
            ClusterState expectedClusterState =
                ClusterState.builder(state).metadata(Metadata.builder().coordinationMetadata(expectedCoordinationMetadata)
                                                         .clusterUUID(state.metadata().clusterUUID()).clusterUUIDCommitted(true).build()).build();

            gateway = maybeNew(gateway);
            assertClusterStateEqual(expectedClusterState, gateway.getLastAcceptedState());
            gateway.markLastAcceptedStateAsCommitted();

            gateway = maybeNew(gateway);
            assertClusterStateEqual(expectedClusterState, gateway.getLastAcceptedState());
        } finally {
            IOUtils.close(gateway);
        }
    }

    @Test
    public void testStatePersistedOnLoad() throws IOException {
        // open LucenePersistedState to make sure that cluster state is written out to each data path
        final PersistedClusterStateService persistedClusterStateService =
            new PersistedClusterStateService(nodeEnvironment, xContentRegistry(), writableRegistry(), getBigArrays(),
                new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS), () -> 0L);
        final ClusterState state = createClusterState(randomNonNegativeLong(),
                                                      Metadata.builder().clusterUUID(randomAlphaOfLength(10)).build());
        try (GatewayMetaState.LucenePersistedState ignored = new GatewayMetaState.LucenePersistedState(
            persistedClusterStateService, 42L, state)) {

        }

        nodeEnvironment.close();

        // verify that the freshest state was rewritten to each data path
        Path[] paths = nodeEnvironment.nodeDataPaths();
        for (Path path : paths) {
            Settings settings = Settings.builder()
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toAbsolutePath())
                .put(Environment.PATH_DATA_SETTING.getKey(), path.getParent().getParent().toString()).build();
            try (NodeEnvironment nodeEnvironment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings))) {
                final PersistedClusterStateService newPersistedClusterStateService =
                    new PersistedClusterStateService(nodeEnvironment, xContentRegistry(), writableRegistry(), getBigArrays(),
                        new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS), () -> 0L);
                final PersistedClusterStateService.OnDiskState onDiskState = newPersistedClusterStateService.loadBestOnDiskState();
                assertThat(onDiskState.empty()).as("Path should not be empty: " + path.toAbsolutePath()).isFalse();
                assertThat(onDiskState.currentTerm).isEqualTo(42L);
                assertClusterStateEqual(state,
                                        ClusterState.builder(ClusterName.DEFAULT)
                                            .version(onDiskState.lastAcceptedVersion)
                                            .metadata(onDiskState.metadata).build());
            }
        }
    }

    @Test
    public void testDataOnlyNodePersistence() throws Exception {
        DiscoveryNode localNode = new DiscoveryNode("node1", buildNewFakeTransportAddress(), Collections.emptyMap(),
                                                    Set.of(DiscoveryNodeRole.DATA_ROLE), Version.CURRENT);
        Settings settings = Settings.builder().put(ClusterName.CLUSTER_NAME_SETTING.getKey(), clusterName.value()).put(
            Node.NODE_MASTER_SETTING.getKey(), false).put(Node.NODE_NAME_SETTING.getKey(), "test").build();
        final MockGatewayMetaState gateway = new MockGatewayMetaState(localNode, getBigArrays());
        final TransportService transportService = mock(TransportService.class);
        TestThreadPool threadPool = new TestThreadPool("testMarkAcceptedConfigAsCommittedOnDataOnlyNode");
        when(transportService.getThreadPool()).thenReturn(threadPool);
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.getClusterSettings()).thenReturn(
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS));
        final PersistedClusterStateService persistedClusterStateService =
            new PersistedClusterStateService(nodeEnvironment, xContentRegistry(), writableRegistry(), BigArrays.NON_RECYCLING_INSTANCE,
                new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS), () -> 0L);
        gateway.start(
            settings,
            transportService,
            clusterService,
            new MetaStateService(nodeEnvironment, writableRegistry(), xContentRegistry()),
            null,
            persistedClusterStateService
        );
        final CoordinationState.PersistedState persistedState = gateway.getPersistedState();
        assertThat(persistedState).isExactlyInstanceOf(GatewayMetaState.AsyncLucenePersistedState.class);

        //generate random coordinationMetadata with different lastAcceptedConfiguration and lastCommittedConfiguration
        CoordinationMetadata coordinationMetadata;
        do {
            coordinationMetadata = createCoordinationMetadata(randomNonNegativeLong());
        } while (coordinationMetadata.getLastAcceptedConfiguration().equals(coordinationMetadata.getLastCommittedConfiguration()));

        ClusterState state = createClusterState(randomNonNegativeLong(),
                                                Metadata.builder().coordinationMetadata(coordinationMetadata)
                                                    .clusterUUID(randomAlphaOfLength(10)).build());
        persistedState.setLastAcceptedState(state);
        assertBusy(() -> assertThat(gateway.allPendingAsyncStatesWritten()).isTrue());

        assertThat(persistedState.getLastAcceptedState().getLastAcceptedConfiguration()).isNotEqualTo(persistedState.getLastAcceptedState().getLastCommittedConfiguration());
        CoordinationMetadata persistedCoordinationMetadata =
            persistedClusterStateService.loadBestOnDiskState().metadata.coordinationMetadata();
        assertThat(persistedCoordinationMetadata.getLastAcceptedConfiguration()).isEqualTo(GatewayMetaState.AsyncLucenePersistedState.STALE_STATE_CONFIG);
        assertThat(persistedCoordinationMetadata.getLastCommittedConfiguration()).isEqualTo(GatewayMetaState.AsyncLucenePersistedState.STALE_STATE_CONFIG);

        persistedState.markLastAcceptedStateAsCommitted();
        assertBusy(() -> assertThat(gateway.allPendingAsyncStatesWritten()).isTrue());

        CoordinationMetadata expectedCoordinationMetadata = CoordinationMetadata.builder(coordinationMetadata)
            .lastCommittedConfiguration(coordinationMetadata.getLastAcceptedConfiguration()).build();
        ClusterState expectedClusterState =
            ClusterState.builder(state).metadata(Metadata.builder().coordinationMetadata(expectedCoordinationMetadata)
                                                     .clusterUUID(state.metadata().clusterUUID()).clusterUUIDCommitted(true).build()).build();

        assertClusterStateEqual(expectedClusterState, persistedState.getLastAcceptedState());
        persistedCoordinationMetadata = persistedClusterStateService.loadBestOnDiskState().metadata.coordinationMetadata();
        assertThat(persistedCoordinationMetadata.getLastAcceptedConfiguration()).isEqualTo(GatewayMetaState.AsyncLucenePersistedState.STALE_STATE_CONFIG);
        assertThat(persistedCoordinationMetadata.getLastCommittedConfiguration()).isEqualTo(GatewayMetaState.AsyncLucenePersistedState.STALE_STATE_CONFIG);
        assertThat(persistedClusterStateService.loadBestOnDiskState().metadata.clusterUUIDCommitted()).isTrue();

        // generate a series of updates and check if batching works
        final String indexName = randomAlphaOfLength(10);
        long currentTerm = state.term();
        for (int i = 0; i < 1000; i++) {
            if (rarely()) {
                // bump term
                currentTerm = currentTerm + (rarely() ? randomIntBetween(1, 5) : 0L);
                persistedState.setCurrentTerm(currentTerm);
            } else {
                // update cluster state
                final int numberOfShards = randomIntBetween(1, 5);
                final long term = Math.min(state.term() + (rarely() ? randomIntBetween(1, 5) : 0L), currentTerm);
                final IndexMetadata indexMetadata = createIndexMetadata(indexName, numberOfShards, i);
                state = createClusterState(state.version() + 1,
                                           Metadata.builder().coordinationMetadata(createCoordinationMetadata(term)).put(indexMetadata, false).build());
                persistedState.setLastAcceptedState(state);
            }
        }
        assertThat(persistedState.getCurrentTerm()).isEqualTo(currentTerm);
        assertClusterStateEqual(state, persistedState.getLastAcceptedState());
        assertBusy(() -> assertThat(gateway.allPendingAsyncStatesWritten()).isTrue());

        gateway.close();

        try (CoordinationState.PersistedState reloadedPersistedState = newGatewayPersistedState()) {
            assertThat(reloadedPersistedState.getCurrentTerm()).isEqualTo(currentTerm);
            assertClusterStateEqual(GatewayMetaState.AsyncLucenePersistedState.resetVotingConfiguration(state),
                                    reloadedPersistedState.getLastAcceptedState());
            assertThat(reloadedPersistedState.getLastAcceptedState().metadata().index(indexName)).isNotNull();
        }

        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
    }

    @Test
    public void testStatePersistenceWithIOIssues() throws IOException {
        final AtomicReference<Double> ioExceptionRate = new AtomicReference<>(0.01d);
        final List<MockDirectoryWrapper> list = new ArrayList<>();
        final PersistedClusterStateService persistedClusterStateService =
            new PersistedClusterStateService(nodeEnvironment, xContentRegistry(), writableRegistry(), getBigArrays(),
                new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS), () -> 0L) {
                @Override
                Directory createDirectory(Path path) {
                    final MockDirectoryWrapper wrapper = newMockFSDirectory(path);
                    wrapper.setAllowRandomFileNotFoundException(randomBoolean());
                    wrapper.setRandomIOExceptionRate(ioExceptionRate.get());
                    wrapper.setRandomIOExceptionRateOnOpen(ioExceptionRate.get());
                    list.add(wrapper);
                    return wrapper;
                }
            };
        ClusterState state = createClusterState(randomNonNegativeLong(),
                                                Metadata.builder().clusterUUID(randomAlphaOfLength(10)).build());
        long currentTerm = 42L;
        try (GatewayMetaState.LucenePersistedState persistedState = new GatewayMetaState.LucenePersistedState(
            persistedClusterStateService, currentTerm, state)) {

            try {
                if (randomBoolean()) {
                    final ClusterState newState = createClusterState(randomNonNegativeLong(),
                                                                     Metadata.builder().clusterUUID(randomAlphaOfLength(10)).build());
                    persistedState.setLastAcceptedState(newState);
                    state = newState;
                } else {
                    final long newTerm = currentTerm + 1;
                    persistedState.setCurrentTerm(newTerm);
                    currentTerm = newTerm;
                }
            } catch (IOError | Exception e) {
                assertThat(Exceptions.firstCause(e, IOException.class)).isNotNull();
            }

            ioExceptionRate.set(0.0d);
            for (MockDirectoryWrapper wrapper : list) {
                wrapper.setRandomIOExceptionRate(ioExceptionRate.get());
                wrapper.setRandomIOExceptionRateOnOpen(ioExceptionRate.get());
            }

            for (int i = 0; i < randomIntBetween(1, 5); i++) {
                if (randomBoolean()) {
                    final long version = randomNonNegativeLong();
                    final String indexName = randomAlphaOfLength(10);
                    final IndexMetadata indexMetadata = createIndexMetadata(indexName, randomIntBetween(1, 5), randomNonNegativeLong());
                    final Metadata metadata = Metadata.builder().
                        persistentSettings(Settings.builder().put(randomAlphaOfLength(10), randomAlphaOfLength(10)).build()).
                        coordinationMetadata(createCoordinationMetadata(1L)).
                        put(indexMetadata, false).
                        build();
                    state = createClusterState(version, metadata);
                    persistedState.setLastAcceptedState(state);
                } else {
                    currentTerm += 1;
                    persistedState.setCurrentTerm(currentTerm);
                }
            }

            assertThat(persistedState.getLastAcceptedState()).isEqualTo(state);
            assertThat(persistedState.getCurrentTerm()).isEqualTo(currentTerm);

        } catch (IOError | Exception e) {
            if (ioExceptionRate.get() == 0.0d) {
                throw e;
            }
            assertThat(Exceptions.firstCause(e, IOException.class)).isNotNull();
            return;
        }

        nodeEnvironment.close();

        // verify that the freshest state was rewritten to each data path
        for (Path path : nodeEnvironment.nodeDataPaths()) {
            Settings settings = Settings.builder()
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toAbsolutePath())
                .put(Environment.PATH_DATA_SETTING.getKey(), path.getParent().getParent().toString()).build();
            try (NodeEnvironment nodeEnvironment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings))) {
                final PersistedClusterStateService newPersistedClusterStateService =
                    new PersistedClusterStateService(nodeEnvironment, xContentRegistry(), writableRegistry(), getBigArrays(),
                        new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS), () -> 0L);
                final PersistedClusterStateService.OnDiskState onDiskState = newPersistedClusterStateService.loadBestOnDiskState();
                assertThat(onDiskState.empty()).isFalse();
                assertThat(onDiskState.currentTerm).isEqualTo(currentTerm);
                assertClusterStateEqual(state,
                                        ClusterState.builder(ClusterName.DEFAULT)
                                            .version(onDiskState.lastAcceptedVersion)
                                            .metadata(onDiskState.metadata).build());
            }
        }
    }

    private static BigArrays getBigArrays() {
        return usually()
                ? BigArrays.NON_RECYCLING_INSTANCE
                : new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());
    }
}
