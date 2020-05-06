/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package org.elasticsearch.test;

import com.carrotsearch.randomizedtesting.RandomizedContext;
import com.carrotsearch.randomizedtesting.generators.RandomNumbers;
import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.cluster.tasks.PendingClusterTasksResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequestBuilder;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.RestoreInProgress;
import org.elasticsearch.cluster.SnapshotDeletionsInProgress;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.IndexGraveyard;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.RepositoriesMetaData;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings;
import org.elasticsearch.cluster.service.ClusterService;
import javax.annotation.Nullable;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import io.crate.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import io.crate.common.io.IOUtils;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.MergePolicyConfig;
import org.elasticsearch.index.MergeSchedulerConfig;
import org.elasticsearch.index.MockEngineFactoryPlugin;
import org.elasticsearch.index.codec.CodecService;
import org.elasticsearch.index.mapper.MockFieldFilterPlugin;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.IndicesQueryCache;
import org.elasticsearch.indices.store.IndicesStore;
import org.elasticsearch.node.NodeMocksPlugin;
import org.elasticsearch.plugins.NetworkPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.disruption.NetworkDisruption;
import org.elasticsearch.test.disruption.ServiceDisruptionScheme;
import org.elasticsearch.test.store.MockFSIndexStore;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TransportInterceptor;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportService;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_AUTO_EXPAND_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_WAIT_FOR_ACTIVE_SHARDS;
import static org.elasticsearch.discovery.DiscoveryModule.DISCOVERY_SEED_PROVIDERS_SETTING;
import static org.elasticsearch.discovery.SettingsBasedSeedHostsProvider.DISCOVERY_SEED_HOSTS_SETTING;
import static org.elasticsearch.test.XContentTestUtils.convertToMap;
import static org.elasticsearch.test.XContentTestUtils.differenceBetweenMapsIgnoringArrayOrder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoTimeout;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.startsWith;
import static org.hamcrest.core.IsEqual.equalTo;

/**
 * {@link ESIntegTestCase} is an abstract base class to run integration
 * tests against a JVM private Elasticsearch Cluster. The test class supports 2 different
 * cluster scopes.
 * <ul>
 * <li>{@link Scope#TEST} - uses a new cluster for each individual test method.</li>
 * <li>{@link Scope#SUITE} - uses a cluster shared across all test methods in the same suite</li>
 * </ul>
 * <p>
 * The most common test scope is {@link Scope#SUITE} which shares a cluster per test suite.
 * <p>
 * If the test methods need specific node settings or change persistent and/or transient cluster settings {@link Scope#TEST}
 * should be used. To configure a scope for the test cluster the {@link ClusterScope} annotation
 * should be used, here is an example:
 * <pre>
 *
 * {@literal @}NodeScope(scope=Scope.TEST) public class SomeIT extends ESIntegTestCase {
 * public void testMethod() {}
 * }
 * </pre>
 * <p>
 * If no {@link ClusterScope} annotation is present on an integration test the default scope is {@link Scope#SUITE}
 * <p>
 * A test cluster creates a set of nodes in the background before the test starts. The number of nodes in the cluster is
 * determined at random and can change across tests. The {@link ClusterScope} allows configuring the initial number of nodes
 * that are created before the tests start.
 *  <pre>
 * {@literal @}NodeScope(scope=Scope.SUITE, numDataNodes=3)
 * public class SomeIT extends ESIntegTestCase {
 * public void testMethod() {}
 * }
 * </pre>
 * <p>
 * Note, the {@link ESIntegTestCase} uses randomized settings on a cluster and index level. For instance
 * each test might use different directory implementation for each test or will return a random client to one of the
 * nodes in the cluster for each call to {@link #client()}. Test failures might only be reproducible if the correct
 * system properties are passed to the test execution environment.
 * <p>
 * This class supports the following system properties (passed with -Dkey=value to the application)
 * <ul>
 * <li>-D{@value #TESTS_ENABLE_MOCK_MODULES} - a boolean value to enable or disable mock modules. This is
 * useful to test the system without asserting modules that to make sure they don't hide any bugs in production.</li>
 * <li> - a random seed used to initialize the index random context.
 * </ul>
 */
@LuceneTestCase.SuppressFileSystems("ExtrasFS") // doesn't work with potential multi data path from test cluster yet
public abstract class ESIntegTestCase extends ESTestCase {

    @BeforeClass
    public static void disableProcessorCheck() {
        System.setProperty("es.set.netty.runtime.available.processors", "false");
    }

    /** node names of the corresponding clusters will start with these prefixes */
    public static final String SUITE_CLUSTER_NODE_PREFIX = "node_s";
    public static final String TEST_CLUSTER_NODE_PREFIX = "node_t";

    /**
     * Key used to retrieve the index random seed from the index settings on a running node.
     * The value of this seed can be used to initialize a random context for a specific index.
     * It's set once per test via a generic index template.
     */
    public static final Setting<Long> INDEX_TEST_SEED_SETTING =
        Setting.longSetting("index.tests.seed", 0, Long.MIN_VALUE, Property.IndexScope);

    /**
     * A boolean value to enable or disable mock modules. This is useful to test the
     * system without asserting modules that to make sure they don't hide any bugs in
     * production.
     *
     * @see ESIntegTestCase
     */
    public static final String TESTS_ENABLE_MOCK_MODULES = "tests.enable_mock_modules";

    private static final boolean MOCK_MODULES_ENABLED = "true".equals(System.getProperty(TESTS_ENABLE_MOCK_MODULES, "true"));

    /**
     * Default minimum number of shards for an index
     */
    protected static final int DEFAULT_MIN_NUM_SHARDS = 1;

    /**
     * Default maximum number of shards for an index
     */
    protected static final int DEFAULT_MAX_NUM_SHARDS = 10;

    /**
     * The current cluster depending on the configured {@link Scope}.
     * By default if no {@link ClusterScope} is configured this will hold a reference to the suite cluster.
     */
    private static TestCluster currentCluster;

    private static final Map<Class<?>, TestCluster> clusters = new IdentityHashMap<>();

    private static ESIntegTestCase INSTANCE = null; // see @SuiteScope
    private static Long SUITE_SEED = null;

    @BeforeClass
    public static void beforeClass() throws Exception {
        SUITE_SEED = randomLong();
        initializeSuiteScope();
    }

    @Override
    protected final boolean enableWarningsCheck() {
        //In an integ test it doesn't make sense to keep track of warnings: if the cluster is external the warnings are in another jvm,
        //if the cluster is internal the deprecation logger is shared across all nodes
        return false;
    }

    protected final void beforeInternal() throws Exception {
        final Scope currentClusterScope = getCurrentClusterScope();
        switch (currentClusterScope) {
            case SUITE:
                assert SUITE_SEED != null : "Suite seed was not initialized";
                currentCluster = buildAndPutCluster(currentClusterScope, SUITE_SEED);
                break;
            case TEST:
                currentCluster = buildAndPutCluster(currentClusterScope, randomLong());
                break;
            default:
                fail("Unknown Scope: [" + currentClusterScope + "]");
        }
        cluster().beforeTest(random());
        cluster().wipe(excludeTemplates());
        randomIndexTemplate();
    }

    private void printTestMessage(String message) {
        if (isSuiteScopedTest(getClass()) && (getTestName().equals("<unknown>"))) {
            logger.info("[{}]: {} suite", getTestClass().getSimpleName(), message);
        } else {
            logger.info("[{}#{}]: {} test", getTestClass().getSimpleName(), getTestName(), message);
        }
    }

    /**
     * Creates a randomized index template. This template is used to pass in randomized settings on a
     * per index basis. Allows to enable/disable the randomization for number of shards and replicas
     */
    public void randomIndexTemplate() throws IOException {

        // TODO move settings for random directory etc here into the index based randomized settings.
        if (cluster().size() > 0) {
            Settings.Builder randomSettingsBuilder =
                setRandomIndexSettings(random(), Settings.builder());
            if (isInternalCluster()) {
                // this is only used by mock plugins and if the cluster is not internal we just can't set it
                randomSettingsBuilder.put(INDEX_TEST_SEED_SETTING.getKey(), random().nextLong());
            }

            randomSettingsBuilder.put(SETTING_NUMBER_OF_SHARDS, numberOfShards())
                .put(SETTING_NUMBER_OF_REPLICAS, numberOfReplicas());

            // if the test class is annotated with SuppressCodecs("*"), it means don't use lucene's codec randomization
            // otherwise, use it, it has assertions and so on that can find bugs.
            SuppressCodecs annotation = getClass().getAnnotation(SuppressCodecs.class);
            if (annotation != null && annotation.value().length == 1 && "*".equals(annotation.value()[0])) {
                randomSettingsBuilder.put("index.codec", randomFrom(CodecService.DEFAULT_CODEC, CodecService.BEST_COMPRESSION_CODEC));
            } else {
                randomSettingsBuilder.put("index.codec", CodecService.LUCENE_DEFAULT_CODEC);
            }

            for (String setting : randomSettingsBuilder.keys()) {
                assertThat("non index. prefix setting set on index template, its a node setting...", setting, startsWith("index."));
            }
            // always default delayed allocation to 0 to make sure we have tests are not delayed
            randomSettingsBuilder.put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), 0);
            if (randomBoolean()) {
                randomSettingsBuilder.put(IndexModule.INDEX_QUERY_CACHE_ENABLED_SETTING.getKey(), randomBoolean());
            }
            PutIndexTemplateRequestBuilder putTemplate = client().admin().indices()
                .preparePutTemplate("random_index_template")
                .setPatterns(Collections.singletonList("*"))
                .setOrder(0)
                .setSettings(randomSettingsBuilder);
            assertAcked(putTemplate.execute().actionGet());
        }
    }

    protected Settings.Builder setRandomIndexSettings(Random random, Settings.Builder builder) {
        setRandomIndexMergeSettings(random, builder);
        setRandomIndexTranslogSettings(random, builder);

        if (random.nextBoolean()) {
            builder.put(MergeSchedulerConfig.AUTO_THROTTLE_SETTING.getKey(), false);
        }

        if (random.nextBoolean()) {
            builder.put(IndexSettings.INDEX_CHECK_ON_STARTUP.getKey(), randomFrom("false", "checksum", "true"));
        }

        if (randomBoolean()) {
            // keep this low so we don't stall tests
            builder.put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(),
                    RandomNumbers.randomIntBetween(random, 1, 15) + "ms");
        }

        return builder;
    }

    private static Settings.Builder setRandomIndexMergeSettings(Random random, Settings.Builder builder) {
        if (random.nextBoolean()) {
            builder.put(MergePolicyConfig.INDEX_COMPOUND_FORMAT_SETTING.getKey(),
                (random.nextBoolean() ? random.nextDouble() : random.nextBoolean()).toString());
        }
        switch (random.nextInt(4)) {
            case 3:
                final int maxThreadCount = RandomNumbers.randomIntBetween(random, 1, 4);
                final int maxMergeCount = RandomNumbers.randomIntBetween(random, maxThreadCount, maxThreadCount + 4);
                builder.put(MergeSchedulerConfig.MAX_MERGE_COUNT_SETTING.getKey(), maxMergeCount);
                builder.put(MergeSchedulerConfig.MAX_THREAD_COUNT_SETTING.getKey(), maxThreadCount);
                break;
        }

        return builder;
    }

    private static Settings.Builder setRandomIndexTranslogSettings(Random random, Settings.Builder builder) {
        if (random.nextBoolean()) {
            builder.put(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(),
                    new ByteSizeValue(RandomNumbers.randomIntBetween(random, 1, 300), ByteSizeUnit.MB));
        }
        if (random.nextBoolean()) {
            builder.put(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(),
                    new ByteSizeValue(1, ByteSizeUnit.PB)); // just don't flush
        }
        if (random.nextBoolean()) {
            builder.put(IndexSettings.INDEX_TRANSLOG_DURABILITY_SETTING.getKey(),
                    RandomPicks.randomFrom(random, Translog.Durability.values()));
        }

        if (random.nextBoolean()) {
            builder.put(IndexSettings.INDEX_TRANSLOG_SYNC_INTERVAL_SETTING.getKey(),
                    RandomNumbers.randomIntBetween(random, 100, 5000), TimeUnit.MILLISECONDS);
        }

        return builder;
    }

    private TestCluster buildWithPrivateContext(final Scope scope, final long seed) throws Exception {
        return RandomizedContext.current().runWithPrivateRandomness(seed, new Callable<TestCluster>() {
            @Override
            public TestCluster call() throws Exception {
                return buildTestCluster(scope, seed);
            }
        });
    }

    private TestCluster buildAndPutCluster(Scope currentClusterScope, long seed) throws Exception {
        final Class<?> clazz = this.getClass();
        TestCluster testCluster = clusters.remove(clazz); // remove this cluster first
        clearClusters(); // all leftovers are gone by now... this is really just a double safety if we miss something somewhere
        switch (currentClusterScope) {
            case SUITE:
                if (testCluster == null) { // only build if it's not there yet
                    testCluster = buildWithPrivateContext(currentClusterScope, seed);
                }
                break;
            case TEST:
                // close the previous one and create a new one
                IOUtils.closeWhileHandlingException(testCluster);
                testCluster = buildTestCluster(currentClusterScope, seed);
                break;
        }
        clusters.put(clazz, testCluster);
        return testCluster;
    }

    private static void clearClusters() throws IOException {
        if (!clusters.isEmpty()) {
            IOUtils.close(clusters.values());
            clusters.clear();
        }
    }

    protected final void afterInternal(boolean afterClass) throws Exception {
        boolean success = false;
        try {
            final Scope currentClusterScope = getCurrentClusterScope();
            clearDisruptionScheme();
            try {
                if (cluster() != null) {
                    if (currentClusterScope != Scope.TEST) {
                        MetaData metaData = client().admin().cluster().prepareState().execute().actionGet().getState().getMetaData();
                        final Set<String> persistent = metaData.persistentSettings().keySet();
                        assertThat("test leaves persistent cluster metadata behind: " + persistent, persistent.size(), equalTo(0));
                        final Set<String> transientSettings =  new HashSet<>(metaData.transientSettings().keySet());
                        // CRATE_PATCH: crate has a cluster id that is generated upon startup ... remove it here
                        transientSettings.remove("cluster_id");

                        assertThat("test leaves transient cluster metadata behind: " + transientSettings,
                            transientSettings, empty());
                    }
                    ensureClusterSizeConsistency();
                    ensureClusterStateConsistency();
                    beforeIndexDeletion();
                    cluster().wipe(excludeTemplates()); // wipe after to make sure we fail in the test that didn't ack the delete
                    if (afterClass || currentClusterScope == Scope.TEST) {
                        cluster().close();
                    }
                    cluster().assertAfterTest();
                }
            } finally {
                if (currentClusterScope == Scope.TEST) {
                    clearClusters(); // it is ok to leave persistent / transient cluster state behind if scope is TEST
                }
            }
            success = true;
        } finally {
            if (!success) {
                // if we failed here that means that something broke horribly so we should clear all clusters
                // TODO: just let the exception happen, WTF is all this horseshit
                // afterTestRule.forceFailure();
            }
        }
    }

    /**
     * @return An exclude set of index templates that will not be removed in between tests.
     */
    protected Set<String> excludeTemplates() {
        return Collections.emptySet();
    }

    protected void beforeIndexDeletion() throws Exception {
        cluster().beforeIndexDeletion();
    }

    public static TestCluster cluster() {
        return currentCluster;
    }

    public static boolean isInternalCluster() {
        return (currentCluster instanceof InternalTestCluster);
    }

    public static InternalTestCluster internalCluster() {
        if (!isInternalCluster()) {
            throw new UnsupportedOperationException("current test cluster is immutable");
        }
        return (InternalTestCluster) currentCluster;
    }

    public ClusterService clusterService() {
        return internalCluster().clusterService();
    }

    public static Client client() {
        return client(null);
    }

    public static Client client(@Nullable String node) {
        if (node != null) {
            return internalCluster().client(node);
        }
        return cluster().client();
    }

    public static Client dataNodeClient() {
        return internalCluster().dataNodeClient();
    }

    public static Iterable<Client> clients() {
        return cluster().getClients();
    }

    protected int minimumNumberOfShards() {
        return DEFAULT_MIN_NUM_SHARDS;
    }

    protected int maximumNumberOfShards() {
        return DEFAULT_MAX_NUM_SHARDS;
    }

    protected int numberOfShards() {
        return between(minimumNumberOfShards(), maximumNumberOfShards());
    }

    protected int minimumNumberOfReplicas() {
        return 0;
    }

    protected int maximumNumberOfReplicas() {
        //use either 0 or 1 replica, yet a higher amount when possible, but only rarely
        int maxNumReplicas = Math.max(0, cluster().numDataNodes() - 1);
        return frequently() ? Math.min(1, maxNumReplicas) : maxNumReplicas;
    }

    protected int numberOfReplicas() {
        return between(minimumNumberOfReplicas(), maximumNumberOfReplicas());
    }


    public void setDisruptionScheme(ServiceDisruptionScheme scheme) {
        internalCluster().setDisruptionScheme(scheme);
    }

    public void clearDisruptionScheme() {
        if (isInternalCluster()) {
            internalCluster().clearDisruptionScheme();
        }
    }

    /**
     * Returns a settings object used in {@link #createIndex(String...)} and {@link #prepareCreate(String)} and friends.
     * This method can be overwritten by subclasses to set defaults for the indices that are created by the test.
     * By default it returns a settings object that sets a random number of shards. Number of shards and replicas
     * can be controlled through specific methods.
     */
    public Settings indexSettings() {
        Settings.Builder builder = Settings.builder();
        int numberOfShards = numberOfShards();
        if (numberOfShards > 0) {
            builder.put(SETTING_NUMBER_OF_SHARDS, numberOfShards).build();
        }
        int numberOfReplicas = numberOfReplicas();
        if (numberOfReplicas >= 0) {
            builder.put(SETTING_NUMBER_OF_REPLICAS, numberOfReplicas).build();
        }
        // 30% of the time
        if (randomInt(9) < 3) {
            final String dataPath = randomAlphaOfLength(10);
            logger.info("using custom data_path for index: [{}]", dataPath);
            builder.put(IndexMetaData.SETTING_DATA_PATH, dataPath);
        }
        // always default delayed allocation to 0 to make sure we have tests are not delayed
        builder.put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), 0);
        builder.put(SETTING_AUTO_EXPAND_REPLICAS, "false");
        builder.put(SETTING_WAIT_FOR_ACTIVE_SHARDS.getKey(), ActiveShardCount.ONE.toString());
        builder.put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), randomBoolean());
        if (randomBoolean()) {
            builder.put(IndexSettings.INDEX_SOFT_DELETES_RETENTION_OPERATIONS_SETTING.getKey(), between(0, 1000));
        }
        return builder.build();
    }

    /**
     * Creates one or more indices and asserts that the indices are acknowledged. If one of the indices
     * already exists this method will fail and wipe all the indices created so far.
     */
    public final void createIndex(String... names) {

        List<String> created = new ArrayList<>();
        for (String name : names) {
            boolean success = false;
            try {
                assertAcked(prepareCreate(name));
                created.add(name);
                success = true;
            } finally {
                if (!success && !created.isEmpty()) {
                    cluster().wipeIndices(created.toArray(new String[created.size()]));
                }
            }
        }
    }

    /**
     * Creates a new {@link CreateIndexRequestBuilder} with the settings obtained from {@link #indexSettings()}.
     */
    public final CreateIndexRequestBuilder prepareCreate(String index) {
        return prepareCreate(index, Settings.builder());
    }

    /**
     * Creates a new {@link CreateIndexRequestBuilder} with the settings obtained from {@link #indexSettings()}.
     */
    public CreateIndexRequestBuilder prepareCreate(String index, Settings.Builder settingsBuilder) {
        Settings.Builder builder = Settings.builder().put(indexSettings()).put(settingsBuilder.build());
        return client().admin().indices().prepareCreate(index).setSettings(builder.build());
    }

    /**
     * Waits until all nodes have no pending tasks.
     */
    public void waitNoPendingTasksOnAll() throws Exception {
        assertNoTimeout(client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).get());
        assertBusy(() -> {
            for (Client client : clients()) {
                ClusterHealthResponse clusterHealth = client.admin().cluster().prepareHealth().setLocal(true).get();
                assertThat("client " + client + " still has in flight fetch", clusterHealth.getNumberOfInFlightFetch(), equalTo(0));
                PendingClusterTasksResponse pendingTasks = client.admin().cluster().preparePendingClusterTasks().setLocal(true).get();
                assertThat("client " + client + " still has pending tasks " + pendingTasks, pendingTasks, Matchers.emptyIterable());
                clusterHealth = client.admin().cluster().prepareHealth().setLocal(true).get();
                assertThat("client " + client + " still has in flight fetch", clusterHealth.getNumberOfInFlightFetch(), equalTo(0));
            }
        });
        assertNoTimeout(client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).get());
    }

    /**
     * Ensures the cluster has a green state via the cluster health API. This method will also wait for relocations.
     * It is useful to ensure that all action on the cluster have finished and all shards that were currently relocating
     * are now allocated and started.
     */
    public ClusterHealthStatus ensureGreen(String... indices) {
        return ensureGreen(TimeValue.timeValueSeconds(30), indices);
    }

    /**
     * Ensures the cluster has a green state via the cluster health API. This method will also wait for relocations.
     * It is useful to ensure that all action on the cluster have finished and all shards that were currently relocating
     * are now allocated and started.
     *
     * @param timeout time out value to set on {@link org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest}
     */
    public ClusterHealthStatus ensureGreen(TimeValue timeout, String... indices) {
        return ensureColor(ClusterHealthStatus.GREEN, timeout, false, indices);
    }

    /**
     * Ensures the cluster has a yellow state via the cluster health API.
     */
    public ClusterHealthStatus ensureYellow(String... indices) {
        return ensureColor(ClusterHealthStatus.YELLOW, TimeValue.timeValueSeconds(30), false, indices);
    }

    /**
     * Ensures the cluster has a yellow state via the cluster health API and ensures the that cluster has no initializing shards
     * for the given indices
     */
    public ClusterHealthStatus ensureYellowAndNoInitializingShards(String... indices) {
        return ensureColor(ClusterHealthStatus.YELLOW, TimeValue.timeValueSeconds(30), true, indices);
    }

    private ClusterHealthStatus ensureColor(ClusterHealthStatus clusterHealthStatus, TimeValue timeout, boolean waitForNoInitializingShards,
                                            String... indices) {
        String color = clusterHealthStatus.name().toLowerCase(Locale.ROOT);
        String method = "ensure" + Strings.capitalize(color);

        ClusterHealthRequest healthRequest = Requests.clusterHealthRequest(indices)
            .timeout(timeout)
            .waitForStatus(clusterHealthStatus)
            .waitForEvents(Priority.LANGUID)
            .waitForNoRelocatingShards(true)
            .waitForNoInitializingShards(waitForNoInitializingShards)
            // We currently often use ensureGreen or ensureYellow to check whether the cluster is back in a good state after shutting down
            // a node. If the node that is stopped is the master node, another node will become master and publish a cluster state where it
            // is master but where the node that was stopped hasn't been removed yet from the cluster state. It will only subsequently
            // publish a second state where the old master is removed. If the ensureGreen/ensureYellow is timed just right, it will get to
            // execute before the second cluster state update removes the old master and the condition ensureGreen / ensureYellow will
            // trivially hold if it held before the node was shut down. The following "waitForNodes" condition ensures that the node has
            // been removed by the master so that the health check applies to the set of nodes we expect to be part of the cluster.
            .waitForNodes(Integer.toString(cluster().size()));

        ClusterHealthResponse actionGet = client().admin().cluster().health(healthRequest).actionGet();
        if (actionGet.isTimedOut()) {
            logger.info("{} timed out, cluster state:\n{}\n{}",
                method,
                client().admin().cluster().prepareState().get().getState(),
                client().admin().cluster().preparePendingClusterTasks().get());
            fail("timed out waiting for " + color + " state");
        }
        assertThat("Expected at least " + clusterHealthStatus + " but got " + actionGet.getStatus(),
            actionGet.getStatus().value(), lessThanOrEqualTo(clusterHealthStatus.value()));
        logger.debug("indices {} are {}", indices.length == 0 ? "[_all]" : indices, color);
        return actionGet.getStatus();
    }

    /**
     * Waits for all relocating shards to become active using the cluster health API.
     */
    public ClusterHealthStatus waitForRelocation() {
        return waitForRelocation(null);
    }

    /**
     * Waits for all relocating shards to become active and the cluster has reached the given health status
     * using the cluster health API.
     */
    public ClusterHealthStatus waitForRelocation(ClusterHealthStatus status) {
        ClusterHealthRequest request = Requests.clusterHealthRequest().waitForNoRelocatingShards(true);
        if (status != null) {
            request.waitForStatus(status);
        }
        ClusterHealthResponse actionGet = client().admin().cluster()
            .health(request).actionGet();
        if (actionGet.isTimedOut()) {
            logger.info("waitForRelocation timed out (status={}), cluster state:\n{}\n{}", status,
                client().admin().cluster().prepareState().get().getState(), client().admin().cluster().preparePendingClusterTasks().get());
            assertThat("timed out waiting for relocation", actionGet.isTimedOut(), equalTo(false));
        }
        if (status != null) {
            assertThat(actionGet.getStatus(), equalTo(status));
        }
        return actionGet.getStatus();
    }

    /**
     * Prints the current cluster state as debug logging.
     */
    public void logClusterState() {
        logger.debug("cluster state:\n{}\n{}",
            client().admin().cluster().prepareState().get().getState(), client().admin().cluster().preparePendingClusterTasks().get());
    }

    protected void ensureClusterSizeConsistency() {
        if (cluster() != null && cluster().size() > 0) { // if static init fails the cluster can be null
            logger.trace("Check consistency for [{}] nodes", cluster().size());
            assertNoTimeout(client().admin().cluster().prepareHealth().setWaitForNodes(Integer.toString(cluster().size())).get());
        }
    }

    /**
     * Verifies that all nodes that have the same version of the cluster state as master have same cluster state
     */
    protected void ensureClusterStateConsistency() throws IOException {
        if (cluster() != null && cluster().size() > 0) {
            final NamedWriteableRegistry namedWriteableRegistry = cluster().getNamedWriteableRegistry();
            final Client masterClient = client();
            ClusterState masterClusterState = masterClient.admin().cluster().prepareState().all().get().getState();
            byte[] masterClusterStateBytes = ClusterState.Builder.toBytes(masterClusterState);
            // remove local node reference
            masterClusterState = ClusterState.Builder.fromBytes(masterClusterStateBytes, null, namedWriteableRegistry);
            int masterClusterStateSize = ClusterState.Builder.toBytes(masterClusterState).length;
            String masterId = masterClusterState.nodes().getMasterNodeId();
            for (Client client : cluster().getClients()) {
                ClusterState localClusterState = client.admin().cluster().prepareState().all().setLocal(true).get().getState();
                byte[] localClusterStateBytes = ClusterState.Builder.toBytes(localClusterState);
                // remove local node reference
                localClusterState = ClusterState.Builder.fromBytes(localClusterStateBytes, null, namedWriteableRegistry);
                final int localClusterStateSize = ClusterState.Builder.toBytes(localClusterState).length;
                // Check that the non-master node has the same version of the cluster state as the master and
                // that the master node matches the master (otherwise there is no requirement for the cluster state to match)
                if (masterClusterState.version() == localClusterState.version()
                        && masterId.equals(localClusterState.nodes().getMasterNodeId())) {
                    try {
                        assertThat("cluster state UUID does not match", masterClusterState.stateUUID(), is(localClusterState.stateUUID()));
                        assertThat("cluster state size does not match", masterClusterStateSize, is(localClusterStateSize));
                        // remove non-core customs and compare the cluster states
                        assertNull(
                                "cluster state JSON serialization does not match (after removing some customs)",
                                differenceBetweenMapsIgnoringArrayOrder(
                                        convertToMap(removePluginCustoms(masterClusterState)),
                                        convertToMap(removePluginCustoms(localClusterState))));
                    } catch (final AssertionError error) {
                        logger.error(
                                "Cluster state from master:\n{}\nLocal cluster state:\n{}",
                                masterClusterState.toString(),
                                localClusterState.toString());
                        throw error;
                    }
                }
            }
        }

    }

    private static final Set<String> SAFE_METADATA_CUSTOMS =
            Collections.unmodifiableSet(
                    new HashSet<>(Arrays.asList(IndexGraveyard.TYPE, RepositoriesMetaData.TYPE)));

    private static final Set<String> SAFE_CUSTOMS =
            Collections.unmodifiableSet(
                    new HashSet<>(Arrays.asList(RestoreInProgress.TYPE, SnapshotDeletionsInProgress.TYPE, SnapshotsInProgress.TYPE)));

    /**
     * Remove any customs except for customs that we know all clients understand.
     *
     * @param clusterState the cluster state to remove possibly-unknown customs from
     * @return the cluster state with possibly-unknown customs removed
     */
    private ClusterState removePluginCustoms(final ClusterState clusterState) {
        final ClusterState.Builder builder = ClusterState.builder(clusterState);
        clusterState.customs().keysIt().forEachRemaining(key -> {
            if (SAFE_CUSTOMS.contains(key) == false) {
                builder.removeCustom(key);
            }
        });
        final MetaData.Builder mdBuilder = MetaData.builder(clusterState.metaData());
        clusterState.metaData().customs().keysIt().forEachRemaining(key -> {
            if (SAFE_METADATA_CUSTOMS.contains(key) == false) {
                mdBuilder.removeCustom(key);
            }
        });
        builder.metaData(mdBuilder);
        return builder.build();
    }

    protected void ensureStableCluster(int nodeCount) {
        ensureStableCluster(nodeCount, TimeValue.timeValueSeconds(30));
    }

    protected void ensureStableCluster(int nodeCount, TimeValue timeValue) {
        ensureStableCluster(nodeCount, timeValue, false, null);
    }

    protected void ensureStableCluster(int nodeCount, @Nullable String viaNode) {
        ensureStableCluster(nodeCount, TimeValue.timeValueSeconds(30), false, viaNode);
    }

    protected void ensureStableCluster(int nodeCount, TimeValue timeValue, boolean local, @Nullable String viaNode) {
        if (viaNode == null) {
            viaNode = randomFrom(internalCluster().getNodeNames());
        }
        logger.debug("ensuring cluster is stable with [{}] nodes. access node: [{}]. timeout: [{}]", nodeCount, viaNode, timeValue);
        ClusterHealthResponse clusterHealthResponse = client(viaNode).admin().cluster().prepareHealth()
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForNodes(Integer.toString(nodeCount))
            .setTimeout(timeValue)
            .setLocal(local)
            .setWaitForNoRelocatingShards(true)
            .get();
        if (clusterHealthResponse.isTimedOut()) {
            ClusterStateResponse stateResponse = client(viaNode).admin().cluster().prepareState().get();
            fail("failed to reach a stable cluster of [" + nodeCount + "] nodes. Tried via [" + viaNode + "]. last cluster state:\n"
                 + stateResponse.getState());
        }
        assertThat(clusterHealthResponse.isTimedOut(), is(false));
        ensureFullyConnectedCluster();
    }

    /**
     * See {@link NetworkDisruption#ensureFullyConnectedCluster(InternalTestCluster)}
     */
    protected void ensureFullyConnectedCluster() {
        NetworkDisruption.ensureFullyConnectedCluster(internalCluster());
    }


    /**
     * Waits for relocations and refreshes all indices in the cluster.
     *
     * @see #waitForRelocation()
     */
    protected final RefreshResponse refresh(String... indices) {
        waitForRelocation();
        // TODO RANDOMIZE with flush?
        RefreshResponse actionGet = client().admin().indices().prepareRefresh(indices).execute().actionGet();
        assertNoFailures(actionGet);
        return actionGet;
    }

    /**
     * Returns a random admin client. This client can either be a node or a transport client pointing to any of
     * the nodes in the cluster.
     */
    protected AdminClient admin() {
        return client().admin();
    }

    /**
     * The scope of a test cluster used together with
     * {@link ESIntegTestCase.ClusterScope} annotations on {@link ESIntegTestCase} subclasses.
     */
    public enum Scope {
        /**
         * A cluster shared across all method in a single test suite
         */
        SUITE,
        /**
         * A test exclusive test cluster
         */
        TEST
    }

    /**
     * Defines a cluster scope for a {@link ESIntegTestCase} subclass.
     * By default if no {@link ClusterScope} annotation is present {@link ESIntegTestCase.Scope#SUITE} is used
     * together with randomly chosen settings like number of nodes etc.
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target({ElementType.TYPE})
    public @interface ClusterScope {
        /**
         * Returns the scope. {@link ESIntegTestCase.Scope#SUITE} is default.
         */
        Scope scope() default Scope.SUITE;

        /**
         * Returns the number of nodes in the cluster. Default is {@code -1} which means
         * a random number of nodes is used, where the minimum and maximum number of nodes
         * are either the specified ones or the default ones if not specified.
         */
        int numDataNodes() default -1;

        /**
         * Returns the minimum number of data nodes in the cluster. Default is {@code -1}.
         * Ignored when {@link ClusterScope#numDataNodes()} is set.
         */
        int minNumDataNodes() default -1;

        /**
         * Returns the maximum number of data nodes in the cluster.  Default is {@code -1}.
         * Ignored when {@link ClusterScope#numDataNodes()} is set.
         */
        int maxNumDataNodes() default -1;

        /**
         * Indicates whether the cluster can have dedicated master nodes. If {@code false} means data nodes will serve as master nodes
         * and there will be no dedicated master (and data) nodes. Default is {@code false} which means
         * dedicated master nodes will be randomly used.
         */
        boolean supportsDedicatedMasters() default true;

        /**
         * The cluster automatically manages the bootstrap voting configuration. Set this to false to manage the setting manually.
         */
        boolean autoMinMasterNodes() default true;

        /**
         * Returns the number of client nodes in the cluster. Default is {@link InternalTestCluster#DEFAULT_NUM_CLIENT_NODES}, a
         * negative value means that the number of client nodes will be randomized.
         */
        int numClientNodes() default InternalTestCluster.DEFAULT_NUM_CLIENT_NODES;
    }

    private static <A extends Annotation> A getAnnotation(Class<?> clazz, Class<A> annotationClass) {
        if (clazz == Object.class || clazz == ESIntegTestCase.class) {
            return null;
        }
        A annotation = clazz.getAnnotation(annotationClass);
        if (annotation != null) {
            return annotation;
        }
        return getAnnotation(clazz.getSuperclass(), annotationClass);
    }

    private Scope getCurrentClusterScope() {
        return getCurrentClusterScope(this.getClass());
    }

    private static Scope getCurrentClusterScope(Class<?> clazz) {
        ClusterScope annotation = getAnnotation(clazz, ClusterScope.class);
        // if we are not annotated assume suite!
        return annotation == null ? Scope.SUITE : annotation.scope();
    }

    private boolean getSupportsDedicatedMasters() {
        ClusterScope annotation = getAnnotation(this.getClass(), ClusterScope.class);
        return annotation == null ? true : annotation.supportsDedicatedMasters();
    }

    private boolean getAutoMinMasterNodes() {
        ClusterScope annotation = getAnnotation(this.getClass(), ClusterScope.class);
        return annotation == null ? true : annotation.autoMinMasterNodes();
    }

    private int getNumDataNodes() {
        ClusterScope annotation = getAnnotation(this.getClass(), ClusterScope.class);
        return annotation == null ? -1 : annotation.numDataNodes();
    }

    private int getMinNumDataNodes() {
        ClusterScope annotation = getAnnotation(this.getClass(), ClusterScope.class);
        return annotation == null || annotation.minNumDataNodes() == -1
                ? InternalTestCluster.DEFAULT_MIN_NUM_DATA_NODES : annotation.minNumDataNodes();
    }

    private int getMaxNumDataNodes() {
        ClusterScope annotation = getAnnotation(this.getClass(), ClusterScope.class);
        return annotation == null || annotation.maxNumDataNodes() == -1
                ? InternalTestCluster.DEFAULT_MAX_NUM_DATA_NODES : annotation.maxNumDataNodes();
    }

    private int getNumClientNodes() {
        ClusterScope annotation = getAnnotation(this.getClass(), ClusterScope.class);
        return annotation == null ? InternalTestCluster.DEFAULT_NUM_CLIENT_NODES : annotation.numClientNodes();
    }

    /**
     * This method is used to obtain settings for the {@code N}th node in the cluster.
     * Nodes in this cluster are associated with an ordinal number such that nodes can
     * be started with specific configurations. This method might be called multiple
     * times with the same ordinal and is expected to return the same value for each invocation.
     * In other words subclasses must ensure this method is idempotent.
     */
    protected Settings nodeSettings(int nodeOrdinal) {
        Settings.Builder builder = Settings.builder()
            // Default the watermarks to absurdly low to prevent the tests
            // from failing on nodes without enough disk space
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(), "1b")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(), "1b")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.getKey(), "1b")
            // by default we never cache below 10k docs in a segment,
            // bypass this limit so that caching gets some testing in
            // integration tests that usually create few documents
            .put(IndicesQueryCache.INDICES_QUERIES_CACHE_ALL_SEGMENTS_SETTING.getKey(), nodeOrdinal % 2 == 0)
            // wait short time for other active shards before actually deleting, default 30s not needed in tests
            .put(IndicesStore.INDICES_STORE_DELETE_SHARD_TIMEOUT.getKey(), new TimeValue(1, TimeUnit.SECONDS))
            .putList(DISCOVERY_SEED_HOSTS_SETTING.getKey()) // empty list disables a port scan for other nodes
            .putList(DISCOVERY_SEED_PROVIDERS_SETTING.getKey(), "file");

        return builder.build();
    }

    protected Path nodeConfigPath(int nodeOrdinal) {
        return null;
    }

    /**
     * Returns a collection of plugins that should be loaded on each node.
     */
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.emptyList();
    }

    /**
     * Returns a collection of plugins that should be loaded when creating a transport client.
     */
    protected Collection<Class<? extends Plugin>> transportClientPlugins() {
        return Collections.emptyList();
    }

    /**
     * This method is used to obtain additional settings for clients created by the internal cluster.
     * These settings will be applied on the client in addition to some randomized settings defined in
     * the cluster. These settings will also override any other settings the internal cluster might
     * add by default.
     */
    protected Settings transportClientSettings() {
        return Settings.EMPTY;
    }

    protected TestCluster buildTestCluster(Scope scope, long seed) throws IOException {
        final String nodePrefix;
        switch (scope) {
            case TEST:
                nodePrefix = TEST_CLUSTER_NODE_PREFIX;
                break;
            case SUITE:
                nodePrefix = SUITE_CLUSTER_NODE_PREFIX;
                break;
            default:
                throw new ElasticsearchException("Scope not supported: " + scope);
        }


        boolean supportsDedicatedMasters = getSupportsDedicatedMasters();
        int numDataNodes = getNumDataNodes();
        int minNumDataNodes;
        int maxNumDataNodes;
        if (numDataNodes >= 0) {
            minNumDataNodes = maxNumDataNodes = numDataNodes;
        } else {
            minNumDataNodes = getMinNumDataNodes();
            maxNumDataNodes = getMaxNumDataNodes();
        }
        Collection<Class<? extends Plugin>> mockPlugins = getMockPlugins();
        final NodeConfigurationSource nodeConfigurationSource = getNodeConfigSource();
        if (addMockTransportService()) {
            ArrayList<Class<? extends Plugin>> mocks = new ArrayList<>(mockPlugins);
            // add both mock plugins - local and tcp if they are not there
            // we do this in case somebody overrides getMockPlugins and misses to call super
            if (mockPlugins.contains(getTestTransportPlugin()) == false) {
                mocks.add(getTestTransportPlugin());
            }
            mockPlugins = mocks;
        }
        return new InternalTestCluster(seed, createTempDir(), supportsDedicatedMasters, getAutoMinMasterNodes(),
            minNumDataNodes, maxNumDataNodes,
            InternalTestCluster.clusterName(scope.name(), seed) + "-cluster", nodeConfigurationSource, getNumClientNodes(),
            nodePrefix, mockPlugins, getClientWrapper(), forbidPrivateIndexSettings());
    }

    protected NodeConfigurationSource getNodeConfigSource() {
        Settings.Builder networkSettings = Settings.builder();
        if (addMockTransportService()) {
            networkSettings.put(NetworkModule.TRANSPORT_TYPE_KEY, getTestTransportType());
        }

        return new NodeConfigurationSource() {
            @Override
            public Settings nodeSettings(int nodeOrdinal) {
                return Settings.builder()
                    .put(networkSettings.build())
                    .put(ESIntegTestCase.this.nodeSettings(nodeOrdinal)).build();
            }

            @Override
            public Path nodeConfigPath(int nodeOrdinal) {
                return ESIntegTestCase.this.nodeConfigPath(nodeOrdinal);
            }

            @Override
            public Collection<Class<? extends Plugin>> nodePlugins() {
                return ESIntegTestCase.this.nodePlugins();
            }

            @Override
            public Settings transportClientSettings() {
                return Settings.builder().put(networkSettings.build())
                    .put(ESIntegTestCase.this.transportClientSettings()).build();
            }

            @Override
            public Collection<Class<? extends Plugin>> transportClientPlugins() {
                Collection<Class<? extends Plugin>> plugins = ESIntegTestCase.this.transportClientPlugins();
                if (plugins.contains(getTestTransportPlugin()) == false) {
                    plugins = new ArrayList<>(plugins);
                    plugins.add(getTestTransportPlugin());
                }
                return Collections.unmodifiableCollection(plugins);
            }
        };
    }

    /**
     * Iff this returns true mock transport implementations are used for the test runs. Otherwise not mock transport impls are used.
     * The default is {@code true}.
     */
    protected boolean addMockTransportService() {
        return true;
    }

    /** Returns {@code true} iff this test cluster should use a dummy http transport */
    protected boolean addMockHttpTransport() {
        return true;
    }

    /**
     * Returns a function that allows to wrap / filter all clients that are exposed by the test cluster. This is useful
     * for debugging or request / response pre and post processing. It also allows to intercept all calls done by the test
     * framework. By default this method returns an identity function {@link Function#identity()}.
     */
    protected Function<Client,Client> getClientWrapper() {
        return Function.identity();
    }

    /** Return the mock plugins the cluster should use */
    protected Collection<Class<? extends Plugin>> getMockPlugins() {
        final ArrayList<Class<? extends Plugin>> mocks = new ArrayList<>();
        if (MOCK_MODULES_ENABLED && randomBoolean()) { // sometimes run without those completely
            if (randomBoolean() && addMockTransportService()) {
                mocks.add(MockTransportService.TestPlugin.class);
            }
            if (randomBoolean()) {
                mocks.add(MockFSIndexStore.TestPlugin.class);
            }
            if (randomBoolean()) {
                mocks.add(NodeMocksPlugin.class);
            }
            if (randomBoolean()) {
                mocks.add(MockEngineFactoryPlugin.class);
            }
            if (randomBoolean()) {
                mocks.add(MockFieldFilterPlugin.class);
            }
        }

        if (addMockTransportService()) {
            mocks.add(getTestTransportPlugin());
        }

        if (addMockHttpTransport()) {
            mocks.add(MockHttpTransport.TestPlugin.class);
        }

        mocks.add(TestSeedPlugin.class);
        mocks.add(AssertActionNamePlugin.class);
        return Collections.unmodifiableList(mocks);
    }

    public static final class TestSeedPlugin extends Plugin {
        @Override
        public List<Setting<?>> getSettings() {
            return Collections.singletonList(INDEX_TEST_SEED_SETTING);
        }
    }

    public static final class AssertActionNamePlugin extends Plugin implements NetworkPlugin {
        @Override
        public List<TransportInterceptor> getTransportInterceptors(NamedWriteableRegistry namedWriteableRegistry,
                                                                   ThreadContext threadContext) {
            return Arrays.asList(new TransportInterceptor() {
                @Override
                public <T extends TransportRequest> TransportRequestHandler<T> interceptHandler(String action, String executor,
                                                                                                boolean forceExecution,
                                                                                                TransportRequestHandler<T> actualHandler) {
                    if (TransportService.isValidActionName(action) == false) {
                        throw new IllegalArgumentException("invalid action name [" + action + "] must start with one of: " +
                            TransportService.VALID_ACTION_PREFIXES );
                    }
                    return actualHandler;
                }
            });
        }
    }

    /**
     * Returns path to a random directory that can be used to create a temporary file system repo
     */
    public Path randomRepoPath() {
        if (currentCluster instanceof InternalTestCluster) {
            return randomRepoPath(((InternalTestCluster) currentCluster).getDefaultSettings());
        }
        throw new UnsupportedOperationException("unsupported cluster type");
    }

    /**
     * Returns path to a random directory that can be used to create a temporary file system repo
     */
    public static Path randomRepoPath(Settings settings) {
        Environment environment = TestEnvironment.newEnvironment(settings);
        Path[] repoFiles = environment.repoFiles();
        assert repoFiles.length > 0;
        Path path;
        do {
            path = repoFiles[0].resolve(randomAlphaOfLength(10));
        } while (Files.exists(path));
        return path;
    }


    /**
     * Asserts that all shards are allocated on nodes matching the given node pattern.
     */
    public Set<String> assertAllShardsOnNodes(String index, String... pattern) {
        Set<String> nodes = new HashSet<>();
        ClusterState clusterState = client().admin().cluster().prepareState().execute().actionGet().getState();
        for (IndexRoutingTable indexRoutingTable : clusterState.routingTable()) {
            for (IndexShardRoutingTable indexShardRoutingTable : indexRoutingTable) {
                for (ShardRouting shardRouting : indexShardRoutingTable) {
                    if (shardRouting.currentNodeId() != null && index.equals(shardRouting.getIndexName())) {
                        String name = clusterState.nodes().get(shardRouting.currentNodeId()).getName();
                        nodes.add(name);
                        assertThat("Allocated on new node: " + name, Regex.simpleMatch(pattern, name), is(true));
                    }
                }
            }
        }
        return nodes;
    }

    private static boolean runTestScopeLifecycle() {
        return INSTANCE == null;
    }

    @Before
    public final void setupTestCluster() throws Exception {
        if (runTestScopeLifecycle()) {
            printTestMessage("setting up");
            beforeInternal();
            printTestMessage("all set up");
        }
    }

    @After
    public final void cleanUpCluster() throws Exception {
        // Deleting indices is going to clear search contexts implicitly so we
        // need to check that there are no more in-flight search contexts before
        // we remove indices
        if (runTestScopeLifecycle()) {
            printTestMessage("cleaning up after");
            afterInternal(false);
            printTestMessage("cleaned up after");
        }
    }

    @AfterClass
    public static void afterClass() throws Exception {
        if (!runTestScopeLifecycle()) {
            try {
                INSTANCE.printTestMessage("cleaning up after");
                INSTANCE.afterInternal(true);
                checkStaticState(true);
            } finally {
                INSTANCE = null;
            }
        } else {
            clearClusters();
        }
        SUITE_SEED = null;
        currentCluster = null;
    }

    private static void initializeSuiteScope() throws Exception {
        Class<?> targetClass = getTestClass();
        /**
         * Note we create these test class instance via reflection
         * since JUnit creates a new instance per test and that is also
         * the reason why INSTANCE is static since this entire method
         * must be executed in a static context.
         */
        assert INSTANCE == null;
        if (isSuiteScopedTest(targetClass)) {
            // note we need to do this this way to make sure this is reproducible
            INSTANCE = (ESIntegTestCase) targetClass.getConstructor().newInstance();
            boolean success = false;
            try {
                INSTANCE.printTestMessage("setup");
                INSTANCE.beforeInternal();
                INSTANCE.setupSuiteScopeCluster();
                success = true;
            } finally {
                if (!success) {
                    afterClass();
                }
            }
        } else {
            INSTANCE = null;
        }
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        if (isInternalCluster() && cluster().size() > 0) {
            // If it's internal cluster - using existing registry in case plugin registered custom data
            return internalCluster().getInstance(NamedXContentRegistry.class);
        } else {
            // If it's external cluster - fall back to the standard set
            return new NamedXContentRegistry(ClusterModule.getNamedXWriteables());
        }
    }

    protected boolean forbidPrivateIndexSettings() {
        return true;
    }

    /**
     * This method is executed iff the test is annotated with {@link SuiteScopeTestCase}
     * before the first test of this class is executed.
     *
     * @see SuiteScopeTestCase
     */
    protected void setupSuiteScopeCluster() throws Exception {
    }

    private static boolean isSuiteScopedTest(Class<?> clazz) {
        return clazz.getAnnotation(SuiteScopeTestCase.class) != null;
    }

    /**
     * If a test is annotated with {@link SuiteScopeTestCase}
     * the checks and modifications that are applied to the used test cluster are only done after all tests
     * of this class are executed. This also has the side-effect of a suite level setup method {@link #setupSuiteScopeCluster()}
     * that is executed in a separate test instance. Variables that need to be accessible across test instances must be static.
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Inherited
    @Target(ElementType.TYPE)
    public @interface SuiteScopeTestCase {
    }
}
