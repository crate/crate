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
package org.elasticsearch.test;

import com.carrotsearch.hppc.ObjectLongMap;
import com.carrotsearch.hppc.cursors.IntObjectCursor;
import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import com.carrotsearch.randomizedtesting.RandomizedTest;
import com.carrotsearch.randomizedtesting.SeedUtils;
import com.carrotsearch.randomizedtesting.SysGlobals;
import com.carrotsearch.randomizedtesting.generators.RandomNumbers;
import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import com.carrotsearch.randomizedtesting.generators.RandomStrings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.action.admin.cluster.configuration.AddVotingConfigExclusionsAction;
import org.elasticsearch.action.admin.cluster.configuration.AddVotingConfigExclusionsRequest;
import org.elasticsearch.action.admin.cluster.configuration.ClearVotingConfigExclusionsAction;
import org.elasticsearch.action.admin.cluster.configuration.ClearVotingConfigExclusionsRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.action.index.MappingUpdatedAction;
import org.elasticsearch.cluster.coordination.ClusterBootstrapService;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNode.Role;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.OperationRouting;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings;
import org.elasticsearch.cluster.routing.allocation.decider.ThrottlingAllocationDecider;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.component.LifecycleListener;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.Settings.Builder;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import io.crate.common.unit.TimeValue;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.util.set.Sets;
import io.crate.common.io.IOUtils;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.ShardLockObtainFailedException;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.CommitStats;
import org.elasticsearch.index.engine.DocIdSeqNoAndSource;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.InternalEngine;
import org.elasticsearch.index.seqno.SeqNoStats;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardTestCase;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.indices.fielddata.cache.IndicesFieldDataCache;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.node.MockNode;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeValidationException;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.disruption.ServiceDisruptionScheme;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.transport.TransportSettings;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.util.Collections.emptyList;
import static org.apache.lucene.util.LuceneTestCase.TEST_NIGHTLY;
import static org.apache.lucene.util.LuceneTestCase.rarely;
import static org.elasticsearch.cluster.coordination.ClusterBootstrapService.INITIAL_MASTER_NODES_SETTING;
import static io.crate.common.unit.TimeValue.timeValueSeconds;
import static org.elasticsearch.discovery.DiscoveryModule.DISCOVERY_TYPE_SETTING;
import static org.elasticsearch.discovery.DiscoveryModule.ZEN2_DISCOVERY_TYPE;
import static org.elasticsearch.discovery.DiscoverySettings.INITIAL_STATE_TIMEOUT_SETTING;
import static org.elasticsearch.discovery.FileBasedSeedHostsProvider.UNICAST_HOSTS_FILE;
import static org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService.TOTAL_CIRCUIT_BREAKER_LIMIT_SETTING;
import static org.elasticsearch.test.ESTestCase.assertBusy;
import static org.elasticsearch.test.ESTestCase.awaitBusy;
import static org.elasticsearch.test.ESTestCase.randomFrom;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * InternalTestCluster manages a set of JVM private nodes and allows convenient access to them.
 * The cluster supports randomized configuration such that nodes started in the cluster will
 * automatically load asserting services tracking resources like file handles or open searchers.
 * <p>
 * The Cluster is bound to a test lifecycle where tests must call {@link #beforeTest(java.util.Random)} and
 * {@link #afterTest()} to initialize and reset the cluster in order to be more reproducible. The term "more" relates
 * to the async nature of Elasticsearch in combination with randomized testing. Once Threads and asynchronous calls
 * are involved reproducibility is very limited. This class should only be used through {@link ESIntegTestCase}.
 * </p>
 */
public final class InternalTestCluster extends TestCluster {

    private final Logger logger = LogManager.getLogger(getClass());

    private static final Predicate<NodeAndClient> DATA_NODE_PREDICATE =
        nodeAndClient -> DiscoveryNode.isDataNode(nodeAndClient.node.settings());

    private static final Predicate<NodeAndClient> NO_DATA_NO_MASTER_PREDICATE = nodeAndClient ->
        DiscoveryNode.isMasterNode(nodeAndClient.node.settings()) == false
            && DiscoveryNode.isDataNode(nodeAndClient.node.settings()) == false;

    private static final Predicate<NodeAndClient> MASTER_NODE_PREDICATE =
        nodeAndClient -> DiscoveryNode.isMasterNode(nodeAndClient.node.settings());

    public static final int DEFAULT_LOW_NUM_MASTER_NODES = 1;
    public static final int DEFAULT_HIGH_NUM_MASTER_NODES = 3;

    static final int DEFAULT_MIN_NUM_DATA_NODES = 1;
    static final int DEFAULT_MAX_NUM_DATA_NODES = TEST_NIGHTLY ? 6 : 3;

    static final int DEFAULT_NUM_CLIENT_NODES = -1;
    static final int DEFAULT_MIN_NUM_CLIENT_NODES = 0;
    static final int DEFAULT_MAX_NUM_CLIENT_NODES = 1;

    /* Sorted map to make traverse order reproducible.
     * The map of nodes is never mutated so individual reads are safe without synchronization.
     * Updates are intended to follow a copy-on-write approach. */
    private volatile NavigableMap<String, NodeAndClient> nodes = Collections.emptyNavigableMap();

    private final Set<Path> dataDirToClean = new HashSet<>();

    private final String clusterName;

    private final AtomicBoolean open = new AtomicBoolean(true);

    private final Settings defaultSettings;

    private final AtomicInteger nextNodeId = new AtomicInteger(0);

    /* Each shared node has a node seed that is used to start up the node and get default settings
     * this is important if a node is randomly shut down in a test since the next test relies on a
     * fully shared cluster to be more reproducible */
    private final long[] sharedNodesSeeds;


    // if set to 0, data nodes will also assume the master role
    private final int numSharedDedicatedMasterNodes;

    private final int numSharedDataNodes;

    private final int numSharedCoordOnlyNodes;

    private final NodeConfigurationSource nodeConfigurationSource;

    private final ExecutorService executor;

    private final boolean autoManageMinMasterNodes;

    private final Collection<Class<? extends Plugin>> mockPlugins;

    private final boolean forbidPrivateIndexSettings;

    private final int numDataPaths;

    /**
     * All nodes started by the cluster will have their name set to nodePrefix followed by a positive number
     */
    private final String nodePrefix;
    private final Path baseDir;

    private ServiceDisruptionScheme activeDisruptionScheme;
    private final Function<Client, Client> clientWrapper;

    private int bootstrapMasterNodeIndex = -1;

    public InternalTestCluster(
            final long clusterSeed,
            final Path baseDir,
            final boolean randomlyAddDedicatedMasters,
            final boolean autoManageMinMasterNodes,
            final int minNumDataNodes,
            final int maxNumDataNodes,
            final String clusterName,
            final NodeConfigurationSource nodeConfigurationSource,
            final int numClientNodes,
            final String nodePrefix,
            final Collection<Class<? extends Plugin>> mockPlugins,
            final Function<Client, Client> clientWrapper) {
        this(
                clusterSeed,
                baseDir,
                randomlyAddDedicatedMasters,
                autoManageMinMasterNodes,
                minNumDataNodes,
                maxNumDataNodes,
                clusterName,
                nodeConfigurationSource,
                numClientNodes,
                nodePrefix,
                mockPlugins,
                clientWrapper,
                true);
    }

    public InternalTestCluster(
            final long clusterSeed,
            final Path baseDir,
            final boolean randomlyAddDedicatedMasters,
            final boolean autoManageMinMasterNodes,
            final int minNumDataNodes,
            final int maxNumDataNodes,
            final String clusterName,
            final NodeConfigurationSource nodeConfigurationSource,
            final int numClientNodes,
            final String nodePrefix,
            final Collection<Class<? extends Plugin>> mockPlugins,
            final Function<Client, Client> clientWrapper,
            final boolean forbidPrivateIndexSettings) {
        super(clusterSeed);
        this.autoManageMinMasterNodes = autoManageMinMasterNodes;
        this.clientWrapper = clientWrapper;
        this.forbidPrivateIndexSettings = forbidPrivateIndexSettings;
        this.baseDir = baseDir;
        this.clusterName = clusterName;
        if (minNumDataNodes < 0 || maxNumDataNodes < 0) {
            throw new IllegalArgumentException("minimum and maximum number of data nodes must be >= 0");
        }

        if (maxNumDataNodes < minNumDataNodes) {
            throw new IllegalArgumentException("maximum number of data nodes must be >= minimum number of  data nodes");
        }

        Random random = new Random(clusterSeed);

        boolean useDedicatedMasterNodes = randomlyAddDedicatedMasters ? random.nextBoolean() : false;

        this.numSharedDataNodes = RandomNumbers.randomIntBetween(random, minNumDataNodes, maxNumDataNodes);
        assert this.numSharedDataNodes >= 0;

        if (numSharedDataNodes == 0) {
            this.numSharedCoordOnlyNodes = 0;
            this.numSharedDedicatedMasterNodes = 0;
        } else {
            if (useDedicatedMasterNodes) {
                if (random.nextBoolean()) {
                    // use a dedicated master, but only low number to reduce overhead to tests
                    this.numSharedDedicatedMasterNodes = DEFAULT_LOW_NUM_MASTER_NODES;
                } else {
                    this.numSharedDedicatedMasterNodes = DEFAULT_HIGH_NUM_MASTER_NODES;
                }
            } else {
                this.numSharedDedicatedMasterNodes = 0;
            }
            if (numClientNodes < 0) {
                this.numSharedCoordOnlyNodes =  RandomNumbers.randomIntBetween(random,
                        DEFAULT_MIN_NUM_CLIENT_NODES, DEFAULT_MAX_NUM_CLIENT_NODES);
            } else {
                this.numSharedCoordOnlyNodes = numClientNodes;
            }
        }
        assert this.numSharedCoordOnlyNodes >= 0;

        this.nodePrefix = nodePrefix;

        assert nodePrefix != null;

        this.mockPlugins = mockPlugins;

        sharedNodesSeeds = new long[numSharedDedicatedMasterNodes + numSharedDataNodes + numSharedCoordOnlyNodes];
        for (int i = 0; i < sharedNodesSeeds.length; i++) {
            sharedNodesSeeds[i] = random.nextLong();
        }

        logger.info("Setup InternalTestCluster [{}] with seed [{}] using [{}] dedicated masters, " +
                "[{}] (data) nodes and [{}] coord only nodes (min_master_nodes are [{}])",
            clusterName, SeedUtils.formatSeed(clusterSeed),
            numSharedDedicatedMasterNodes, numSharedDataNodes, numSharedCoordOnlyNodes,
            autoManageMinMasterNodes ? "auto-managed" : "manual");
        this.nodeConfigurationSource = nodeConfigurationSource;
        numDataPaths = random.nextInt(5) == 0 ? 2 + random.nextInt(3) : 1;
        Builder builder = Settings.builder();
        builder.put(Environment.PATH_HOME_SETTING.getKey(), baseDir);
        builder.put(Environment.PATH_REPO_SETTING.getKey(), baseDir.resolve("repos"));
        builder.put(TransportSettings.PORT.getKey(), 0);
        builder.put("http.port", 0);
        if (Strings.hasLength(System.getProperty("tests.es.logger.level"))) {
            builder.put("logger.level", System.getProperty("tests.es.logger.level"));
        }
        if (Strings.hasLength(System.getProperty("es.logger.prefix"))) {
            builder.put("logger.prefix", System.getProperty("es.logger.prefix"));
        }

        builder.put(TOTAL_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), "100%");

        // Default the watermarks to absurdly low to prevent the tests
        // from failing on nodes without enough disk space
        builder.put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(), "1b");
        builder.put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(), "1b");
        builder.put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.getKey(), "1b");
        if (TEST_NIGHTLY) {
            builder.put(ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_INCOMING_RECOVERIES_SETTING.getKey(),
                    RandomNumbers.randomIntBetween(random, 5, 10));
            builder.put(ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_OUTGOING_RECOVERIES_SETTING.getKey(),
                    RandomNumbers.randomIntBetween(random, 5, 10));
        } else if (random.nextInt(100) <= 90) {
            builder.put(ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_INCOMING_RECOVERIES_SETTING.getKey(),
                    RandomNumbers.randomIntBetween(random, 2, 5));
            builder.put(ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_OUTGOING_RECOVERIES_SETTING.getKey(),
                    RandomNumbers.randomIntBetween(random, 2, 5));
        }
        // always reduce this - it can make tests really slow
        builder.put(RecoverySettings.INDICES_RECOVERY_RETRY_DELAY_STATE_SYNC_SETTING.getKey(), TimeValue.timeValueMillis(
                RandomNumbers.randomIntBetween(random, 20, 50)));
        builder.put(RecoverySettings.INDICES_RECOVERY_MAX_CONCURRENT_FILE_CHUNKS_SETTING.getKey(),
                    RandomNumbers.randomIntBetween(random, 1, 5));
        defaultSettings = builder.build();
        executor = EsExecutors.newScaling("internal_test_cluster_executor", 0, Integer.MAX_VALUE, 0, TimeUnit.SECONDS,
                EsExecutors.daemonThreadFactory("test_" + clusterName), new ThreadContext(Settings.EMPTY));
    }

    /**
     * Sets {@link #bootstrapMasterNodeIndex} to the given value, see {@link #bootstrapMasterNodeWithSpecifiedIndex(List)}
     * for the description of how this field is used.
     * It's only possible to change {@link #bootstrapMasterNodeIndex} value if autoManageMinMasterNodes is false.
     */
    public void setBootstrapMasterNodeIndex(int bootstrapMasterNodeIndex) {
        if (autoManageMinMasterNodes && bootstrapMasterNodeIndex != -1) {
            throw new AssertionError("bootstrapMasterNodeIndex should be -1 if autoManageMinMasterNodes is true");
        }
        this.bootstrapMasterNodeIndex = bootstrapMasterNodeIndex;
    }

    @Override
    public String getClusterName() {
        return clusterName;
    }

    public String[] getNodeNames() {
        return nodes.keySet().toArray(Strings.EMPTY_ARRAY);
    }

    private Settings getSettings(int nodeOrdinal, long nodeSeed, Settings others) {
        Builder builder = Settings.builder().put(defaultSettings)
            .put(getRandomNodeSettings(nodeSeed));
        Settings settings = nodeConfigurationSource.nodeSettings(nodeOrdinal);
        if (settings != null) {
            if (settings.get(ClusterName.CLUSTER_NAME_SETTING.getKey()) != null) {
                throw new IllegalStateException("Tests must not set a '" + ClusterName.CLUSTER_NAME_SETTING.getKey()
                        + "' as a node setting set '" + ClusterName.CLUSTER_NAME_SETTING.getKey() + "': ["
                        + settings.get(ClusterName.CLUSTER_NAME_SETTING.getKey()) + "]");
            }
            builder.put(settings);
        }
        if (others != null) {
            builder.put(others);
        }
        builder.put(ClusterName.CLUSTER_NAME_SETTING.getKey(), clusterName);
        return builder.build();
    }

    public Collection<Class<? extends Plugin>> getPlugins() {
        Set<Class<? extends Plugin>> plugins = new HashSet<>(nodeConfigurationSource.nodePlugins());
        plugins.addAll(mockPlugins);
        return plugins;
    }

    private static Settings getRandomNodeSettings(long seed) {
        Random random = new Random(seed);
        Builder builder = Settings.builder();
        builder.put(TransportSettings.TRANSPORT_COMPRESS.getKey(), rarely(random));
        if (random.nextBoolean()) {
            builder.put("cache.recycler.page.type", RandomPicks.randomFrom(random, PageCacheRecycler.Type.values()));
        }

        builder.put(EsExecutors.PROCESSORS_SETTING.getKey(), 1 + random.nextInt(3));
        if (random.nextBoolean()) {
            if (random.nextBoolean()) {
                builder.put("indices.fielddata.cache.size", 1 + random.nextInt(1000), ByteSizeUnit.MB);
            }
        }

        // randomize tcp settings
        if (random.nextBoolean()) {
            builder.put(TransportSettings.CONNECTIONS_PER_NODE_RECOVERY.getKey(), random.nextInt(2) + 1);
            builder.put(TransportSettings.CONNECTIONS_PER_NODE_BULK.getKey(), random.nextInt(3) + 1);
            builder.put(TransportSettings.CONNECTIONS_PER_NODE_REG.getKey(), random.nextInt(6) + 1);
        }

        if (random.nextBoolean()) {
            builder.put(MappingUpdatedAction.INDICES_MAPPING_DYNAMIC_TIMEOUT_SETTING.getKey(),
                    timeValueSeconds(RandomNumbers.randomIntBetween(random, 10, 30)).getStringRep());
        }

        if (random.nextInt(10) == 0) {
            builder.put(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_TYPE_SETTING.getKey(), "noop");
            builder.put(HierarchyCircuitBreakerService.FIELDDATA_CIRCUIT_BREAKER_TYPE_SETTING.getKey(), "noop");
        }

        if (random.nextBoolean()) {
            if (random.nextInt(10) == 0) { // do something crazy slow here
                builder.put(RecoverySettings.INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING.getKey(),
                        new ByteSizeValue(RandomNumbers.randomIntBetween(random, 1, 10), ByteSizeUnit.MB));
            } else {
                builder.put(RecoverySettings.INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING.getKey(),
                        new ByteSizeValue(RandomNumbers.randomIntBetween(random, 10, 200), ByteSizeUnit.MB));
            }
        }

        if (random.nextBoolean()) {
            builder.put(TransportSettings.PING_SCHEDULE.getKey(), RandomNumbers.randomIntBetween(random, 100, 2000) + "ms");
        }

        return builder.build();
    }

    public static String clusterName(String prefix, long clusterSeed) {
        StringBuilder builder = new StringBuilder(prefix);
        final int childVM = RandomizedTest.systemPropertyAsInt(SysGlobals.CHILDVM_SYSPROP_JVM_ID, 0);
        builder.append("-CHILD_VM=[").append(childVM).append(']');
        builder.append("-CLUSTER_SEED=[").append(clusterSeed).append(']');
        // if multiple maven task run on a single host we better have an identifier that doesn't rely on input params
        builder.append("-HASH=[").append(SeedUtils.formatSeed(System.nanoTime())).append(']');
        return builder.toString();
    }

    private void ensureOpen() {
        if (!open.get()) {
            throw new RuntimeException("Cluster is already closed");
        }
    }

    private NodeAndClient getOrBuildRandomNode() {
        assert Thread.holdsLock(this);
        final NodeAndClient randomNodeAndClient = getRandomNodeAndClient();
        if (randomNodeAndClient != null) {
            return randomNodeAndClient;
        }
        final Runnable onTransportServiceStarted = () -> {}; // do not create unicast host file for this one node.

        final int nodeId = nextNodeId.getAndIncrement();
        final Settings settings = getNodeSettings(nodeId, random.nextLong(), Settings.EMPTY, 1);
        final Settings nodeSettings = Settings.builder()
                .putList(INITIAL_MASTER_NODES_SETTING.getKey(), Node.NODE_NAME_SETTING.get(settings))
                .put(settings)
                .build();
        final NodeAndClient buildNode = buildNode(nodeId, nodeSettings, false, onTransportServiceStarted);
        assert nodes.isEmpty();
        buildNode.startNode();
        publishNode(buildNode);
        return buildNode;
    }

    private NodeAndClient getRandomNodeAndClient() {
        return getRandomNodeAndClient(nc -> true);
    }

    private synchronized NodeAndClient getRandomNodeAndClient(Predicate<NodeAndClient> predicate) {
        ensureOpen();
        List<NodeAndClient> values = nodes.values().stream().filter(predicate).collect(Collectors.toList());
        if (values.isEmpty() == false) {
            return randomFrom(random, values);
        }
        return null;
    }

    /**
     * Ensures that at least <code>n</code> data nodes are present in the cluster.
     * if more nodes than <code>n</code> are present this method will not
     * stop any of the running nodes.
     */
    public synchronized void ensureAtLeastNumDataNodes(int n) {
        int size = numDataNodes();
        if (size < n) {
            logger.info("increasing cluster size from {} to {}", size, n);
            if (numSharedDedicatedMasterNodes > 0) {
                startDataOnlyNodes(n - size);
            } else {
                startNodes(n - size);
            }
            validateClusterFormed();
        }
    }

    /**
     * Ensures that at most <code>n</code> are up and running.
     * If less nodes that <code>n</code> are running this method
     * will not start any additional nodes.
     */
    public synchronized void ensureAtMostNumDataNodes(int n) throws IOException {
        int size = numDataNodes();
        if (size <= n) {
            return;
        }
        // prevent killing the master if possible and client nodes
        final Stream<NodeAndClient> collection = n == 0
                ? nodes.values().stream()
                : nodes.values().stream()
                        .filter(DATA_NODE_PREDICATE.and(new NodeNamePredicate(getMasterName()).negate()));
        final Iterator<NodeAndClient> values = collection.iterator();

        logger.info("changing cluster size from {} data nodes to {}", size, n);
        Set<NodeAndClient> nodesToRemove = new HashSet<>();
        int numNodesAndClients = 0;
        while (values.hasNext() && numNodesAndClients++ < size - n) {
            NodeAndClient next = values.next();
            nodesToRemove.add(next);
        }

        stopNodesAndClients(nodesToRemove);
        if (!nodesToRemove.isEmpty() && size() > 0) {
            validateClusterFormed();
        }
    }

    private Settings getNodeSettings(final int nodeId, final long seed, final Settings extraSettings, final int defaultMinMasterNodes) {
        final Settings settings = getSettings(nodeId, seed, extraSettings);

        final String name = buildNodeName(nodeId, settings);

        final Settings.Builder updatedSettings = Settings.builder();

        updatedSettings.put(Environment.PATH_HOME_SETTING.getKey(), baseDir);

        if (numDataPaths > 1) {
            updatedSettings.putList(Environment.PATH_DATA_SETTING.getKey(), IntStream.range(0, numDataPaths).mapToObj(i ->
                baseDir.resolve(name).resolve("d" + i).toString()).collect(Collectors.toList()));
        } else {
            updatedSettings.put(Environment.PATH_DATA_SETTING.getKey(), baseDir.resolve(name));
        }

        updatedSettings.put(Environment.PATH_SHARED_DATA_SETTING.getKey(), baseDir.resolve(name + "-shared"));

        // allow overriding the above
        updatedSettings.put(settings);
        // force certain settings
        updatedSettings.put("node.name", name);
        updatedSettings.put(NodeEnvironment.NODE_ID_SEED_SETTING.getKey(), seed);

        if (autoManageMinMasterNodes) {
            assertThat("if master nodes are automatically managed then nodes must complete a join cycle when starting",
                updatedSettings.get(INITIAL_STATE_TIMEOUT_SETTING.getKey()), nullValue());
        }

        return updatedSettings.build();
    }

    /**
     * builds a new node
     *
     * @param nodeId                    node ordinal
     * @param settings                  the settings to use
     * @param reuseExisting             if a node with the same name is already part of {@link #nodes}, no new node will be built and
     *                                  the method will return the existing one
     * @param onTransportServiceStarted callback to run when transport service is started
     */
    private synchronized NodeAndClient buildNode(int nodeId, Settings settings,
                                    boolean reuseExisting, Runnable onTransportServiceStarted) {
        assert Thread.holdsLock(this);
        ensureOpen();
        Collection<Class<? extends Plugin>> plugins = getPlugins();
        String name = settings.get("node.name");

        final NodeAndClient nodeAndClient = nodes.get(name);
        if (reuseExisting && nodeAndClient != null) {
            onTransportServiceStarted.run(); // reusing an existing node implies its transport service already started
            return nodeAndClient;
        }
        assert reuseExisting == true || nodeAndClient == null : "node name [" + name + "] already exists but not allowed to use it";

        MockNode node = new MockNode(
                settings,
                plugins,
                nodeConfigurationSource.nodeConfigPath(nodeId),
                forbidPrivateIndexSettings);
        node.injector().getInstance(TransportService.class).addLifecycleListener(new LifecycleListener() {
            @Override
            public void afterStart() {
                onTransportServiceStarted.run();
            }
        });
        return new NodeAndClient(name, node, settings, nodeId);
    }

    private String getNodePrefix(Settings settings) {
        return nodePrefix + getRoleSuffix(settings);
    }

    private String buildNodeName(int id, Settings settings) {
        return getNodePrefix(settings) + id;
    }

    /**
     * returns a suffix string based on the node role. If no explicit role is defined, the suffix will be empty
     */
    private static String getRoleSuffix(Settings settings) {
        String suffix = "";
        if (Node.NODE_MASTER_SETTING.exists(settings) && Node.NODE_MASTER_SETTING.get(settings)) {
            suffix = suffix + Role.MASTER.getAbbreviation();
        }
        if (Node.NODE_DATA_SETTING.exists(settings) && Node.NODE_DATA_SETTING.get(settings)) {
            suffix = suffix + Role.DATA.getAbbreviation();
        }
        if (Node.NODE_MASTER_SETTING.exists(settings) && Node.NODE_MASTER_SETTING.get(settings) == false &&
            Node.NODE_DATA_SETTING.exists(settings) && Node.NODE_DATA_SETTING.get(settings) == false
            ) {
            suffix = suffix + "c";
        }
        return suffix;
    }

    @Override
    public synchronized Client client() {
        ensureOpen();
        /* Randomly return a client to one of the nodes in the cluster */
        return getOrBuildRandomNode().client(random);
    }

    /**
     * Returns a node client to a data node in the cluster.
     * Note: use this with care tests should not rely on a certain nodes client.
     */
    public Client dataNodeClient() {
        /* Randomly return a client to one of the nodes in the cluster */
        return getRandomNodeAndClient(DATA_NODE_PREDICATE).client(random);
    }

    /**
     * Returns a node client to the current master node.
     * Note: use this with care tests should not rely on a certain nodes client.
     */
    public Client masterClient() {
        NodeAndClient randomNodeAndClient = getRandomNodeAndClient(new NodeNamePredicate(getMasterName()));
        if (randomNodeAndClient != null) {
            return randomNodeAndClient.nodeClient(); // ensure node client master is requested
        }
        throw new AssertionError("No master client found");
    }

    /**
     * Returns a node client to random node but not the master. This method will fail if no non-master client is available.
     */
    public Client nonMasterClient() {
        NodeAndClient randomNodeAndClient = getRandomNodeAndClient(new NodeNamePredicate(getMasterName()).negate());
        if (randomNodeAndClient != null) {
            return randomNodeAndClient.nodeClient(); // ensure node client non-master is requested
        }
        throw new AssertionError("No non-master client found");
    }

    /**
     * Returns a client to a coordinating only node
     */
    public synchronized Client coordOnlyNodeClient() {
        ensureOpen();
        NodeAndClient randomNodeAndClient = getRandomNodeAndClient(NO_DATA_NO_MASTER_PREDICATE);
        if (randomNodeAndClient != null) {
            return randomNodeAndClient.client(random);
        }
        int nodeId = nextNodeId.getAndIncrement();
        Settings settings = getSettings(nodeId, random.nextLong(), Settings.EMPTY);
        startCoordinatingOnlyNode(settings);
        return getRandomNodeAndClient(NO_DATA_NO_MASTER_PREDICATE).client(random);
    }

    public synchronized String startCoordinatingOnlyNode(Settings settings) {
        ensureOpen(); // currently unused
        Builder builder = Settings.builder().put(settings).put(Node.NODE_MASTER_SETTING.getKey(), false)
            .put(Node.NODE_DATA_SETTING.getKey(), false);
        return startNode(builder);
    }

    /**
     * Returns a node client to a given node.
     */
    public Client client(String nodeName) {
        NodeAndClient nodeAndClient = nodes.get(nodeName);
        if (nodeAndClient != null) {
            return nodeAndClient.client(random);
        }
        throw new AssertionError("No node found with name: [" + nodeName + "]");
    }


    /**
     * Returns a "smart" node client to a random node in the cluster
     */
    public Client smartClient() {
        NodeAndClient randomNodeAndClient = getRandomNodeAndClient();
        if (randomNodeAndClient != null) {
            return randomNodeAndClient.nodeClient();
        }
        throw new AssertionError("No smart client found");
    }

    @Override
    public synchronized void close() throws IOException {
        if (this.open.compareAndSet(true, false)) {
            if (activeDisruptionScheme != null) {
                activeDisruptionScheme.testClusterClosed();
                activeDisruptionScheme = null;
            }
            try {
                IOUtils.close(nodes.values());
            } finally {
                nodes = Collections.emptyNavigableMap();
                executor.shutdownNow();
            }
        }
    }

    private final class NodeAndClient implements Closeable {
        private MockNode node;
        private final Settings originalNodeSettings;
        private Client nodeClient;
        private Client transportClient;
        private final AtomicBoolean closed = new AtomicBoolean(false);
        private final String name;
        private final int nodeAndClientId;

        NodeAndClient(String name, MockNode node, Settings originalNodeSettings, int nodeAndClientId) {
            this.node = node;
            this.name = name;
            this.originalNodeSettings = originalNodeSettings;
            this.nodeAndClientId = nodeAndClientId;
            markNodeDataDirsAsNotEligibleForWipe(node);
        }

        Node node() {
            if (closed.get()) {
                throw new RuntimeException("already closed");
            }
            return node;
        }

        public int nodeAndClientId() {
            return nodeAndClientId;
        }

        public String getName() {
            return name;
        }

        public boolean isMasterEligible() {
            return Node.NODE_MASTER_SETTING.get(node.settings());
        }

        Client client(Random random) {
            return getOrBuildNodeClient();
        }

        Client nodeClient() {
            if (closed.get()) {
                throw new RuntimeException("already closed");
            }
            return getOrBuildNodeClient();
        }

        private Client getOrBuildNodeClient() {
            synchronized (InternalTestCluster.this) {
                if (closed.get()) {
                    throw new RuntimeException("already closed");
                }
                if (nodeClient == null) {
                    nodeClient = node.client();
                }
                return clientWrapper.apply(nodeClient);
            }
        }

        void resetClient() {
            if (closed.get() == false) {
                Releasables.close(nodeClient, transportClient);
                nodeClient = null;
                transportClient = null;
            }
        }

        void startNode() {
            try {
                node.start();
            } catch (NodeValidationException e) {
                throw new RuntimeException(e);
            }
        }

        /**
         * closes the node and prepares it to be restarted
         */
        Settings closeForRestart(RestartCallback callback, int minMasterNodes) throws Exception {
            assert callback != null;
            close();
            Settings callbackSettings = callback.onNodeStopped(name);
            assert callbackSettings != null;
            Settings.Builder newSettings = Settings.builder();
            newSettings.put(callbackSettings);
            if (minMasterNodes >= 0) {
                if (INITIAL_MASTER_NODES_SETTING.exists(callbackSettings) == false) {
                    newSettings.putList(INITIAL_MASTER_NODES_SETTING.getKey());
                }
            }
            // delete data folders now, before we start other nodes that may claim it
            clearDataIfNeeded(callback);
            return newSettings.build();
        }

        private void clearDataIfNeeded(RestartCallback callback) throws IOException {
            if (callback.clearData(name)) {
                NodeEnvironment nodeEnv = node.getNodeEnvironment();
                if (nodeEnv.hasNodeFile()) {
                    final Path[] locations = nodeEnv.nodeDataPaths();
                    logger.debug("removing node data paths: [{}]", Arrays.toString(locations));
                    IOUtils.rm(locations);
                }
            }
        }

        private void recreateNode(final Settings newSettings, final Runnable onTransportServiceStarted) {
            if (closed.get() == false) {
                throw new IllegalStateException("node " + name + " should be closed before recreating it");
            }
            // use a new seed to make sure we generate a fresh new node id if the data folder has been wiped
            final long newIdSeed = NodeEnvironment.NODE_ID_SEED_SETTING.get(node.settings()) + 1;
            Settings finalSettings = Settings.builder()
                    .put(originalNodeSettings)
                    .put(newSettings)
                    .put(NodeEnvironment.NODE_ID_SEED_SETTING.getKey(), newIdSeed)
                    .build();
            Collection<Class<? extends Plugin>> plugins = node.getClasspathPlugins();
            node = new MockNode(finalSettings, plugins);
            node.injector().getInstance(TransportService.class).addLifecycleListener(new LifecycleListener() {
                @Override
                public void afterStart() {
                    onTransportServiceStarted.run();
                }
            });
            closed.set(false);
            markNodeDataDirsAsNotEligibleForWipe(node);
        }

        @Override
        public void close() throws IOException {
            assert Thread.holdsLock(InternalTestCluster.this);
            try {
                resetClient();
            } finally {
                closed.set(true);
                markNodeDataDirsAsPendingForWipe(node);
                node.close();
            }
        }

        private void markNodeDataDirsAsPendingForWipe(Node node) {
            assert Thread.holdsLock(InternalTestCluster.this);
            NodeEnvironment nodeEnv = node.getNodeEnvironment();
            if (nodeEnv.hasNodeFile()) {
                dataDirToClean.addAll(Arrays.asList(nodeEnv.nodeDataPaths()));
            }
        }

        private void markNodeDataDirsAsNotEligibleForWipe(Node node) {
            assert Thread.holdsLock(InternalTestCluster.this);
            NodeEnvironment nodeEnv = node.getNodeEnvironment();
            if (nodeEnv.hasNodeFile()) {
                dataDirToClean.removeAll(Arrays.asList(nodeEnv.nodeDataPaths()));
            }
        }
    }

    @Override
    public void beforeTest(Random random) throws IOException, InterruptedException {
        super.beforeTest(random);
        reset(true);
    }

    private synchronized void reset(boolean wipeData) throws IOException {
        // clear all rules for mock transport services
        for (NodeAndClient nodeAndClient : nodes.values()) {
            TransportService transportService = nodeAndClient.node.injector().getInstance(TransportService.class);
            if (transportService instanceof MockTransportService) {
                final MockTransportService mockTransportService = (MockTransportService) transportService;
                mockTransportService.clearAllRules();
            }
        }
        randomlyResetClients();
        final int newSize = sharedNodesSeeds.length;
        if (nextNodeId.get() == newSize && nodes.size() == newSize) {
            if (wipeData) {
                wipePendingDataDirectories();
            }
            logger.debug("Cluster hasn't changed - moving out - nodes: [{}] nextNodeId: [{}] numSharedNodes: [{}]",
                    nodes.keySet(), nextNodeId.get(), newSize);
            return;
        }
        logger.debug("Cluster is NOT consistent - restarting shared nodes - nodes: [{}] nextNodeId: [{}] numSharedNodes: [{}]",
                nodes.keySet(), nextNodeId.get(), newSize);

        // trash all nodes with id >= sharedNodesSeeds.length - they are non shared
        final List<NodeAndClient> toClose = new ArrayList<>();
        for (NodeAndClient nodeAndClient : nodes.values()) {
            if (nodeAndClient.nodeAndClientId() >= sharedNodesSeeds.length) {
                logger.debug("Close Node [{}] not shared", nodeAndClient.name);
                toClose.add(nodeAndClient);
            }
        }
        stopNodesAndClients(toClose);

        // clean up what the nodes left that is unused
        if (wipeData) {
            wipePendingDataDirectories();
        }

        assertTrue("expected at least one master-eligible node left in " + nodes,
            nodes.isEmpty() || nodes.values().stream().anyMatch(NodeAndClient::isMasterEligible));

        final int prevNodeCount = nodes.size();

        // start any missing node
        assert newSize == numSharedDedicatedMasterNodes + numSharedDataNodes + numSharedCoordOnlyNodes;
        final int numberOfMasterNodes = numSharedDedicatedMasterNodes > 0 ? numSharedDedicatedMasterNodes : numSharedDataNodes;
        final int defaultMinMasterNodes = (numberOfMasterNodes / 2) + 1;
        final List<NodeAndClient> toStartAndPublish = new ArrayList<>(); // we want to start nodes in one go due to min master nodes
        final Runnable onTransportServiceStarted = () -> rebuildUnicastHostFiles(toStartAndPublish);

        final List<Settings> settings = new ArrayList<>();

        for (int i = 0; i < numSharedDedicatedMasterNodes; i++) {
            final Settings.Builder extraSettings = Settings.builder();
            extraSettings.put(Node.NODE_MASTER_SETTING.getKey(), true);
            extraSettings.put(Node.NODE_DATA_SETTING.getKey(), false);
            settings.add(getNodeSettings(i, sharedNodesSeeds[i], extraSettings.build(), defaultMinMasterNodes));
        }
        for (int i = numSharedDedicatedMasterNodes; i < numSharedDedicatedMasterNodes + numSharedDataNodes; i++) {
            final Settings.Builder extraSettings = Settings.builder();
            if (numSharedDedicatedMasterNodes > 0) {
                // if we don't have dedicated master nodes, keep things default
                extraSettings.put(Node.NODE_MASTER_SETTING.getKey(), false).build();
                extraSettings.put(Node.NODE_DATA_SETTING.getKey(), true).build();
            }
            settings.add(getNodeSettings(i, sharedNodesSeeds[i], extraSettings.build(), defaultMinMasterNodes));
        }
        for (int i = numSharedDedicatedMasterNodes + numSharedDataNodes;
             i < numSharedDedicatedMasterNodes + numSharedDataNodes + numSharedCoordOnlyNodes; i++) {
            final Builder extraSettings = Settings.builder().put(Node.NODE_MASTER_SETTING.getKey(), false)
                .put(Node.NODE_DATA_SETTING.getKey(), false);
            settings.add(getNodeSettings(i, sharedNodesSeeds[i], extraSettings.build(), defaultMinMasterNodes));
        }

        int autoBootstrapMasterNodeIndex = -1;
        final List<String> masterNodeNames = settings.stream()
                .filter(Node.NODE_MASTER_SETTING::get)
                .map(Node.NODE_NAME_SETTING::get)
                .collect(Collectors.toList());

        if (prevNodeCount == 0 && autoManageMinMasterNodes) {
            if (numSharedDedicatedMasterNodes > 0) {
                autoBootstrapMasterNodeIndex = RandomNumbers.randomIntBetween(random, 0, numSharedDedicatedMasterNodes - 1);
            } else if (numSharedDataNodes > 0) {
                autoBootstrapMasterNodeIndex = RandomNumbers.randomIntBetween(random, 0, numSharedDataNodes - 1);
            }
        }

        final List<Settings> updatedSettings = bootstrapMasterNodeWithSpecifiedIndex(settings);

        for (int i = 0; i < numSharedDedicatedMasterNodes + numSharedDataNodes + numSharedCoordOnlyNodes; i++) {
            Settings nodeSettings = updatedSettings.get(i);
            if (i == autoBootstrapMasterNodeIndex) {
                nodeSettings = Settings.builder().putList(INITIAL_MASTER_NODES_SETTING.getKey(), masterNodeNames).put(nodeSettings).build();
            }
            final NodeAndClient nodeAndClient = buildNode(i, nodeSettings, true, onTransportServiceStarted);
            toStartAndPublish.add(nodeAndClient);
        }

        startAndPublishNodesAndClients(toStartAndPublish);

        nextNodeId.set(newSize);
        assert size() == newSize;
        if (autoManageMinMasterNodes && newSize > 0) {
            validateClusterFormed();
        }
        logger.debug("Cluster is consistent again - nodes: [{}] nextNodeId: [{}] numSharedNodes: [{}]",
                nodes.keySet(), nextNodeId.get(), newSize);
    }

    /** ensure a cluster is formed with all published nodes. */
    public synchronized void validateClusterFormed() {
        String name = randomFrom(random, getNodeNames());
        validateClusterFormed(name);
    }

    /** ensure a cluster is formed with all published nodes, but do so by using the client of the specified node */
    private synchronized void validateClusterFormed(String viaNode) {
        Set<DiscoveryNode> expectedNodes = new HashSet<>();
        for (NodeAndClient nodeAndClient : nodes.values()) {
            expectedNodes.add(getInstanceFromNode(ClusterService.class, nodeAndClient.node()).localNode());
        }
        logger.trace("validating cluster formed via [{}], expecting {}", viaNode, expectedNodes);
        final Client client = client(viaNode);
        try {
            if (awaitBusy(() -> {
                DiscoveryNodes discoveryNodes = client.admin().cluster().prepareState().get().getState().nodes();
                if (discoveryNodes.getSize() != expectedNodes.size()) {
                    return false;
                }
                for (DiscoveryNode expectedNode : expectedNodes) {
                    if (discoveryNodes.nodeExists(expectedNode) == false) {
                        return false;
                    }
                }
                return true;
            }, 30, TimeUnit.SECONDS) == false) {
                throw new IllegalStateException("cluster failed to form with expected nodes " + expectedNodes + " and actual nodes " +
                    client.admin().cluster().prepareState().get().getState().nodes());
            }
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public synchronized void afterTest() {
        wipePendingDataDirectories();
        randomlyResetClients(); /* reset all clients - each test gets its own client based on the Random instance created above. */
    }

    @Override
    public void beforeIndexDeletion() throws Exception {
        // Check that the operations counter on index shard has reached 0.
        // The assumption here is that after a test there are no ongoing write operations.
        // test that have ongoing write operations after the test (for example because ttl is used
        // and not all docs have been purged after the test) and inherit from
        // ElasticsearchIntegrationTest must override beforeIndexDeletion() to avoid failures.
        assertNoPendingIndexOperations();
        //check that shards that have same sync id also contain same number of documents
        assertSameSyncIdSameDocs();
        assertOpenTranslogReferences();
    }

    private void assertSameSyncIdSameDocs() {
        Map<String, Long> docsOnShards = new HashMap<>();
        final Collection<NodeAndClient> nodesAndClients = nodes.values();
        for (NodeAndClient nodeAndClient : nodesAndClients) {
            IndicesService indexServices = getInstance(IndicesService.class, nodeAndClient.name);
            for (IndexService indexService : indexServices) {
                for (IndexShard indexShard : indexService) {
                    try {
                        CommitStats commitStats = indexShard.commitStats();
                        String syncId = commitStats.getUserData().get(Engine.SYNC_COMMIT_ID);
                        if (syncId != null) {
                            long liveDocsOnShard = commitStats.getNumDocs();
                            if (docsOnShards.get(syncId) != null) {
                                assertThat("sync id is equal but number of docs does not match on node "
                                    + nodeAndClient.name + ". expected " + docsOnShards.get(syncId) + " but got "
                                    + liveDocsOnShard, docsOnShards.get(syncId), equalTo(liveDocsOnShard));
                            } else {
                                docsOnShards.put(syncId, liveDocsOnShard);
                            }
                        }
                    } catch (AlreadyClosedException e) {
                        // the engine is closed or if the shard is recovering
                    }
                }
            }
        }
    }

    private void assertNoPendingIndexOperations() throws Exception {
        assertBusy(() -> {
            for (NodeAndClient nodeAndClient : nodes.values()) {
                IndicesService indexServices = getInstance(IndicesService.class, nodeAndClient.name);
                for (IndexService indexService : indexServices) {
                    for (IndexShard indexShard : indexService) {
                        List<String> operations = indexShard.getActiveOperations();
                        if (operations.size() > 0) {
                            throw new AssertionError(
                                "shard " + indexShard.shardId() + " on node [" + nodeAndClient.name + "] has pending operations:\n --> " +
                                    String.join("\n --> ", operations)
                            );
                        }
                    }
                }
            }
        });
    }

    private void assertOpenTranslogReferences() throws Exception {
        assertBusy(() -> {
            for (NodeAndClient nodeAndClient : nodes.values()) {
                IndicesService indexServices = getInstance(IndicesService.class, nodeAndClient.name);
                for (IndexService indexService : indexServices) {
                    for (IndexShard indexShard : indexService) {
                        try {
                            if (IndexShardTestCase.getEngine(indexShard) instanceof InternalEngine) {
                                IndexShardTestCase.getTranslog(indexShard).getDeletionPolicy().assertNoOpenTranslogRefs();
                            }
                        } catch (AlreadyClosedException ok) {
                            // all good
                        }
                    }
                }
            }
        });
    }

    /**
     * Asserts that the document history in Lucene index is consistent with Translog's on every index shard of the cluster.
     * This assertion might be expensive, thus we prefer not to execute on every test but only interesting tests.
     */
    public void assertConsistentHistoryBetweenTranslogAndLuceneIndex() throws IOException {
        for (NodeAndClient nodeAndClient : nodes.values()) {
            IndicesService indexServices = getInstance(IndicesService.class, nodeAndClient.name);
            for (IndexService indexService : indexServices) {
                for (IndexShard indexShard : indexService) {
                    try {
                        IndexShardTestCase.assertConsistentHistoryBetweenTranslogAndLucene(indexShard);
                    } catch (AlreadyClosedException ignored) {
                        // shard is closed
                    }
                }
            }
        }
    }

    private IndexShard getShardOrNull(ClusterState clusterState, ShardRouting shardRouting) {
        if (shardRouting == null || shardRouting.assignedToNode() == false) {
            return null;
        }
        final DiscoveryNode assignedNode = clusterState.nodes().get(shardRouting.currentNodeId());
        if (assignedNode == null) {
            return null;
        }
        return getInstance(IndicesService.class, assignedNode.getName()).getShardOrNull(shardRouting.shardId());
    }

    public void assertSeqNos() throws Exception {
        assertBusy(() -> {
            final ClusterState state = clusterService().state();
            for (ObjectObjectCursor<String, IndexRoutingTable> indexRoutingTable : state.routingTable().indicesRouting()) {
                for (IntObjectCursor<IndexShardRoutingTable> indexShardRoutingTable : indexRoutingTable.value.shards()) {
                    ShardRouting primaryShardRouting = indexShardRoutingTable.value.primaryShard();
                    final IndexShard primaryShard = getShardOrNull(state, primaryShardRouting);
                    if (primaryShard == null) {
                        continue; //just ignore - shard movement
                    }
                    final SeqNoStats primarySeqNoStats;
                    final ObjectLongMap<String> syncGlobalCheckpoints;
                    try {
                        primarySeqNoStats = primaryShard.seqNoStats();
                        syncGlobalCheckpoints = primaryShard.getInSyncGlobalCheckpoints();
                    } catch (AlreadyClosedException ex) {
                        continue; // shard is closed - just ignore
                    }
                    assertThat(primaryShardRouting + " should have set the global checkpoint",
                        primarySeqNoStats.getGlobalCheckpoint(), not(equalTo(SequenceNumbers.UNASSIGNED_SEQ_NO)));
                    for (ShardRouting replicaShardRouting : indexShardRoutingTable.value.replicaShards()) {
                        final IndexShard replicaShard = getShardOrNull(state, replicaShardRouting);
                        if (replicaShard == null) {
                            continue; //just ignore - shard movement
                        }
                        final SeqNoStats seqNoStats;
                        try {
                            seqNoStats = replicaShard.seqNoStats();
                        } catch (AlreadyClosedException e) {
                            continue; // shard is closed - just ignore
                        }
                        assertThat(replicaShardRouting + " seq_no_stats mismatch", seqNoStats, equalTo(primarySeqNoStats));
                        // the local knowledge on the primary of the global checkpoint equals the global checkpoint on the shard
                        assertThat(replicaShardRouting + " global checkpoint syncs mismatch", seqNoStats.getGlobalCheckpoint(),
                            equalTo(syncGlobalCheckpoints.get(replicaShardRouting.allocationId().getId())));
                    }
                }
            }
        });
    }

    /**
     * Asserts that all shards with the same shardId should have document Ids.
     */
    public void assertSameDocIdsOnShards() throws Exception {
        assertBusy(() -> {
            ClusterState state = client().admin().cluster().prepareState().get().getState();
            for (ObjectObjectCursor<String, IndexRoutingTable> indexRoutingTable : state.routingTable().indicesRouting()) {
                for (IntObjectCursor<IndexShardRoutingTable> indexShardRoutingTable : indexRoutingTable.value.shards()) {
                    ShardRouting primaryShardRouting = indexShardRoutingTable.value.primaryShard();
                    IndexShard primaryShard = getShardOrNull(state, primaryShardRouting);
                    if (primaryShard == null) {
                        continue;
                    }
                    final List<DocIdSeqNoAndSource> docsOnPrimary;
                    try {
                        docsOnPrimary = IndexShardTestCase.getDocIdAndSeqNos(primaryShard);
                    } catch (AlreadyClosedException ex) {
                        continue;
                    }
                    for (ShardRouting replicaShardRouting : indexShardRoutingTable.value.replicaShards()) {
                        IndexShard replicaShard = getShardOrNull(state, replicaShardRouting);
                        if (replicaShard == null) {
                            continue;
                        }
                        final List<DocIdSeqNoAndSource> docsOnReplica;
                        try {
                            docsOnReplica = IndexShardTestCase.getDocIdAndSeqNos(replicaShard);
                        } catch (AlreadyClosedException ex) {
                            continue;
                        }
                        assertThat("out of sync shards: primary=[" + primaryShardRouting + "] num_docs_on_primary=[" + docsOnPrimary.size()
                                + "] vs replica=[" + replicaShardRouting + "] num_docs_on_replica=[" + docsOnReplica.size() + "]",
                            docsOnReplica, equalTo(docsOnPrimary));
                    }
                }
            }
        });
    }

    private void randomlyResetClients() {
        assert Thread.holdsLock(this);
        // only reset the clients on nightly tests, it causes heavy load...
        if (RandomizedTest.isNightly() && rarely(random)) {
            final Collection<NodeAndClient> nodesAndClients = nodes.values();
            for (NodeAndClient nodeAndClient : nodesAndClients) {
                nodeAndClient.resetClient();
            }
        }
    }

    public synchronized void wipePendingDataDirectories() {
        if (!dataDirToClean.isEmpty()) {
            try {
                for (Path path : dataDirToClean) {
                    try {
                        FileSystemUtils.deleteSubDirectories(path);
                        logger.info("Successfully wiped data directory for node location: {}", path);
                    } catch (IOException e) {
                        logger.info("Failed to wipe data directory for node location: {}", path);
                    }
                }
            } finally {
                dataDirToClean.clear();
            }
        }
    }

    /**
     * Returns a reference to a random node's {@link ClusterService}
     */
    public ClusterService clusterService() {
        return clusterService(null);
    }

    /**
     * Returns a reference to a node's {@link ClusterService}. If the given node is null, a random node will be selected.
     */
    public ClusterService clusterService(@Nullable String node) {
        return getInstance(ClusterService.class, node);
    }

    /**
     * Returns an Iterable to all instances for the given class &gt;T&lt; across all nodes in the cluster.
     */
    public <T> Iterable<T> getInstances(Class<T> clazz) {
        return nodes.values().stream().map(node -> getInstanceFromNode(clazz, node.node)).collect(Collectors.toList());
    }

    /**
     * Returns an Iterable to all instances for the given class &gt;T&lt; across all data nodes in the cluster.
     */
    public <T> Iterable<T> getDataNodeInstances(Class<T> clazz) {
        return getInstances(clazz, DATA_NODE_PREDICATE);
    }

    public synchronized <T> T getCurrentMasterNodeInstance(Class<T> clazz) {
        return getInstance(clazz, new NodeNamePredicate(getMasterName()));
    }

    /**
     * Returns an Iterable to all instances for the given class &gt;T&lt; across all data and master nodes
     * in the cluster.
     */
    public <T> Iterable<T> getDataOrMasterNodeInstances(Class<T> clazz) {
        return getInstances(clazz, DATA_NODE_PREDICATE.or(MASTER_NODE_PREDICATE));
    }

    private <T> Iterable<T> getInstances(Class<T> clazz, Predicate<NodeAndClient> predicate) {
        Iterable<NodeAndClient> filteredNodes = nodes.values().stream().filter(predicate)::iterator;
        List<T> instances = new ArrayList<>();
        for (NodeAndClient nodeAndClient : filteredNodes) {
            instances.add(getInstanceFromNode(clazz, nodeAndClient.node));
        }
        return instances;
    }

    /**
     * Returns a reference to the given nodes instances of the given class &gt;T&lt;
     */
    public <T> T getInstance(Class<T> clazz, final String node) {
        return getInstance(clazz, nc -> node == null || node.equals(nc.name));
    }

    public <T> T getDataNodeInstance(Class<T> clazz) {
        return getInstance(clazz, DATA_NODE_PREDICATE);
    }

    public <T> T getMasterNodeInstance(Class<T> clazz) {
        return getInstance(clazz, MASTER_NODE_PREDICATE);
    }

    private synchronized <T> T getInstance(Class<T> clazz, Predicate<NodeAndClient> predicate) {
        NodeAndClient randomNodeAndClient = getRandomNodeAndClient(predicate);
        assert randomNodeAndClient != null;
        return getInstanceFromNode(clazz, randomNodeAndClient.node);
    }

    /**
     * Returns a reference to a random nodes instances of the given class &gt;T&lt;
     */
    public <T> T getInstance(Class<T> clazz) {
        return getInstance(clazz, nc -> true);
    }

    private static <T> T getInstanceFromNode(Class<T> clazz, Node node) {
        return node.injector().getInstance(clazz);
    }

    public Settings dataPathSettings(String node) {
        return nodes.values()
            .stream()
            .filter(nc -> nc.name.equals(node))
            .findFirst().get().node().settings()
            .filter(key -> key.equals(Environment.PATH_DATA_SETTING.getKey()) ||  key.equals(Environment.PATH_SHARED_DATA_SETTING.getKey()));
    }


    @Override
    public int size() {
        return nodes.size();
    }

    @Override
    public InetSocketAddress[] httpAddresses() {
        List<InetSocketAddress> addresses = new ArrayList<>();
        for (HttpServerTransport httpServerTransport : getInstances(HttpServerTransport.class)) {
            addresses.add(httpServerTransport.boundAddress().publishAddress().address());
        }
        return addresses.toArray(new InetSocketAddress[addresses.size()]);
    }

    /**
     * Stops a random data node in the cluster. Returns true if a node was found to stop, false otherwise.
     */
    public synchronized boolean stopRandomDataNode() throws IOException {
        ensureOpen();
        NodeAndClient nodeAndClient = getRandomNodeAndClient(DATA_NODE_PREDICATE);
        if (nodeAndClient != null) {
            logger.info("Closing random node [{}] ", nodeAndClient.name);
            stopNodesAndClient(nodeAndClient);
            return true;
        }
        return false;
    }

    /**
     * Stops a random node in the cluster that applies to the given filter or non if the non of the nodes applies to the
     * filter.
     */
    public synchronized void stopRandomNode(final Predicate<Settings> filter) throws IOException {
        ensureOpen();
        NodeAndClient nodeAndClient = getRandomNodeAndClient(nc -> filter.test(nc.node.settings()));
        if (nodeAndClient != null) {
            logger.info("Closing filtered random node [{}] ", nodeAndClient.name);
            stopNodesAndClient(nodeAndClient);
        }
    }

    /**
     * Stops the current master node forcefully
     */
    public synchronized void stopCurrentMasterNode() throws IOException {
        ensureOpen();
        assert size() > 0;
        String masterNodeName = getMasterName();
        final NodeAndClient masterNode = nodes.get(masterNodeName);
        assert masterNode != null;
        logger.info("Closing master node [{}] ", masterNodeName);
        stopNodesAndClient(masterNode);
    }

    /**
     * Stops any of the current nodes but not the master node.
     */
    public synchronized void stopRandomNonMasterNode() throws IOException {
        NodeAndClient nodeAndClient = getRandomNodeAndClient(new NodeNamePredicate(getMasterName()).negate());
        if (nodeAndClient != null) {
            logger.info("Closing random non master node [{}] current master [{}] ", nodeAndClient.name, getMasterName());
            stopNodesAndClient(nodeAndClient);
        }
    }

    private synchronized void startAndPublishNodesAndClients(List<NodeAndClient> nodeAndClients) {
        if (nodeAndClients.size() > 0) {
            final int newMasters = (int) nodeAndClients.stream().filter(NodeAndClient::isMasterEligible)
                .filter(nac -> nodes.containsKey(nac.name) == false) // filter out old masters
                .count();
            final int currentMasters = getMasterNodesCount();
            rebuildUnicastHostFiles(nodeAndClients); // ensure that new nodes can find the existing nodes when they start
            List<Future<?>> futures = nodeAndClients.stream().map(node -> executor.submit(node::startNode)).collect(Collectors.toList());

            try {
                for (Future<?> future : futures) {
                    future.get();
                }
            } catch (InterruptedException e) {
                throw new AssertionError("interrupted while starting nodes", e);
            } catch (ExecutionException e) {
                throw new RuntimeException("failed to start nodes", e);
            }
            nodeAndClients.forEach(this::publishNode);

            if (autoManageMinMasterNodes && currentMasters > 0 && newMasters > 0 &&
                getMinMasterNodes(currentMasters + newMasters) > currentMasters) {
                // update once masters have joined
                validateClusterFormed();
            }
        }
    }

    private final Object discoveryFileMutex = new Object();

    private void rebuildUnicastHostFiles(List<NodeAndClient> newNodes) {
        // cannot be a synchronized method since it's called on other threads from within synchronized startAndPublishNodesAndClients()
        synchronized (discoveryFileMutex) {
            try {
                final Collection<NodeAndClient> currentNodes = nodes.values();
                Stream<NodeAndClient> unicastHosts = Stream.concat(currentNodes.stream(), newNodes.stream());
                List<String> discoveryFileContents = unicastHosts.map(
                    nac -> nac.node.injector().getInstance(TransportService.class)
                ).filter(Objects::nonNull)
                    .map(TransportService::getLocalNode).filter(Objects::nonNull).filter(DiscoveryNode::isMasterNode)
                    .map(n -> n.getAddress().toString())
                    .distinct().collect(Collectors.toList());
                Set<Path> configPaths = Stream.concat(currentNodes.stream(), newNodes.stream())
                    .map(nac -> nac.node.getEnvironment().configFile()).collect(Collectors.toSet());
                logger.debug("configuring discovery with {} at {}", discoveryFileContents, configPaths);
                for (final Path configPath : configPaths) {
                    Files.createDirectories(configPath);
                    Files.write(configPath.resolve(UNICAST_HOSTS_FILE), discoveryFileContents);
                }
            } catch (IOException e) {
                throw new AssertionError("failed to configure file-based discovery", e);
            }
        }
    }

    private void stopNodesAndClient(NodeAndClient nodeAndClient) throws IOException {
        stopNodesAndClients(Collections.singleton(nodeAndClient));
    }

    private synchronized void stopNodesAndClients(Collection<NodeAndClient> nodeAndClients) throws IOException {
        final Set<String> excludedNodeIds = excludeMasters(nodeAndClients);

        for (NodeAndClient nodeAndClient: nodeAndClients) {
            removeDisruptionSchemeFromNode(nodeAndClient);
            final NodeAndClient previous = removeNode(nodeAndClient);
            assert previous == nodeAndClient;
            nodeAndClient.close();
        }

        removeExclusions(excludedNodeIds);
    }

    /**
     * Restarts a random data node in the cluster
     */
    public void restartRandomDataNode() throws Exception {
        restartRandomDataNode(EMPTY_CALLBACK);
    }

    /**
     * Restarts a random data node in the cluster and calls the callback during restart.
     */
    public synchronized void restartRandomDataNode(RestartCallback callback) throws Exception {
        ensureOpen();
        NodeAndClient nodeAndClient = getRandomNodeAndClient(InternalTestCluster.DATA_NODE_PREDICATE);
        if (nodeAndClient != null) {
            restartNode(nodeAndClient, callback);
        }
    }

    /**
     * Restarts a node and calls the callback during restart.
     */
    public synchronized void restartNode(String nodeName, RestartCallback callback) throws Exception {
        ensureOpen();
        NodeAndClient nodeAndClient = nodes.get(nodeName);
        if (nodeAndClient != null) {
            restartNode(nodeAndClient, callback);
        }
    }

    public static final RestartCallback EMPTY_CALLBACK = new RestartCallback();

    /**
     * Restarts all nodes in the cluster. It first stops all nodes and then restarts all the nodes again.
     */
    public void fullRestart() throws Exception {
        fullRestart(EMPTY_CALLBACK);
    }

    /**
     * Restarts all nodes in a rolling restart fashion ie. only restarts on node a time.
     */
    public synchronized void rollingRestart(RestartCallback callback) throws Exception {
        int numNodesRestarted = 0;
        for (NodeAndClient nodeAndClient : nodes.values()) {
            callback.doAfterNodes(numNodesRestarted++, nodeAndClient.nodeClient());
            restartNode(nodeAndClient, callback);
        }
    }

    private void restartNode(NodeAndClient nodeAndClient, RestartCallback callback) throws Exception {
        assert Thread.holdsLock(this);
        logger.info("Restarting node [{}] ", nodeAndClient.name);

        if (activeDisruptionScheme != null) {
            activeDisruptionScheme.removeFromNode(nodeAndClient.name, this);
        }

        Set<String> excludedNodeIds = excludeMasters(Collections.singleton(nodeAndClient));

        final Settings newSettings = nodeAndClient.closeForRestart(callback,
                autoManageMinMasterNodes ? getMinMasterNodes(getMasterNodesCount()) : -1);

        removeExclusions(excludedNodeIds);

        boolean success = false;
        try {
            nodeAndClient.recreateNode(newSettings, () -> rebuildUnicastHostFiles(emptyList()));
            nodeAndClient.startNode();
            success = true;
        } finally {
            if (success == false) {
                removeNode(nodeAndClient);
            }
        }

        if (activeDisruptionScheme != null) {
            activeDisruptionScheme.applyToNode(nodeAndClient.name, this);
        }

        if (callback.validateClusterForming() || excludedNodeIds.isEmpty() == false) {
            // we have to validate cluster size if updateMinMaster == true, because we need the
            // second node to join in order to increment min_master_nodes back to 2.
            // we also have to do via the node that was just restarted as it may be that the master didn't yet process
            // the fact it left
            validateClusterFormed(nodeAndClient.name);
        }
    }

    private NodeAndClient removeNode(NodeAndClient nodeAndClient) {
        assert Thread.holdsLock(this);
        final NavigableMap<String, NodeAndClient> newNodes = new TreeMap<>(nodes);
        final NodeAndClient previous = newNodes.remove(nodeAndClient.name);
        nodes = Collections.unmodifiableNavigableMap(newNodes);
        return previous;
    }

    private Set<String> excludeMasters(Collection<NodeAndClient> nodeAndClients) {
        assert Thread.holdsLock(this);
        final Set<String> excludedNodeIds = new HashSet<>();
        if (autoManageMinMasterNodes && nodeAndClients.size() > 0) {

            final long currentMasters = nodes.values().stream().filter(NodeAndClient::isMasterEligible).count();
            final long stoppingMasters = nodeAndClients.stream().filter(NodeAndClient::isMasterEligible).count();

            assert stoppingMasters <= currentMasters : currentMasters + " < " + stoppingMasters;
            if (stoppingMasters != currentMasters && stoppingMasters > 0) {
                // If stopping few enough master-nodes that there's still a majority left, there is no need to withdraw their votes first.
                // However, we do not yet have a way to be sure there's a majority left, because the voting configuration may not yet have
                // been updated when the previous nodes shut down, so we must always explicitly withdraw votes.
                // TODO add cluster health API to check that voting configuration is optimal so this isn't always needed
                nodeAndClients.stream().filter(NodeAndClient::isMasterEligible).map(NodeAndClient::getName).forEach(excludedNodeIds::add);
                assert excludedNodeIds.size() == stoppingMasters;

                logger.info("adding voting config exclusions {} prior to restart/shutdown", excludedNodeIds);
                try {
                    client().execute(AddVotingConfigExclusionsAction.INSTANCE,
                            new AddVotingConfigExclusionsRequest(excludedNodeIds.toArray(Strings.EMPTY_ARRAY))).get();
                } catch (InterruptedException | ExecutionException e) {
                    throw new AssertionError("unexpected", e);
                }
            }
        }
        return excludedNodeIds;
    }

    private void removeExclusions(Set<String> excludedNodeIds) {
        assert Thread.holdsLock(this);
        if (excludedNodeIds.isEmpty() == false) {
            logger.info("removing voting config exclusions for {} after restart/shutdown", excludedNodeIds);
            try {
                Client client = getRandomNodeAndClient(node -> excludedNodeIds.contains(node.name) == false).client(random);
                client.execute(ClearVotingConfigExclusionsAction.INSTANCE, new ClearVotingConfigExclusionsRequest()).get();
            } catch (InterruptedException | ExecutionException e) {
                throw new AssertionError("unexpected", e);
            }
        }
    }

    /**
     * Restarts all nodes in the cluster. It first stops all nodes and then restarts all the nodes again.
     */
    public synchronized void fullRestart(RestartCallback callback) throws Exception {
        int numNodesRestarted = 0;
        final Settings[] newNodeSettings = new Settings[nextNodeId.get()];
        Map<Set<Role>, List<NodeAndClient>> nodesByRoles = new HashMap<>();
        Set[] rolesOrderedByOriginalStartupOrder = new Set[nextNodeId.get()];
        final int minMasterNodes = autoManageMinMasterNodes ? getMinMasterNodes(getMasterNodesCount()) : -1;
        for (NodeAndClient nodeAndClient : nodes.values()) {
            callback.doAfterNodes(numNodesRestarted++, nodeAndClient.nodeClient());
            logger.info("Stopping and resetting node [{}] ", nodeAndClient.name);
            if (activeDisruptionScheme != null) {
                activeDisruptionScheme.removeFromNode(nodeAndClient.name, this);
            }
            DiscoveryNode discoveryNode = getInstanceFromNode(ClusterService.class, nodeAndClient.node()).localNode();
            final Settings newSettings = nodeAndClient.closeForRestart(callback, minMasterNodes);
            newNodeSettings[nodeAndClient.nodeAndClientId()] = newSettings;
            rolesOrderedByOriginalStartupOrder[nodeAndClient.nodeAndClientId()] = discoveryNode.getRoles();
            nodesByRoles.computeIfAbsent(discoveryNode.getRoles(), k -> new ArrayList<>()).add(nodeAndClient);
        }

        assert nodesByRoles.values().stream().mapToInt(List::size).sum() == nodes.size();

        // randomize start up order, but making sure that:
        // 1) A data folder that was assigned to a data node will stay so
        // 2) Data nodes will get the same node lock ordinal range, so custom index paths (where the ordinal is used)
        //    will still belong to data nodes
        for (List<NodeAndClient> sameRoleNodes : nodesByRoles.values()) {
            Collections.shuffle(sameRoleNodes, random);
        }
        final List<NodeAndClient> startUpOrder = new ArrayList<>();
        for (Set roles : rolesOrderedByOriginalStartupOrder) {
            if (roles == null) {
                // if some nodes were stopped, we want have a role for that ordinal
                continue;
            }
            final List<NodeAndClient> nodesByRole = nodesByRoles.get(roles);
            startUpOrder.add(nodesByRole.remove(0));
        }
        assert nodesByRoles.values().stream().mapToInt(List::size).sum() == 0;

        for (NodeAndClient nodeAndClient : startUpOrder) {
            logger.info("creating node [{}] ", nodeAndClient.name);
            nodeAndClient.recreateNode(newNodeSettings[nodeAndClient.nodeAndClientId()], () -> rebuildUnicastHostFiles(startUpOrder));
        }

        startAndPublishNodesAndClients(startUpOrder);

        if (callback.validateClusterForming()) {
            validateClusterFormed();
        }
    }

    /**
     * Returns the name of the current master node in the cluster.
     */
    public String getMasterName() {
        return getMasterName(null);
    }

    /**
     * Returns the name of the current master node in the cluster and executes the request via the node specified
     * in the viaNode parameter. If viaNode isn't specified a random node will be picked to the send the request to.
     */
    public String getMasterName(@Nullable String viaNode) {
        try {
            Client client = viaNode != null ? client(viaNode) : client();
            final DiscoveryNode masterNode = client.admin().cluster().prepareState().get().getState().nodes().getMasterNode();
            assertNotNull(masterNode);
            return masterNode.getName();
        } catch (Exception e) {
            logger.warn("Can't fetch cluster state", e);
            throw new RuntimeException("Can't get master node " + e.getMessage(), e);
        }
    }

    synchronized Set<String> allDataNodesButN(int count) {
        final int numNodes = numDataNodes() - count;
        assert size() >= numNodes;
        Map<String, NodeAndClient> dataNodes =
            nodes
                .entrySet()
                .stream()
                .filter(entry -> DATA_NODE_PREDICATE.test(entry.getValue()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        final HashSet<String> set = new HashSet<>();
        final Iterator<String> iterator = dataNodes.keySet().iterator();
        for (int i = 0; i < numNodes; i++) {
            assert iterator.hasNext();
            set.add(iterator.next());
        }
        return set;
    }

    /**
     * Returns a set of nodes that have at least one shard of the given index.
     */
    public synchronized Set<String> nodesInclude(String index) {
        if (clusterService().state().routingTable().hasIndex(index)) {
            List<ShardRouting> allShards = clusterService().state().routingTable().allShards(index);
            DiscoveryNodes discoveryNodes = clusterService().state().getNodes();
            Set<String> nodes = new HashSet<>();
            for (ShardRouting shardRouting : allShards) {
                if (shardRouting.assignedToNode()) {
                    DiscoveryNode discoveryNode = discoveryNodes.get(shardRouting.currentNodeId());
                    nodes.add(discoveryNode.getName());
                }
            }
            return nodes;
        }
        return Collections.emptySet();
    }

    /**
     * Performs cluster bootstrap when node with index {@link #bootstrapMasterNodeIndex} is started
     * with the names of all existing and new master-eligible nodes.
     * Indexing starts from 0.
     * If {@link #bootstrapMasterNodeIndex} is -1 (default), this method does nothing.
     */
    private List<Settings> bootstrapMasterNodeWithSpecifiedIndex(List<Settings> allNodesSettings) {
        assert Thread.holdsLock(this);
        if (bootstrapMasterNodeIndex == -1) { // fast-path
            return allNodesSettings;
        }

        int currentNodeId = numMasterNodes() - 1;
        List<Settings> newSettings = new ArrayList<>();

        for (Settings settings : allNodesSettings) {
            if (Node.NODE_MASTER_SETTING.get(settings) == false) {
                newSettings.add(settings);
            } else {
                currentNodeId++;
                if (currentNodeId != bootstrapMasterNodeIndex) {
                    newSettings.add(settings);
                } else {
                    List<String> nodeNames = new ArrayList<>();

                    for (Settings nodeSettings : getDataOrMasterNodeInstances(Settings.class)) {
                        if (Node.NODE_MASTER_SETTING.get(nodeSettings)) {
                            nodeNames.add(Node.NODE_NAME_SETTING.get(nodeSettings));
                        }
                    }

                    for (Settings nodeSettings : allNodesSettings) {
                        if (Node.NODE_MASTER_SETTING.get(nodeSettings)) {
                            nodeNames.add(Node.NODE_NAME_SETTING.get(nodeSettings));
                        }
                    }

                    newSettings.add(Settings.builder().put(settings)
                            .putList(ClusterBootstrapService.INITIAL_MASTER_NODES_SETTING.getKey(), nodeNames)
                            .build());

                    setBootstrapMasterNodeIndex(-1);
                }
            }
        }

        return newSettings;
    }

    /**
     * Starts a node with default settings and returns its name.
     */
    public String startNode() {
        return startNode(Settings.EMPTY);
    }

    /**
     * Starts a node with the given settings builder and returns its name.
     */
    public String startNode(Settings.Builder settings) {
        return startNode(settings.build());
    }

    /**
     * Starts a node with the given settings and returns its name.
     */
    public String startNode(Settings settings) {
        return startNodes(settings).get(0);
    }

    /**
     * Starts multiple nodes with default settings and returns their names
     */
    public List<String> startNodes(int numOfNodes) {
        return startNodes(numOfNodes, Settings.EMPTY);
    }

    /**
     * Starts multiple nodes with the given settings and returns their names
     */
    public List<String> startNodes(int numOfNodes, Settings settings) {
        return startNodes(Collections.nCopies(numOfNodes, settings).toArray(new Settings[0]));
    }

    /**
     * Starts multiple nodes with the given settings and returns their names
     */
    public synchronized List<String> startNodes(Settings... extraSettings) {
        final int newMasterCount = Math.toIntExact(Stream.of(extraSettings).filter(Node.NODE_MASTER_SETTING::get).count());
        final int defaultMinMasterNodes;
        if (autoManageMinMasterNodes) {
            defaultMinMasterNodes = getMinMasterNodes(getMasterNodesCount() + newMasterCount);
        } else {
            defaultMinMasterNodes = -1;
        }
        final List<NodeAndClient> nodes = new ArrayList<>();
        final int prevMasterCount = getMasterNodesCount();
        int autoBootstrapMasterNodeIndex =
                prevMasterCount == 0 && autoManageMinMasterNodes && newMasterCount > 0 && Arrays.stream(extraSettings)
            .allMatch(s -> Node.NODE_MASTER_SETTING.get(s) == false
                || ZEN2_DISCOVERY_TYPE.equals(DISCOVERY_TYPE_SETTING.get(s)))
            ? RandomNumbers.randomIntBetween(random, 0, newMasterCount - 1) : -1;

        final int numOfNodes = extraSettings.length;
        final int firstNodeId = nextNodeId.getAndIncrement();
        final List<Settings> settings = new ArrayList<>();
        for (int i = 0; i < numOfNodes; i++) {
            settings.add(getNodeSettings(firstNodeId + i, random.nextLong(), extraSettings[i], defaultMinMasterNodes));
        }
        nextNodeId.set(firstNodeId + numOfNodes);

        final List<String> initialMasterNodes = settings.stream()
                .filter(Node.NODE_MASTER_SETTING::get)
                .map(Node.NODE_NAME_SETTING::get)
                .collect(Collectors.toList());

        final List<Settings> updatedSettings = bootstrapMasterNodeWithSpecifiedIndex(settings);

        for (int i = 0; i < numOfNodes; i++) {
            final Settings nodeSettings = updatedSettings.get(i);
            final Builder builder = Settings.builder();
            if (Node.NODE_MASTER_SETTING.get(nodeSettings)) {
                if (autoBootstrapMasterNodeIndex == 0) {
                    builder.putList(INITIAL_MASTER_NODES_SETTING.getKey(), initialMasterNodes);
                }
                autoBootstrapMasterNodeIndex -= 1;
            }

            final NodeAndClient nodeAndClient =
                    buildNode(firstNodeId + i, builder.put(nodeSettings).build(), false, () -> rebuildUnicastHostFiles(nodes));
            nodes.add(nodeAndClient);
        }
        startAndPublishNodesAndClients(nodes);
        if (autoManageMinMasterNodes) {
            validateClusterFormed();
        }
        return nodes.stream().map(NodeAndClient::getName).collect(Collectors.toList());
    }

    public List<String> startMasterOnlyNodes(int numNodes) {
        return startMasterOnlyNodes(numNodes, Settings.EMPTY);
    }

    public List<String> startMasterOnlyNodes(int numNodes, Settings settings) {
        Settings settings1 = Settings.builder()
                .put(settings)
                .put(Node.NODE_MASTER_SETTING.getKey(), true)
                .put(Node.NODE_DATA_SETTING.getKey(), false)
                .build();
        return startNodes(numNodes, settings1);
    }

    public List<String> startDataOnlyNodes(int numNodes) {
        return startNodes(
            numNodes,
            Settings.builder().put(Settings.EMPTY).put(Node.NODE_MASTER_SETTING.getKey(), false)
                .put(Node.NODE_DATA_SETTING.getKey(), true).build());
    }

    /** calculates a min master nodes value based on the given number of master nodes */
    private static int getMinMasterNodes(int eligibleMasterNodes) {
        return eligibleMasterNodes / 2 + 1;
    }

    private int getMasterNodesCount() {
        return (int) nodes.values().stream().filter(n -> Node.NODE_MASTER_SETTING.get(n.node().settings())).count();
    }

    public String startMasterOnlyNode() {
        return startMasterOnlyNode(Settings.EMPTY);
    }

    public String startMasterOnlyNode(Settings settings) {
        Settings settings1 = Settings.builder()
                .put(settings)
                .put(Node.NODE_MASTER_SETTING.getKey(), true)
                .put(Node.NODE_DATA_SETTING.getKey(), false)
                .build();
        return startNode(settings1);
    }

    public String startDataOnlyNode() {
        return startDataOnlyNode(Settings.EMPTY);
    }

    public String startDataOnlyNode(Settings settings) {
        Settings settings1 = Settings.builder()
                .put(settings)
                .put(Node.NODE_MASTER_SETTING.getKey(), false)
                .put(Node.NODE_DATA_SETTING.getKey(), true)
                .build();
        return startNode(settings1);
    }

    private synchronized void publishNode(NodeAndClient nodeAndClient) {
        assert !nodeAndClient.node().isClosed();
        final NavigableMap<String, NodeAndClient> newNodes = new TreeMap<>(nodes);
        newNodes.put(nodeAndClient.name, nodeAndClient);
        nodes = Collections.unmodifiableNavigableMap(newNodes);
        applyDisruptionSchemeToNode(nodeAndClient);
    }

    public void closeNonSharedNodes(boolean wipeData) throws IOException {
        reset(wipeData);
    }

    @Override
    public int numDataNodes() {
        return dataNodeAndClients().size();
    }

    @Override
    public int numDataAndMasterNodes() {
        return filterNodes(nodes, DATA_NODE_PREDICATE.or(MASTER_NODE_PREDICATE)).size();
    }

    public int numMasterNodes() {
      return filterNodes(nodes, NodeAndClient::isMasterEligible).size();
    }

    public void setDisruptionScheme(ServiceDisruptionScheme scheme) {
        assert activeDisruptionScheme == null :
            "there is already and active disruption [" + activeDisruptionScheme + "]. call clearDisruptionScheme first";
        scheme.applyToCluster(this);
        activeDisruptionScheme = scheme;
    }

    public void clearDisruptionScheme() {
        clearDisruptionScheme(true);
    }

    // synchronized to prevent concurrently modifying the cluster.
    public synchronized void clearDisruptionScheme(boolean ensureHealthyCluster) {
        if (activeDisruptionScheme != null) {
            TimeValue expectedHealingTime = activeDisruptionScheme.expectedTimeToHeal();
            logger.info("Clearing active scheme {}, expected healing time {}", activeDisruptionScheme, expectedHealingTime);
            if (ensureHealthyCluster) {
                activeDisruptionScheme.removeAndEnsureHealthy(this);
            } else {
                activeDisruptionScheme.removeFromCluster(this);
            }
        }
        activeDisruptionScheme = null;
    }

    private void applyDisruptionSchemeToNode(NodeAndClient nodeAndClient) {
        if (activeDisruptionScheme != null) {
            assert nodes.containsKey(nodeAndClient.name);
            activeDisruptionScheme.applyToNode(nodeAndClient.name, this);
        }
    }

    private void removeDisruptionSchemeFromNode(NodeAndClient nodeAndClient) {
        if (activeDisruptionScheme != null) {
            assert nodes.containsKey(nodeAndClient.name);
            activeDisruptionScheme.removeFromNode(nodeAndClient.name, this);
        }
    }

    private Collection<NodeAndClient> dataNodeAndClients() {
        return filterNodes(nodes, DATA_NODE_PREDICATE);
    }

    private static Collection<NodeAndClient> filterNodes(Map<String, InternalTestCluster.NodeAndClient> map,
            Predicate<NodeAndClient> predicate) {
        return map
            .values()
            .stream()
            .filter(predicate)
            .collect(Collectors.toCollection(ArrayList::new));
    }

    private static final class NodeNamePredicate implements Predicate<NodeAndClient> {
        private final String nodeName;

        NodeNamePredicate(String nodeName) {
            this.nodeName = nodeName;
        }

        @Override
        public boolean test(NodeAndClient nodeAndClient) {
            return nodeName.equals(nodeAndClient.getName());
        }
    }

    synchronized String routingKeyForShard(Index index, int shard, Random random) {
        assertThat(shard, greaterThanOrEqualTo(0));
        assertThat(shard, greaterThanOrEqualTo(0));
        for (NodeAndClient n : nodes.values()) {
            Node node = n.node;
            IndicesService indicesService = getInstanceFromNode(IndicesService.class, node);
            ClusterService clusterService = getInstanceFromNode(ClusterService.class, node);
            IndexService indexService = indicesService.indexService(index);
            if (indexService != null) {
                assertThat(indexService.getIndexSettings().getSettings().getAsInt(IndexMetaData.SETTING_NUMBER_OF_SHARDS, -1),
                        greaterThan(shard));
                OperationRouting operationRouting = clusterService.operationRouting();
                while (true) {
                    String routing = RandomStrings.randomAsciiOfLength(random, 10);
                    final int targetShard = operationRouting
                            .indexShards(clusterService.state(), index.getName(), null, routing)
                            .shardId().getId();
                    if (shard == targetShard) {
                        return routing;
                    }
                }
            }
        }
        fail("Could not find a node that holds " + index);
        return null;
    }

    @Override
    public Iterable<Client> getClients() {
        return () -> {
            ensureOpen();
            final Iterator<NodeAndClient> iterator = nodes.values().iterator();
            return new Iterator<Client>() {

                @Override
                public boolean hasNext() {
                    return iterator.hasNext();
                }

                @Override
                public Client next() {
                    return iterator.next().client(random);
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException("");
                }

            };
        };
    }

    @Override
    public NamedWriteableRegistry getNamedWriteableRegistry() {
        return getInstance(NamedWriteableRegistry.class);
    }

    /**
     * Returns a predicate that only accepts settings of nodes with one of the given names.
     */
    public static Predicate<Settings> nameFilter(String... nodeNames) {
        final Set<String> nodes = Sets.newHashSet(nodeNames);
        return settings -> nodes.contains(settings.get("node.name"));
    }

    /**
     * An abstract class that is called during {@link #rollingRestart(InternalTestCluster.RestartCallback)}
     * and / or {@link #fullRestart(InternalTestCluster.RestartCallback)} to execute actions at certain
     * stages of the restart.
     */
    public static class RestartCallback {

        /**
         * Executed once the give node name has been stopped.
         */
        public Settings onNodeStopped(String nodeName) throws Exception {
            return Settings.EMPTY;
        }

        /**
         * Executed for each node before the {@code n + 1} node is restarted. The given client is
         * an active client to the node that will be restarted next.
         */
        public void doAfterNodes(int n, Client client) throws Exception {
        }

        /**
         * If this returns <code>true</code> all data for the node with the given node name will be cleared including
         * gateways and all index data. Returns <code>false</code> by default.
         */
        public boolean clearData(String nodeName) {
            return false;
        }

        /** returns true if the restart should also validate the cluster has reformed */
        public boolean validateClusterForming() { return true; }
    }

    public Settings getDefaultSettings() {
        return defaultSettings;
    }

    @Override
    public void ensureEstimatedStats() {
        if (size() > 0) {
            // Checks that the breakers have been reset without incurring a
            // network request, because a network request can increment one
            // of the breakers
            for (NodeAndClient nodeAndClient : nodes.values()) {
                final IndicesFieldDataCache fdCache =
                        getInstanceFromNode(IndicesService.class, nodeAndClient.node).getIndicesFieldDataCache();
                // Clean up the cache, ensuring that entries' listeners have been called
                fdCache.getCache().refresh();

                final String name = nodeAndClient.name;
                final CircuitBreakerService breakerService = getInstanceFromNode(CircuitBreakerService.class, nodeAndClient.node);
                CircuitBreaker fdBreaker = breakerService.getBreaker(CircuitBreaker.FIELDDATA);
                assertThat("Fielddata breaker not reset to 0 on node: " + name, fdBreaker.getUsed(), equalTo(0L));
                try {
                    assertBusy(() -> {
                        CircuitBreaker acctBreaker = breakerService.getBreaker(CircuitBreaker.ACCOUNTING);
                        assertThat("Accounting breaker not reset to 0 on node: " + name + ", are there still Lucene indices around?",
                            acctBreaker.getUsed(), equalTo(0L));
                    });
                } catch (Exception e) {
                    throw new AssertionError("Exception during check for accounting breaker reset to 0", e);
                }
                // Anything that uses transport or HTTP can increase the
                // request breaker (because they use bigarrays), because of
                // that the breaker can sometimes be incremented from ping
                // requests from other clusters because Jenkins is running
                // multiple ES testing jobs in parallel on the same machine.
                // To combat this we check whether the breaker has reached 0
                // in an assertBusy loop, so it will try for 10 seconds and
                // fail if it never reached 0
                try {
                    assertBusy(() -> {
                        CircuitBreaker reqBreaker = breakerService.getBreaker(CircuitBreaker.REQUEST);
                        assertThat("Request breaker not reset to 0 on node: " + name, reqBreaker.getUsed(), equalTo(0L));
                    });
                } catch (Exception e) {
                    throw new AssertionError("Exception during check for request breaker reset to 0", e);
                }

                // RamAccounting release operations can run asynchronous after clients already received results.
                try {
                    assertBusy(() -> {
                        CircuitBreaker crateQueryBreaker = breakerService.getBreaker("query");
                        if (crateQueryBreaker != null) {
                            assertThat("Query breaker not reset to 0 on node: " + name,
                                       crateQueryBreaker.getUsed(),
                                       equalTo(0L));
                        }
                    });
                } catch (Exception e) {
                    throw new AssertionError("Exception during check for query breaker reset to 0", e);
                }
            }
        }
    }

    @Override
    public synchronized void assertAfterTest() throws IOException {
        super.assertAfterTest();
        assertRequestsFinished();
        for (NodeAndClient nodeAndClient : nodes.values()) {
            NodeEnvironment env = nodeAndClient.node().getNodeEnvironment();
            Set<ShardId> shardIds = env.lockedShards();
            for (ShardId id : shardIds) {
                try {
                    env.shardLock(id, "InternalTestCluster assert after test", TimeUnit.SECONDS.toMillis(5)).close();
                } catch (ShardLockObtainFailedException ex) {
                    fail("Shard " + id + " is still locked after 5 sec waiting");
                }
            }
        }
    }

    private void assertRequestsFinished() {
        assert Thread.holdsLock(this);
        if (size() > 0) {
            for (NodeAndClient nodeAndClient : nodes.values()) {
                CircuitBreaker inFlightRequestsBreaker = getInstance(CircuitBreakerService.class, nodeAndClient.name)
                    .getBreaker(CircuitBreaker.IN_FLIGHT_REQUESTS);
                try {
                    // see #ensureEstimatedStats()
                    assertBusy(() -> {
                        // ensure that our size accounting on transport level is reset properly
                        long bytesUsed = inFlightRequestsBreaker.getUsed();
                        assertThat("All incoming requests on node [" + nodeAndClient.name + "] should have finished. Expected 0 but got " +
                            bytesUsed, bytesUsed, equalTo(0L));
                    });
                } catch (Exception e) {
                    logger.error("Could not assert finished requests within timeout", e);
                    fail("Could not assert finished requests within timeout on node [" + nodeAndClient.name + "]");
                }
            }
        }
    }
}
