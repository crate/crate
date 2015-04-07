/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.test.integration;

import com.carrotsearch.randomizedtesting.SeedUtils;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Collections2;
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.decider.DiskThresholdDecider;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.inject.Key;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.network.NetworkUtils;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.engine.IndexEngineModule;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.internal.InternalNode;
import org.elasticsearch.test.engine.MockEngineModule;
import org.elasticsearch.test.store.MockFSIndexStoreModule;
import org.elasticsearch.test.transport.AssertingLocalTransport;
import org.elasticsearch.transport.TransportModule;
import org.junit.Assert;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.collect.Maps.newTreeMap;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

/**
 * TestCluster manages a set of JVM private nodes and allows convenient access to them.
 * The cluster supports randomized configuration such that nodes started in the cluster will
 * automatically load asserting services tracking resources like file handles or open searchers.
 * <p>
 * The Cluster is bound to a test lifecycle where tests must call {@link #beforeTest(java.util.Random)} and
 * {@link #afterTest()} to initialize and reset the cluster in order to be more reproducible. The term "more" relates
 * to the async nature of Elasticsearch in combination with randomized testing. Once Threads and asynchronous calls
 * are involved reproducibility is very limited. This class should only be used through {@link CrateIntegrationTest}.
 * </p>
 */
public class CrateTestCluster implements Iterable<Client> {

    private static final ESLogger logger = Loggers.getLogger(CrateTestCluster.class);

    /* sorted map to make traverse order reproducible */
    private final TreeMap<String, NodeAndClient> nodes = newTreeMap();

    private final Set<File> dataDirToClean = new HashSet<File>();

    private final String clusterName;

    private final AtomicBoolean open = new AtomicBoolean(true);

    private final Settings defaultSettings;

    private Random random;

    private AtomicInteger nextNodeId = new AtomicInteger(0);

    /* We have a fixed number of shared nodes that we keep around across tests */
    private final int numSharedNodes;

    /* Each shared node has a node seed that is used to start up the node and get default settings
     * this is important if a node is randomly shut down in a test since the next test relies on a
     * fully shared cluster to be more reproducible */
    private final long[] sharedNodesSeeds;

    private final NodeSettingsSource nodeSettingsSource;

    private Path tmpDataDir = null;

    public CrateTestCluster(long clusterSeed, String clusterName) {
        this(clusterSeed, 2, "network", clusterName, NodeSettingsSource.EMPTY);
    }

    public <T> T getInstanceFromNode(String nodeName, Class<T> clazz) {
        NodeAndClient node = nodes.get(nodeName);
        return node.node.injector().getInstance(clazz);
    }

    public <T> T getInstanceFromFirstNode(Class<T> clazz) {
        String name = nodes.keySet().iterator().next();
        return getInstanceFromNode(name, clazz);
    }

    public CrateTestCluster(long clusterSeed, int numNodes, String mode, String clusterName, NodeSettingsSource nodeSettingsSource) {
        this.clusterName = clusterName;
        random = new Random(clusterSeed);
        numSharedNodes = numNodes < 0 ? 2 : numNodes;

        sharedNodesSeeds = new long[numSharedNodes];
        for (int i = 0; i < sharedNodesSeeds.length; i++) {
            sharedNodesSeeds[i] = random.nextLong();
        }
        logger.info("Setup TestCluster [{}] with seed [{}] using [{}] nodes", clusterName, SeedUtils.formatSeed(clusterSeed), numSharedNodes);
        try {
            tmpDataDir = Files.createTempDirectory(null);
        } catch (IOException e) {
            // ignore
        }

        ImmutableSettings.Builder builder = settingsBuilder()
            .put("index.store.type", MockFSIndexStoreModule.class.getName()) // no RAM dir for now!
            .put(IndexEngineModule.EngineSettings.ENGINE_TYPE, MockEngineModule.class.getName())
            .put("cluster.name", clusterName)
            .put(DiskThresholdDecider.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED, false)
                // decrease the routing schedule so new nodes will be added quickly - some random value between 30 and 80 ms
            .put("cluster.routing.schedule", "30ms")
            .put("http.port", "44200-44300")
            .put("transport.tcp.port", "44300-44400");

        builder.put("node.mode", mode);
        if (tmpDataDir != null) {
            builder.put("path.data", tmpDataDir.toAbsolutePath());
        }
        if (isLocalTransportConfigured()) {
            builder.put(TransportModule.TRANSPORT_TYPE_KEY, AssertingLocalTransport.class.getName());
        }
        this.defaultSettings = builder.build();
        this.nodeSettingsSource = nodeSettingsSource;
    }

    private static boolean isLocalTransportConfigured() {
        if ("local".equals(System.getProperty("es.node.mode", "network"))) {
            return true;
        }
        return Boolean.parseBoolean(System.getProperty("es.node.local", "false"));
    }

    private Settings getSettings(int nodeOrdinal, Settings others) {
        ImmutableSettings.Builder builder = settingsBuilder().put(defaultSettings);
        Settings settings = nodeSettingsSource.settings(nodeOrdinal);
        if (settings != null) {
            builder.put(settings);
        }
        if (others != null) {
            builder.put(others);
        }
        return builder.build();
    }

    public static String clusterName(String prefix, String childVMId, long clusterSeed) {
        StringBuilder builder = new StringBuilder(prefix);
        builder.append('-').append(NetworkUtils.getLocalAddress().getHostName());
        builder.append("-CHILD_VM=[").append(childVMId).append(']');
        builder.append("-CLUSTER_SEED=[").append(clusterSeed).append(']');
        // if multiple maven task run on a single host we better have an identifier that doesn't rely on input params
        builder.append("-HASH=[").append(SeedUtils.formatSeed(System.nanoTime())).append(']');
        return builder.toString();
    }

    public String clusterName() {
        return clusterName;
    }

    private void ensureOpen() {
        if (!open.get()) {
            throw new RuntimeException("Cluster is already closed");
        }
    }

    private synchronized NodeAndClient getOrBuildRandomNode() {
        ensureOpen();
        NodeAndClient randomNodeAndClient = getRandomNodeAndClient();
        if (randomNodeAndClient != null) {
            return randomNodeAndClient;
        }
        NodeAndClient buildNode = buildNode();
        buildNode.node().start();
        publishNode(buildNode);
        return buildNode;
    }

    private synchronized NodeAndClient getRandomNodeAndClient() {
        Predicate<NodeAndClient> all = Predicates.alwaysTrue();
        return getRandomNodeAndClient(all);
    }


    private synchronized NodeAndClient getRandomNodeAndClient(Predicate<NodeAndClient> predicate) {
        ensureOpen();
        Collection<NodeAndClient> values = Collections2.filter(nodes.values(), predicate);
        if (!values.isEmpty()) {
            int whichOne = random.nextInt(values.size());
            for (NodeAndClient nodeAndClient : values) {
                if (whichOne-- == 0) {
                    return nodeAndClient;
                }
            }
        }
        return null;
    }

    /**
     * Ensures that at least <code>n</code> nodes are present in the cluster.
     * if more nodes than <code>n</code> are present this method will not
     * stop any of the running nodes.
     */
    public synchronized void ensureAtLeastNumNodes(int n) {
        int size = nodes.size();
        for (int i = size; i < n; i++) {
            logger.info("increasing cluster size from {} to {}", size, n);
            NodeAndClient buildNode = buildNode();
            buildNode.node().start();
            publishNode(buildNode);
        }
    }

    /**
     * Ensures that at most <code>n</code> are up and running.
     * If less nodes that <code>n</code> are running this method
     * will not start any additional nodes.
     */
    public synchronized void ensureAtMostNumNodes(int n) {
        if (nodes.size() <= n) {
            return;
        }
        // prevent killing the master if possible
        final Iterator<NodeAndClient> values = n == 0 ? nodes.values().iterator() : Iterators.filter(nodes.values().iterator(), Predicates.not(new MasterNodePredicate(getMasterName())));
        final Iterator<NodeAndClient> limit = Iterators.limit(values, nodes.size() - n);
        logger.info("reducing cluster size from {} to {}", nodes.size() - n, n);
        Set<NodeAndClient> nodesToRemove = new HashSet<NodeAndClient>();
        while (limit.hasNext()) {
            NodeAndClient next = limit.next();
            nodesToRemove.add(next);
            next.close();
        }
        for (NodeAndClient toRemove : nodesToRemove) {
            nodes.remove(toRemove.name);
        }
    }

    protected NodeAndClient buildNode(Settings settings) {
        int ord = nextNodeId.getAndIncrement();
        return buildNode(ord, random.nextLong(), settings);
    }

    protected NodeAndClient buildNode() {
        int ord = nextNodeId.getAndIncrement();
        return buildNode(ord, random.nextLong(), null);
    }

    private NodeAndClient buildNode(int nodeId, long seed, Settings settings) {
        ensureOpen();
        settings = getSettings(nodeId, settings);
        String name;

        if (settings.get("node.name", null) != null) {
            name = settings.get("node.name");
        } else {
            name = buildNodeName(nodeId);
        }

        assert !nodes.containsKey(name);
        Settings finalSettings = settingsBuilder()
            .put(settings)
            .put("name", name)
            .build();
        Node node = nodeBuilder().settings(finalSettings).build();
        return new NodeAndClient(name, node, new ClientFactory());
    }

    private String buildNodeName(int id) {
        return "node_" + id;
    }

    public synchronized Client client() {
        ensureOpen();
        /* Randomly return a client to one of the nodes in the cluster */
        return getOrBuildRandomNode().client(random);
    }

    /**
     * Returns a node client to the current master node.
     * Note: use this with care tests should not rely on a certain nodes client.
     */
    public synchronized Client masterClient() {
        ensureOpen();
        NodeAndClient randomNodeAndClient = getRandomNodeAndClient(new MasterNodePredicate(getMasterName()));
        if (randomNodeAndClient != null) {
            return randomNodeAndClient.nodeClient(); // ensure node client master is requested
        }
        Assert.fail("No master client found");
        return null; // can't happen
    }

    /**
     * Returns a node client to random node but not the master. This method will fail if no non-master client is available.
     */
    public synchronized Client nonMasterClient() {
        ensureOpen();
        NodeAndClient randomNodeAndClient = getRandomNodeAndClient(Predicates.not(new MasterNodePredicate(getMasterName())));
        if (randomNodeAndClient != null) {
            return randomNodeAndClient.nodeClient(); // ensure node client non-master is requested
        }
        Assert.fail("No non-master client found");
        return null; // can't happen
    }

    /**
     * Returns a client to a node started with "node.client: true"
     */
    public synchronized Client clientNodeClient() {
        ensureOpen();
        NodeAndClient randomNodeAndClient = getRandomNodeAndClient(new ClientNodePredicate());
        if (randomNodeAndClient != null) {
            return randomNodeAndClient.client(random);
        }
        startNodeClient(ImmutableSettings.EMPTY);
        return getRandomNodeAndClient(new ClientNodePredicate()).client(random);
    }

    /**
     * Returns a node client to a given node.
     */
    public synchronized Client client(String nodeName) {
        ensureOpen();
        NodeAndClient nodeAndClient = nodes.get(nodeName);
        if (nodeAndClient != null) {
            return nodeAndClient.client(random);
        }
        Assert.fail("No node found with name: [" + nodeName + "]");
        return null; // can't happen
    }


    /**
     * Returns a "smart" node client to a random node in the cluster
     */
    public synchronized Client smartClient() {
        NodeAndClient randomNodeAndClient = getRandomNodeAndClient();
        if (randomNodeAndClient != null) {
            return randomNodeAndClient.nodeClient();
        }
        Assert.fail("No smart client found");
        return null; // can't happen
    }

    /**
     * Returns a random node that applies to the given predicate.
     * The predicate can filter nodes based on the nodes settings.
     * If all nodes are filtered out this method will return <code>null</code>
     */
    public synchronized Client client(final Predicate<Settings> filterPredicate) {
        ensureOpen();
        final NodeAndClient randomNodeAndClient = getRandomNodeAndClient(new Predicate<NodeAndClient>() {
            @Override
            public boolean apply(NodeAndClient nodeAndClient) {
                return filterPredicate.apply(nodeAndClient.node.settings());
            }
        });
        if (randomNodeAndClient != null) {
            return randomNodeAndClient.client(random);
        }
        return null;
    }

    void close() {
        ensureOpen();
        if (this.open.compareAndSet(true, false)) {
            IOUtils.closeWhileHandlingException(nodes.values());
            nodes.clear();
        }
    }

    private final class NodeAndClient implements Closeable {
        private InternalNode node;
        private Client client;
        private Client nodeClient;
        private final AtomicBoolean closed = new AtomicBoolean(false);
        private final ClientFactory clientFactory;
        private final String name;

        NodeAndClient(String name, Node node, ClientFactory factory) {
            this.node = (InternalNode)node;
            this.name = name;
            this.clientFactory = factory;
        }

        Node node() {
            if (closed.get()) {
                throw new RuntimeException("already closed");
            }
            return node;
        }

        Client client(Random random) {
            if (closed.get()) {
                throw new RuntimeException("already closed");
            }
            if (client != null) {
                return client;
            }
            return client = clientFactory.client(node, clusterName);
        }

        Client nodeClient() {
            if (closed.get()) {
                throw new RuntimeException("already closed");
            }
            if (nodeClient == null) {
                nodeClient = node.client();
            }
            return nodeClient;
        }

        void resetClient() {
            if (closed.get()) {
                throw new RuntimeException("already closed");
            }
            if (client != null) {
                client.close();
                client = null;
            }
            if (nodeClient != null) {
                nodeClient.close();
                nodeClient = null;
            }
        }

        void restart(RestartCallback callback) throws Exception {
            assert callback != null;
            if (!node.isClosed()) {
                node.close();
            }
            Settings newSettings = callback.onNodeStopped(name);
            if (newSettings == null) {
                newSettings = ImmutableSettings.EMPTY;
            }
            if (callback.clearData(name)) {
                NodeEnvironment nodeEnv = getInstanceFromNode(NodeEnvironment.class, node);
                if (nodeEnv.hasNodeFile()) {
                    FileSystemUtils.deleteRecursively(nodeEnv.nodeDataLocations());
                }
            }
            node = (InternalNode) nodeBuilder().settings(node.settings()).settings(newSettings).node();
            resetClient();
        }

        @Override
        public void close() {
            closed.set(true);
            if (client != null) {
                client.close();
                client = null;
            }
            if (nodeClient != null) {
                nodeClient.close();
                nodeClient = null;
            }
            node.close();

        }
    }

    static class ClientFactory {

        public Client client(Node node, String clusterName) {
            return node.client();
        }
    }

    /**
     * This method should be exectued before each test to reset the cluster to it's initial state.
     */
    synchronized void beforeTest(Random random) {
        reset(random, true);
    }

    private synchronized void reset(Random random, boolean wipeData) {
        logger.debug("Reset test cluster");
        this.random = new Random(random.nextLong());
        resetClients(); /* reset all clients - each test gets it's own client based on the Random instance created above. */
        if (wipeData) {
            wipeDataDirectories();
        }
        if (nextNodeId.get() == sharedNodesSeeds.length && nodes.size() == sharedNodesSeeds.length) {
            logger.debug("Cluster hasn't changed - moving out - nodes: [{}] nextNodeId: [{}] numSharedNodes: [{}]", nodes.keySet(), nextNodeId.get(), sharedNodesSeeds.length);
            return;
        }
        logger.debug("Cluster is NOT consistent - restarting shared nodes - nodes: [{}] nextNodeId: [{}] numSharedNodes: [{}]", nodes.keySet(), nextNodeId.get(), sharedNodesSeeds.length);


        Set<NodeAndClient> sharedNodes = new HashSet<NodeAndClient>();
        boolean changed = false;
        for (int i = 0; i < sharedNodesSeeds.length; i++) {
            String buildNodeName = buildNodeName(i);
            NodeAndClient nodeAndClient = nodes.get(buildNodeName);
            if (nodeAndClient == null) {
                changed = true;
                nodeAndClient = buildNode(i, sharedNodesSeeds[i], defaultSettings);
                nodeAndClient.node.start();
                logger.info("Start Shared Node [{}] not shared", nodeAndClient.name);
            }
            sharedNodes.add(nodeAndClient);
        }
        if (!changed && sharedNodes.size() == nodes.size()) {
            logger.debug("Cluster is consistent - moving out - nodes: [{}] nextNodeId: [{}] numSharedNodes: [{}]", nodes.keySet(), nextNodeId.get(), sharedNodesSeeds.length);
            if (size() > 0) {
                client().admin().cluster().prepareHealth().setWaitForNodes(Integer.toString(sharedNodesSeeds.length)).get();
            }
            return; // we are consistent - return
        }
        for (NodeAndClient nodeAndClient : sharedNodes) {
            nodes.remove(nodeAndClient.name);
        }

        // trash the remaining nodes
        final Collection<NodeAndClient> toShutDown = nodes.values();
        for (NodeAndClient nodeAndClient : toShutDown) {
            logger.debug("Close Node [{}] not shared", nodeAndClient.name);
            nodeAndClient.close();
        }
        nodes.clear();
        for (NodeAndClient nodeAndClient : sharedNodes) {
            publishNode(nodeAndClient);
        }
        nextNodeId.set(sharedNodesSeeds.length);
        assert size() == sharedNodesSeeds.length;
        if (size() > 0) {
            client().admin().cluster().prepareHealth().setWaitForNodes(Integer.toString(sharedNodesSeeds.length)).get();
        }
        logger.debug("Cluster is consistent again - nodes: [{}] nextNodeId: [{}] numSharedNodes: [{}]", nodes.keySet(), nextNodeId.get(), sharedNodesSeeds.length);
    }

    /**
     * This method should be executed during tearDown
     */
    public synchronized void afterTest() {
        wipeDataDirectories();
        resetClients(); /* reset all clients - each test gets it's own client based on the Random instance created above. */

    }

    private void resetClients() {
        final Collection<NodeAndClient> nodesAndClients = nodes.values();
        for (NodeAndClient nodeAndClient : nodesAndClients) {
            nodeAndClient.resetClient();
        }
    }

    private void wipeDataDirectories() {
        if (!dataDirToClean.isEmpty()) {
            logger.info("Wipe data directory for all nodes locations: {}", this.dataDirToClean);
            try {
                FileSystemUtils.deleteRecursively(dataDirToClean.toArray(new File[0]));
            } finally {
                this.dataDirToClean.clear();
            }
        }
        if (tmpDataDir != null) {
            FileSystemUtils.deleteRecursively(tmpDataDir.toAbsolutePath().toFile());
        }
    }

    /**
     * Returns a reference to a random nodes {@link ClusterService}
     */
    public synchronized ClusterService clusterService() {
        return getInstance(ClusterService.class);
    }

    /**
     * Returns an Iterabel to all instances for the given class &gt;T&lt; across all nodes in the cluster.
     */
    public synchronized <T> Iterable<T> getInstances(Class<T> clazz) {
        List<T> instances = new ArrayList<T>(nodes.size());
        for (NodeAndClient nodeAndClient : nodes.values()) {
            instances.add(getInstanceFromNode(clazz, nodeAndClient.node));
        }
        return instances;
    }

    /**
     * Returns a reference to the given nodes instances of the given class &gt;T&lt;
     */
    public synchronized <T> T getInstance(Class<T> clazz, final String node) {
        final Predicate<CrateTestCluster.NodeAndClient> predicate;
        if (node != null) {
            predicate = new Predicate<CrateTestCluster.NodeAndClient>() {
                public boolean apply(NodeAndClient nodeAndClient) {
                    return node.equals(nodeAndClient.name);
                }
            };
        } else {
            predicate = Predicates.alwaysTrue();
        }
        NodeAndClient randomNodeAndClient = getRandomNodeAndClient(predicate);
        assert randomNodeAndClient != null;
        return getInstanceFromNode(clazz, randomNodeAndClient.node);
    }

    /**
     * Returns a reference to a random nodes instances of the given class &gt;T&lt;
     */
    public synchronized <T> T getInstance(Class<T> clazz) {
        return getInstance(clazz, null);
    }

    private synchronized <T> T getInstanceFromNode(Class<T> clazz, InternalNode node) {
        return node.injector().getInstance(clazz);
    }

    /**
     * Returns a reference to the given nodes instances of the given key &gt;T&lt;
     */
    public synchronized <T> T getInstance(Key<T> key, final String node) {
        final Predicate<CrateTestCluster.NodeAndClient> predicate;
        if (node != null) {
            predicate = new Predicate<CrateTestCluster.NodeAndClient>() {
                public boolean apply(NodeAndClient nodeAndClient) {
                    return node.equals(nodeAndClient.name);
                }
            };
        } else {
            predicate = Predicates.alwaysTrue();
        }
        NodeAndClient randomNodeAndClient = getRandomNodeAndClient(predicate);
        assert randomNodeAndClient != null;
        return getInstanceFromNode(key, randomNodeAndClient.node);
    }

    /**
     * Returns a reference to a random nodes instances of the given key &gt;T&lt;
     */
    public synchronized <T> T getInstance(Key<T> key) {
        return getInstance(key, null);
    }

    private synchronized <T> T getInstanceFromNode(Key<T> key, InternalNode node) {
        return node.injector().getInstance(key);
    }

    /**
     * Returns the number of nodes in the cluster.
     */
    public synchronized int size() {
        return this.nodes.size();
    }

    /**
     * Stops a random node in the cluster.
     */
    public synchronized void stopRandomNode() {
        ensureOpen();
        NodeAndClient nodeAndClient = getRandomNodeAndClient();
        if (nodeAndClient != null) {
            logger.info("Closing random node [{}] ", nodeAndClient.name);
            nodes.remove(nodeAndClient.name);
            nodeAndClient.close();
        }
    }

    public synchronized void stopNode(final String nodeName) {
        ensureOpen();
        NodeAndClient nodeAndClient = getRandomNodeAndClient(new Predicate<NodeAndClient>() {
            @Override
            public boolean apply(@Nullable io.crate.test.integration.CrateTestCluster.NodeAndClient input) {
                return input.name.equals(nodeName);
            }
        });

        if (nodeAndClient != null) {
            logger.info("Closing random node [{}] ", nodeAndClient.name);
            nodes.remove(nodeAndClient.name);
            nodeAndClient.close();
        }
    }

    /**
     * Stops a random node in the cluster that applies to the given filter or non if the non of the nodes applies to the
     * filter.
     */
    public synchronized void stopRandomNode(final Predicate<Settings> filter) {
        ensureOpen();
        NodeAndClient nodeAndClient = getRandomNodeAndClient(new Predicate<CrateTestCluster.NodeAndClient>() {
            @Override
            public boolean apply(NodeAndClient nodeAndClient) {
                return filter.apply(nodeAndClient.node.settings());
            }
        });
        if (nodeAndClient != null) {
            logger.info("Closing filtered random node [{}] ", nodeAndClient.name);
            nodes.remove(nodeAndClient.name);
            nodeAndClient.close();
        }
    }


    /**
     * Stops the current master node forcefully
     */
    public synchronized void stopCurrentMasterNode() {
        ensureOpen();
        assert size() > 0;
        String masterNodeName = getMasterName();
        assert nodes.containsKey(masterNodeName);
        logger.info("Closing master node [{}] ", masterNodeName);
        NodeAndClient remove = nodes.remove(masterNodeName);
        remove.close();
    }

    /**
     * Stops the any of the current nodes but not the master node.
     */
    public void stopRandomNonMasterNode() {
        NodeAndClient nodeAndClient = getRandomNodeAndClient(Predicates.not(new MasterNodePredicate(getMasterName())));
        if (nodeAndClient != null) {
            logger.info("Closing random non master node [{}] current master [{}] ", nodeAndClient.name, getMasterName());
            nodes.remove(nodeAndClient.name);
            nodeAndClient.close();
        }
    }

    /**
     * Restarts a random node in the cluster
     */
    public void restartRandomNode() throws Exception {
        restartRandomNode(EMPTY_CALLBACK);
    }


    /**
     * Restarts a random node in the cluster and calls the callback during restart.
     */
    public void restartRandomNode(RestartCallback callback) throws Exception {
        ensureOpen();
        NodeAndClient nodeAndClient = getRandomNodeAndClient();
        if (nodeAndClient != null) {
            logger.info("Restarting random node [{}] ", nodeAndClient.name);
            nodeAndClient.restart(callback);
        }
    }

    private void restartAllNodes(boolean rollingRestart, RestartCallback callback) throws Exception {
        ensureOpen();
        List<NodeAndClient> toRemove = new ArrayList<CrateTestCluster.NodeAndClient>();
        try {
            for (NodeAndClient nodeAndClient : nodes.values()) {
                if (!callback.doRestart(nodeAndClient.name)) {
                    logger.info("Closing node [{}] during restart", nodeAndClient.name);
                    toRemove.add(nodeAndClient);
                    nodeAndClient.close();
                }
            }
        } finally {
            for (NodeAndClient nodeAndClient : toRemove) {
                nodes.remove(nodeAndClient.name);
            }
        }
        logger.info("Restarting remaining nodes rollingRestart [{}]", rollingRestart);
        if (rollingRestart) {
            int numNodesRestarted = 0;
            for (NodeAndClient nodeAndClient : nodes.values()) {
                callback.doAfterNodes(numNodesRestarted++, nodeAndClient.nodeClient());
                logger.info("Restarting node [{}] ", nodeAndClient.name);
                nodeAndClient.restart(callback);
            }
        } else {
            int numNodesRestarted = 0;
            for (NodeAndClient nodeAndClient : nodes.values()) {
                callback.doAfterNodes(numNodesRestarted++, nodeAndClient.nodeClient());
                logger.info("Stopping node [{}] ", nodeAndClient.name);
                nodeAndClient.node.close();
            }
            for (NodeAndClient nodeAndClient : nodes.values()) {
                logger.info("Starting node [{}] ", nodeAndClient.name);
                nodeAndClient.restart(callback);
            }
        }
    }


    private static final RestartCallback EMPTY_CALLBACK = new RestartCallback() {
        public Settings onNodeStopped(String node) {
            return null;
        }
    };

    /**
     * Restarts all nodes in the cluster. It first stops all nodes and then restarts all the nodes again.
     */
    public void fullRestart() throws Exception {
        fullRestart(EMPTY_CALLBACK);
    }

    /**
     * Restarts all nodes in a rolling restart fashion ie. only restarts on node a time.
     */
    public void rollingRestart() throws Exception {
        rollingRestart(EMPTY_CALLBACK);
    }

    /**
     * Restarts all nodes in a rolling restart fashion ie. only restarts on node a time.
     */
    public void rollingRestart(RestartCallback function) throws Exception {
        restartAllNodes(true, function);
    }

    /**
     * Restarts all nodes in the cluster. It first stops all nodes and then restarts all the nodes again.
     */
    public void fullRestart(RestartCallback function) throws Exception {
        restartAllNodes(false, function);
    }

    private String getMasterName() {
        try {
            ClusterState state = client().admin().cluster().prepareState().execute().actionGet().getState();
            return state.nodes().masterNode().name();
        } catch (Throwable e) {
            logger.warn("Can't fetch cluster state", e);
            throw new RuntimeException("Can't get master node " + e.getMessage(), e);
        }
    }

    synchronized Set<String> allButN(int numNodes) {
        return nRandomNodes(size() - numNodes);
    }

    private synchronized Set<String> nRandomNodes(int numNodes) {
        assert size() >= numNodes;
        return Sets.newHashSet(Iterators.limit(this.nodes.keySet().iterator(), numNodes));
    }

    public synchronized void startNodeClient(Settings settings) {
        ensureOpen(); // currently unused
        startNode(settingsBuilder().put(settings).put("node.client", true));
    }

    /**
     * Returns a set of nodes that have at least one shard of the given index.
     */
    public synchronized Set<String> nodesInclude(String index) {
        if (clusterService().state().routingTable().hasIndex(index)) {
            List<ShardRouting> allShards = clusterService().state().routingTable().allShards(index);
            DiscoveryNodes discoveryNodes = clusterService().state().getNodes();
            Set<String> nodes = new HashSet<String>();
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
     * Starts a node with default settings and returns it's name.
     */
    public String startNode() {
        return startNode(ImmutableSettings.EMPTY);
    }

    /**
     * Starts a node with the given settings builder and returns it's name.
     */
    public String startNode(Settings.Builder settings) {
        return startNode(settings.build());
    }

    /**
     * Starts a node with the given settings and returns it's name.
     */
    public String startNode(Settings settings) {
        NodeAndClient buildNode = buildNode(settings);
        buildNode.node().start();
        publishNode(buildNode);
        return buildNode.name;
    }

    private void publishNode(NodeAndClient nodeAndClient) {
        assert !nodeAndClient.node().isClosed();
        NodeEnvironment nodeEnv = getInstanceFromNode(NodeEnvironment.class, nodeAndClient.node);
        if (nodeEnv.hasNodeFile()) {
            dataDirToClean.addAll(Arrays.asList(nodeEnv.nodeDataLocations()));
        }
        nodes.put(nodeAndClient.name, nodeAndClient);

    }

    public void closeNonSharedNodes(boolean wipeData) {
        reset(random, wipeData);
    }


    private static final class MasterNodePredicate implements Predicate<NodeAndClient> {
        private final String masterNodeName;

        public MasterNodePredicate(String masterNodeName) {
            this.masterNodeName = masterNodeName;
        }

        @Override
        public boolean apply(NodeAndClient nodeAndClient) {
            return masterNodeName.equals(nodeAndClient.name);
        }
    }

    private static final class ClientNodePredicate implements Predicate<NodeAndClient> {

        @Override
        public boolean apply(NodeAndClient nodeAndClient) {
            return nodeAndClient.node.settings().getAsBoolean("node.client", false);
        }
    }

    @Override
    public synchronized Iterator<Client> iterator() {
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
    }

    /**
     * Returns a predicate that only accepts settings of nodes with one of the given names.
     */
    public static Predicate<Settings> nameFilter(String... nodeName) {
        return new NodeNamePredicate(new HashSet<String>(Arrays.asList(nodeName)));
    }

    private static final class NodeNamePredicate implements Predicate<Settings> {
        private final HashSet<String> nodeNames;


        public NodeNamePredicate(HashSet<String> nodeNames) {
            this.nodeNames = nodeNames;
        }

        @Override
        public boolean apply(Settings settings) {
            return nodeNames.contains(settings.get("name"));

        }
    }


    public static abstract class RestartCallback {

        /**
         * Executed once the give node name has been stopped.
         */
        public Settings onNodeStopped(String nodeName) throws Exception {
            return ImmutableSettings.EMPTY;
        }

        /**
         * Executed for each node before the <tt>n+1</tt> node is restarted. The given client is
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


        /**
         * If this returns <code>false</code> the node with the given node name will not be restarted. It will be
         * closed and removed from the cluster. Returns <code>true</code> by default.
         */
        public boolean doRestart(String nodeName) {
            return true;
        }
    }
}
