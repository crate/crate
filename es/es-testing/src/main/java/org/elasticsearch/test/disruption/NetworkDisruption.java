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

package org.elasticsearch.test.disruption;

import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.NodeConnectionsService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.TransportService;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.function.BiConsumer;

import static org.junit.Assert.assertFalse;

/**
 * Network disruptions are modeled using two components:
 * 1) the {@link DisruptedLinks} represents the links in the network that are to be disrupted
 * 2) the {@link NetworkLinkDisruptionType} represents the failure mode that is to be applied to the links
 */
public class NetworkDisruption implements ServiceDisruptionScheme {

    private final Logger logger = Loggers.getLogger(NetworkDisruption.class);

    private final DisruptedLinks disruptedLinks;
    private final NetworkLinkDisruptionType networkLinkDisruptionType;

    protected volatile InternalTestCluster cluster;
    protected volatile boolean activeDisruption = false;

    public NetworkDisruption(DisruptedLinks disruptedLinks, NetworkLinkDisruptionType networkLinkDisruptionType) {
        this.disruptedLinks = disruptedLinks;
        this.networkLinkDisruptionType = networkLinkDisruptionType;
    }

    public DisruptedLinks getDisruptedLinks() {
        return disruptedLinks;
    }

    public NetworkLinkDisruptionType getNetworkLinkDisruptionType() {
        return networkLinkDisruptionType;
    }

    @Override
    public void applyToCluster(InternalTestCluster cluster) {
        this.cluster = cluster;
    }

    @Override
    public void removeFromCluster(InternalTestCluster cluster) {
        stopDisrupting();
    }

    @Override
    public void removeAndEnsureHealthy(InternalTestCluster cluster) {
        removeFromCluster(cluster);
        ensureHealthy(cluster);
    }

    /**
     * ensures the cluster is healthy after the disruption
     */
    public void ensureHealthy(InternalTestCluster cluster) {
        assert activeDisruption == false;
        ensureNodeCount(cluster);
        ensureFullyConnectedCluster(cluster);
    }

    /**
     * Ensures that all nodes in the cluster are connected to each other.
     *
     * Some network disruptions may leave nodes that are not the master disconnected from each other.
     * {@link org.elasticsearch.cluster.NodeConnectionsService} will eventually reconnect but it's
     * handy to be able to ensure this happens faster
     */
    public static void ensureFullyConnectedCluster(InternalTestCluster cluster) {
        for (String node: cluster.getNodeNames()) {
            ClusterState stateOnNode = cluster.getInstance(ClusterService.class, node).state();
            cluster.getInstance(NodeConnectionsService.class, node).connectToNodes(stateOnNode.nodes());
        }
    }

    protected void ensureNodeCount(InternalTestCluster cluster) {
        assertFalse("cluster failed to form after disruption was healed", cluster.client().admin().cluster().prepareHealth()
            .setWaitForNodes(String.valueOf(cluster.size()))
            .setWaitForNoRelocatingShards(true)
            .get().isTimedOut());
    }

    @Override
    public synchronized void applyToNode(String node, InternalTestCluster cluster) {

    }

    @Override
    public synchronized void removeFromNode(String node1, InternalTestCluster cluster) {
        logger.info("stop disrupting node (disruption type: {}, disrupted links: {})", networkLinkDisruptionType, disruptedLinks);
        applyToNodes(new String[]{ node1 }, cluster.getNodeNames(), networkLinkDisruptionType::removeDisruption);
        applyToNodes(cluster.getNodeNames(), new String[]{ node1 }, networkLinkDisruptionType::removeDisruption);
    }

    @Override
    public synchronized void testClusterClosed() {

    }

    @Override
    public synchronized void startDisrupting() {
        logger.info("start disrupting (disruption type: {}, disrupted links: {})", networkLinkDisruptionType, disruptedLinks);
        applyToNodes(cluster.getNodeNames(), cluster.getNodeNames(), networkLinkDisruptionType::applyDisruption);
        activeDisruption = true;
    }

    @Override
    public synchronized void stopDisrupting() {
        if (!activeDisruption) {
            return;
        }
        logger.info("stop disrupting (disruption scheme: {}, disrupted links: {})", networkLinkDisruptionType, disruptedLinks);
        applyToNodes(cluster.getNodeNames(), cluster.getNodeNames(), networkLinkDisruptionType::removeDisruption);
        activeDisruption = false;
    }

    /**
     * Applies action to all disrupted links between two sets of nodes.
     */
    private void applyToNodes(String[] nodes1, String[] nodes2, BiConsumer<MockTransportService, MockTransportService> consumer) {
        for (String node1 : nodes1) {
            if (disruptedLinks.nodes().contains(node1)) {
                for (String node2 : nodes2) {
                    if (disruptedLinks.nodes().contains(node2)) {
                        if (node1.equals(node2) == false) {
                            if (disruptedLinks.disrupt(node1, node2)) {
                                consumer.accept(transport(node1), transport(node2));
                            }
                        }
                    }
                }
            }
        }
    }

    @Override
    public TimeValue expectedTimeToHeal() {
        return networkLinkDisruptionType.expectedTimeToHeal();
    }

    private MockTransportService transport(String node) {
        return (MockTransportService) cluster.getInstance(TransportService.class, node);
    }

    @Override
    public String toString() {
        return "network disruption (disruption type: " + networkLinkDisruptionType + ", disrupted links: " + disruptedLinks + ")";
    }

    /**
     * Represents a set of nodes with connections between nodes that are to be disrupted
     */
    public abstract static class DisruptedLinks {
        private final Set<String> nodes;

        protected DisruptedLinks(Set<String>... nodeSets) {
            Set<String> allNodes = new HashSet<>();
            for (Set<String> nodeSet : nodeSets) {
                allNodes.addAll(nodeSet);
            }
            this.nodes = allNodes;
        }

        /**
         * Set of all nodes that can participate in disruptions
         */
        public Set<String> nodes() {
            return nodes;
        }

        /**
         * Returns true iff network should be disrupted between the two nodes
         */
        public abstract boolean disrupt(String node1, String node2);
    }

    /**
     * Creates two partitions with symmetric failures
     */
    public static class TwoPartitions extends DisruptedLinks {

        protected final Set<String> nodesSideOne;
        protected final Set<String> nodesSideTwo;

        public TwoPartitions(String node1, String node2) {
            this(Collections.singleton(node1), Collections.singleton(node2));
        }

        public TwoPartitions(Set<String> nodesSideOne, Set<String> nodesSideTwo) {
            super(nodesSideOne, nodesSideTwo);
            this.nodesSideOne = nodesSideOne;
            this.nodesSideTwo = nodesSideTwo;
            assert nodesSideOne.isEmpty() == false;
            assert nodesSideTwo.isEmpty() == false;
            assert Sets.haveEmptyIntersection(nodesSideOne, nodesSideTwo);
        }

        public static TwoPartitions random(Random random, String... nodes) {
            return random(random, Sets.newHashSet(nodes));
        }

        public static TwoPartitions random(Random random, Set<String> nodes) {
            assert nodes.size() >= 2 : "two partitions topology requires at least 2 nodes";
            Set<String> nodesSideOne = new HashSet<>();
            Set<String> nodesSideTwo = new HashSet<>();
            for (String node : nodes) {
                if (nodesSideOne.isEmpty()) {
                    nodesSideOne.add(node);
                } else if (nodesSideTwo.isEmpty()) {
                    nodesSideTwo.add(node);
                } else if (random.nextBoolean()) {
                    nodesSideOne.add(node);
                } else {
                    nodesSideTwo.add(node);
                }
            }
            return new TwoPartitions(nodesSideOne, nodesSideTwo);
        }

        @Override
        public boolean disrupt(String node1, String node2) {
            if (nodesSideOne.contains(node1) && nodesSideTwo.contains(node2)) {
                return true;
            }
            if (nodesSideOne.contains(node2) && nodesSideTwo.contains(node1)) {
                return true;
            }
            return false;
        }

        public Set<String> getNodesSideOne() {
            return Collections.unmodifiableSet(nodesSideOne);
        }

        public Set<String> getNodesSideTwo() {
            return Collections.unmodifiableSet(nodesSideTwo);
        }

        public Collection<String> getMajoritySide() {
            if (nodesSideOne.size() >= nodesSideTwo.size()) {
                return getNodesSideOne();
            } else {
                return getNodesSideTwo();
            }
        }

        public Collection<String> getMinoritySide() {
            if (nodesSideOne.size() >= nodesSideTwo.size()) {
                return getNodesSideTwo();
            } else {
                return getNodesSideOne();
            }
        }

        @Override
        public String toString() {
            return "two partitions (partition 1: " + nodesSideOne + " and partition 2: " + nodesSideTwo + ")";
        }
    }

    /**
     * Creates two partitions with symmetric failures and a bridge node that can connect to both of the partitions
     */
    public static class Bridge extends DisruptedLinks {

        private final String bridgeNode;
        private final Set<String> nodesSideOne;
        private final Set<String> nodesSideTwo;

        public Bridge(String bridgeNode, Set<String> nodesSideOne, Set<String> nodesSideTwo) {
            super(Collections.singleton(bridgeNode), nodesSideOne, nodesSideTwo);
            this.bridgeNode = bridgeNode;
            this.nodesSideOne = nodesSideOne;
            this.nodesSideTwo = nodesSideTwo;
            assert nodesSideOne.isEmpty() == false;
            assert nodesSideTwo.isEmpty() == false;
            assert Sets.haveEmptyIntersection(nodesSideOne, nodesSideTwo);
            assert nodesSideOne.contains(bridgeNode) == false && nodesSideTwo.contains(bridgeNode) == false;
        }

        public static Bridge random(Random random, String... nodes) {
            return random(random, Sets.newHashSet(nodes));
        }

        public static Bridge random(Random random, Set<String> nodes) {
            assert nodes.size() >= 3 : "bridge topology requires at least 3 nodes";
            String bridgeNode = RandomPicks.randomFrom(random, nodes);
            Set<String> nodesSideOne = new HashSet<>();
            Set<String> nodesSideTwo = new HashSet<>();
            for (String node : nodes) {
                if (node.equals(bridgeNode) == false) {
                    if (nodesSideOne.isEmpty()) {
                        nodesSideOne.add(node);
                    } else if (nodesSideTwo.isEmpty()) {
                        nodesSideTwo.add(node);
                    } else if (random.nextBoolean()) {
                        nodesSideOne.add(node);
                    } else {
                        nodesSideTwo.add(node);
                    }
                }
            }
            return new Bridge(bridgeNode, nodesSideOne, nodesSideTwo);
        }

        @Override
        public boolean disrupt(String node1, String node2) {
            if (nodesSideOne.contains(node1) && nodesSideTwo.contains(node2)) {
                return true;
            }
            if (nodesSideOne.contains(node2) && nodesSideTwo.contains(node1)) {
                return true;
            }
            return false;
        }

        public String getBridgeNode() {
            return bridgeNode;
        }

        public Set<String> getNodesSideOne() {
            return nodesSideOne;
        }

        public Set<String> getNodesSideTwo() {
            return nodesSideTwo;
        }

        public String toString() {
            return "bridge partition (super connected node: [" + bridgeNode + "], partition 1: " + nodesSideOne +
                " and partition 2: " + nodesSideTwo + ")";
        }
    }

    public static class IsolateAllNodes extends DisruptedLinks {

        public IsolateAllNodes(Set<String> nodes) {
            super(nodes);
        }

        @Override
        public boolean disrupt(String node1, String node2) {
            return true;
        }
    }

    /**
     * Abstract class representing various types of network disruptions. Instances of this class override the {@link #applyDisruption}
     * method to apply their specific disruption type to requests that are send from a source to a target node.
     */
    public abstract static class NetworkLinkDisruptionType {

        /**
         * Applies network disruption for requests send from the node represented by the source transport service to the node represented
         * by the target transport service.
         *
         * @param sourceTransportService source transport service from which requests are sent
         * @param targetTransportService target transport service to which requests are sent
         */
        public abstract void applyDisruption(MockTransportService sourceTransportService, MockTransportService targetTransportService);

        /**
         * Removes network disruption that was added by {@link #applyDisruption}.
         *
         * @param sourceTransportService source transport service from which requests are sent
         * @param targetTransportService target transport service to which requests are sent
         */
        public void removeDisruption(MockTransportService sourceTransportService, MockTransportService targetTransportService) {
            sourceTransportService.clearRule(targetTransportService);
        }

        /**
         * Returns expected time to heal after disruption has been removed. Defaults to instant healing.
         */
        public TimeValue expectedTimeToHeal() {
            return TimeValue.timeValueMillis(0);
        }

    }

    /**
     * Simulates a network disconnect. Sending a request from source to target node throws a {@link ConnectTransportException}.
     */
    public static class NetworkDisconnect extends NetworkLinkDisruptionType {

        @Override
        public void applyDisruption(MockTransportService sourceTransportService, MockTransportService targetTransportService) {
            sourceTransportService.addFailToSendNoConnectRule(targetTransportService);
        }

        @Override
        public String toString() {
            return "network disconnects";
        }
    }

    /**
     * Simulates an unresponsive target node by dropping requests sent from source to target node.
     */
    public static class NetworkUnresponsive extends NetworkLinkDisruptionType {

        @Override
        public void applyDisruption(MockTransportService sourceTransportService, MockTransportService targetTransportService) {
            sourceTransportService.addUnresponsiveRule(targetTransportService);
        }

        @Override
        public String toString() {
            return "network unresponsive";
        }
    }

    /**
     * Simulates slow or congested network. Delivery of requests that are sent from source to target node are delayed by a configurable
     * time amount.
     */
    public static class NetworkDelay extends NetworkLinkDisruptionType {

        public static TimeValue DEFAULT_DELAY_MIN = TimeValue.timeValueSeconds(10);
        public static TimeValue DEFAULT_DELAY_MAX = TimeValue.timeValueSeconds(90);

        private final TimeValue delay;

        /**
         * Delays requests by a fixed time value.
         *
         * @param delay time to delay requests
         */
        public NetworkDelay(TimeValue delay) {
            this.delay = delay;
        }

        /**
         * Delays requests by a random but fixed time value between {@link #DEFAULT_DELAY_MIN} and {@link #DEFAULT_DELAY_MAX}.
         *
         * @param random instance to use for randomization of delay
         */
        public static NetworkDelay random(Random random) {
            return random(random, DEFAULT_DELAY_MIN, DEFAULT_DELAY_MAX);
        }

        /**
         * Delays requests by a random but fixed time value between delayMin and delayMax.
         *
         * @param random   instance to use for randomization of delay
         * @param delayMin minimum delay
         * @param delayMax maximum delay
         */
        public static NetworkDelay random(Random random, TimeValue delayMin, TimeValue delayMax) {
            return new NetworkDelay(TimeValue.timeValueMillis(delayMin.millis() == delayMax.millis() ?
                    delayMin.millis() :
                    delayMin.millis() + random.nextInt((int) (delayMax.millis() - delayMin.millis()))));
        }

        @Override
        public void applyDisruption(MockTransportService sourceTransportService, MockTransportService targetTransportService) {
            sourceTransportService.addUnresponsiveRule(targetTransportService, delay);
        }

        @Override
        public TimeValue expectedTimeToHeal() {
            return delay;
        }

        @Override
        public String toString() {
            return "network delays for [" + delay + "]";
        }
    }

}
