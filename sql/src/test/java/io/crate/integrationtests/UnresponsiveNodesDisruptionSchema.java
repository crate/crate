package io.crate.integrationtests;

import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.disruption.ServiceDisruptionScheme;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TransportService;

import java.util.*;

import static org.junit.Assert.assertFalse;

public abstract class UnresponsiveNodesDisruptionSchema implements ServiceDisruptionScheme {

    private final Set<String> nodesToDisturb;
    private volatile boolean autoExpand;
    protected final Random random;
    protected volatile InternalTestCluster cluster;
    private volatile boolean activeDisruption = false;


    public UnresponsiveNodesDisruptionSchema(Random random) {
        this.random = new Random(random.nextLong());
        nodesToDisturb = new HashSet<>();
        autoExpand = true;
    }

    public UnresponsiveNodesDisruptionSchema(String node, Random random) {
        this(random);
        nodesToDisturb.add(node);
        autoExpand = false;
    }

    public Collection<String> getNodesToDisturb() {
        return Collections.unmodifiableCollection(nodesToDisturb);
    }

    @Override
    public void applyToCluster(InternalTestCluster cluster) {
        this.cluster = cluster;
        if (autoExpand) {
            for (String node : cluster.getNodeNames()) {
                applyToNode(node, cluster);
            }
        }
    }

    @Override
    public void removeFromCluster(InternalTestCluster cluster) {
        stopDisrupting();
    }

    @Override
    public void removeAndEnsureHealthy(InternalTestCluster cluster) {
        removeFromCluster(cluster);
        ensureNodeCount(cluster);
    }

    protected void ensureNodeCount(InternalTestCluster cluster) {
        assertFalse("cluster failed to form after disruption was healed", cluster.client().admin().cluster().prepareHealth()
            .setWaitForNodes("" + cluster.size())
            .setWaitForRelocatingShards(0)
            .get().isTimedOut());
    }

    @Override
    public synchronized void applyToNode(String node, InternalTestCluster cluster) {
        if (!autoExpand || nodesToDisturb.contains(node)) {
            return;
        }
        if (nodesToDisturb.isEmpty()) {
            nodesToDisturb.add(node);
        }
    }

    @Override
    public synchronized void removeFromNode(String node, InternalTestCluster cluster) {
        MockTransportService transportService = (MockTransportService) cluster.getInstance(TransportService.class, node);
        if (nodesToDisturb.contains(node)) {
            nodesToDisturb.remove(node);
        } else {
            return;
        }
        removeDisruption(transportService);
    }

    @Override
    public synchronized void testClusterClosed() {

    }

    @Override
    public synchronized void startDisrupting() {
        if (nodesToDisturb.size() == 0) {
            return;
        }
        activeDisruption = true;
        for (String node : nodesToDisturb) {
            MockTransportService transportService = (MockTransportService) cluster.getInstance(TransportService.class, node);
            applyDisruption(transportService);
        }
    }

    @Override
    public synchronized void stopDisrupting() {
        if (nodesToDisturb.size() == 0 || !activeDisruption) {
            return;
        }
        for (String node : nodesToDisturb) {
            MockTransportService transportService = (MockTransportService) cluster.getInstance(TransportService.class, node);
            removeDisruption(transportService);
        }
        activeDisruption = false;
    }

    private void applyDisruption(MockTransportService transportService) {
        transportService.addUnresponsiveRule(transportService);
    }

    private void removeDisruption(MockTransportService transportService) {
        transportService.clearAllRules();
    }
}
