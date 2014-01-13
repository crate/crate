package org.cratedb;

import org.cratedb.test.integration.CrateIntegrationTest;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.junit.Test;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;

@CrateIntegrationTest.ClusterScope(scope = CrateIntegrationTest.Scope.TEST, numNodes = 0)
public class ClusterIdServiceTest extends CrateIntegrationTest {

    @Test
    public void testClusterIdGeneration() throws Exception {
        Settings localSettings = settingsBuilder()
                .put("discovery.type", "local").build();
        String node_0 = cluster().startNode(localSettings);
        ensureGreen();

        ClusterService clusterService = cluster().getInstance(ClusterService.class, node_0);
        assertNotNull(clusterService.state().metaData().persistentSettings().get(ClusterIdService.clusterIdSettingsKey));
    }

    @Test
    public void testClusterIdPersistence() throws Exception {
        Settings localSettings = settingsBuilder()
                .put("discovery.type", "local").build();
        String node_0 = cluster().startNode(localSettings);
        ensureGreen();

        ClusterService clusterService = cluster().getInstance(ClusterService.class, node_0);
        String clusterId = clusterService.state().metaData().persistentSettings().get(ClusterIdService.clusterIdSettingsKey);

        cluster().stopNode(node_0);
        node_0 = cluster().startNode(localSettings);
        ensureGreen();

        clusterService = cluster().getInstance(ClusterService.class, node_0);
        String clusterId2 = clusterService.state().metaData().persistentSettings().get(ClusterIdService.clusterIdSettingsKey);

        assertEquals(clusterId, clusterId2);
    }

    @Test
    public void testClusterIdDistribution() throws Exception {
        Settings localSettings = settingsBuilder()
                .put("discovery.type", "zen").build();
        String node_0 = cluster().startNode(localSettings);
        ensureGreen();

        ClusterIdService clusterIdServiceNode0 = cluster().getInstance(ClusterIdService.class, node_0);
        assertNotNull(clusterIdServiceNode0.clusterId());

        String node_1 = cluster().startNode(localSettings);
        ensureGreen();

        ClusterIdService clusterIdServiceNode1 = cluster().getInstance(ClusterIdService.class, node_1);
        assertNotNull(clusterIdServiceNode1.clusterId());

        assertEquals(clusterIdServiceNode0.clusterId(), clusterIdServiceNode1.clusterId());
    }

}
