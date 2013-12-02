package org.cratedb.doctests;

import org.cratedb.test.integration.CrateIntegrationTest;
import org.cratedb.test.integration.DoctestTestCase;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.cratedb.test.integration.PathAccessor.stringFromPath;


@CrateIntegrationTest.ClusterScope(scope = CrateIntegrationTest.Scope.SUITE, numNodes = 0, transportClientRatio = 0)
public class DocTest extends DoctestTestCase {

    static {
        ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(true);
    }

    String node1;
    String node2;

    @Before
    public void setUpNodes() throws Exception {

        Settings s1 = ImmutableSettings.settingsBuilder()
                .put("cluster.name", "a")
                .put("http.port", 9202)
                .put("transport.tcp.port", 9302)
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 0)
                .build();

        node1 = cluster().startNode(s1);
        client(node1).admin().indices().prepareDelete().execute().actionGet();
        client(node1).admin().indices().prepareCreate("users").setSettings(
            ImmutableSettings.builder().loadFromClasspath("essetup/settings/test_b.json").build())
            .addMapping("d", stringFromPath("/essetup/mappings/test_b.json", getClass())).execute().actionGet();

        waitForRelocation(ClusterHealthStatus.GREEN);

        loadBulk(client(node1), "/essetup/data/test_b.json", getClass());
        client(node1).admin().indices().prepareRefresh("users").execute().actionGet();

        Settings s2 = ImmutableSettings.settingsBuilder()
                .put("http.port", 9203)
                .put("transport.tcp.port", 9303)
                .put("cluster.name", "b")
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 0)
                .build();
        node2 = cluster().startNode(s2);
        client(node2).admin().indices().prepareDelete().execute().actionGet();
    }

    @After
    public void tearDownNodes() throws Exception {
        cluster().stopNode(node1);
        cluster().stopNode(node2);
    }

    @Test
    public void testSearchInto() throws Exception {
        execDocFile("search_into.rst", getClass());
    }

    @Test
    public void testReindex() throws Exception {
        execDocFile("reindex.rst", getClass());
    }

}
