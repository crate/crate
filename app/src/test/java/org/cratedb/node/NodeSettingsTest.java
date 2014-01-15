package org.cratedb.node;

import junit.framework.TestCase;
import org.cratedb.Constants;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.node.internal.InternalSettingsPreparer;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;


public class NodeSettingsTest extends TestCase {

    @Rule
    public TemporaryFolder tmp = new TemporaryFolder();

    static {
        ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(true);
    }

    protected Node node;
    protected Client client;

    private void doSetup() throws IOException {
        doSetup(true);
    }

    private void doSetup(boolean localNode) throws IOException {
        tmp.create();
        ImmutableSettings.Builder builder = ImmutableSettings.settingsBuilder()
            .put("node.name", "node-test")
            .put("node.data", true)
            .put("index.store.type", "memory")
            .put("index.store.fs.memory.enabled", "true")
            .put("gateway.type", "none")
            .put("path.data", new File(tmp.getRoot(), "data"))
            .put("path.work", new File(tmp.getRoot(), "work"))
            .put("path.logs", new File(tmp.getRoot(), "logs"))
            .put("index.number_of_shards", "1")
            .put("index.number_of_replicas", "0")
            .put("cluster.routing.schedule", "50ms")
            .put("node.local", localNode);
        Tuple<Settings,Environment> settingsEnvironmentTuple = InternalSettingsPreparer.prepareSettings(builder.build(), true);
        node = NodeBuilder.nodeBuilder()
            .settings(settingsEnvironmentTuple.v1())
            .loadConfigSettings(false)
            .build();
        node.start();
        client = node.client();
        client.admin().indices().prepareCreate("test").execute().actionGet();
    }

    @After
    public void tearDown() throws IOException {
        if (client != null) {
            client.admin().indices().prepareDelete("test").execute().actionGet();
            client = null;
        }
        if (node != null) {
            node.stop();
            node = null;
        }


    }

    /**
     * Deleting all indexes must be deactivated by default
     */
    @Test(expected = ElasticSearchIllegalArgumentException.class)
    public void testDeleteAll() throws IOException {
        doSetup();
        client.admin().indices().prepareDelete().execute();
    }

    /**
     * The default cluster name is "crate" if not set differently in crate settings
     */
    @Test
    public void testClusterName() throws IOException {
        doSetup();
        assertEquals("crate",
            client.admin().cluster().prepareHealth().
                setWaitForGreenStatus().execute().actionGet().getClusterName());
    }

    /**
     * The default cluster name is "crate" if not set differently in crate settings
     */
    @Test
    public void testClusterNameSystemProp() throws IOException {
        System.setProperty("es.cluster.name", "system");
        doSetup();
        assertEquals("system",
            client.admin().cluster().prepareHealth().
                setWaitForGreenStatus().execute().actionGet().getClusterName());
        System.clearProperty("es.cluster.name");

    }

    /**
     * The location of the used config file might be defined with the system
     * property crate.config. The configuration located at crate's default
     * location will get ignored.
     *
     * @throws IOException
     */
    @Test
    public void testCustomYMLSettings() throws IOException {

        File custom = new File("custom");
        custom.mkdir();
        File file = new File(custom, "custom.yml");
        FileWriter customWriter = new FileWriter(file, false);
        customWriter.write("cluster.name: custom");
        customWriter.close();

        System.setProperty("es.config", "custom/custom.yml");

        doSetup();

        file.delete();
        custom.delete();
        System.clearProperty("es.config");

        assertEquals("custom",
            client.admin().cluster().prepareHealth().
                setWaitForGreenStatus().execute().actionGet().getClusterName());
    }

    @Test
    public void testDefaultPorts() throws IOException {
        doSetup(false);

        assertEquals(
                Constants.HTTP_PORT_RANGE,
                node.settings().get("http.port")
        );
        assertEquals(
                Constants.TRANSPORT_PORT_RANGE,
                node.settings().get("transport.tcp.port")
        );
    }
}
