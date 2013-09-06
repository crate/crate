package org.cratedb.plugin.crate;

import com.github.tlrx.elasticsearch.test.EsSetup;
import com.github.tlrx.elasticsearch.test.provider.LocalClientProvider;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import static com.github.tlrx.elasticsearch.test.EsSetup.createIndex;
import static com.github.tlrx.elasticsearch.test.EsSetup.deleteAll;
import static junit.framework.Assert.assertEquals;

public class CrateModuleTest {

    protected EsSetup esSetup;

    private void doSetUp() {
        esSetup = new CustomESSetup();
        esSetup.execute(createIndex("test"));
    }

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
        esSetup.terminate();
    }

    /**
     * Deleting all indexes must be deactivated by default
     */
    @Test(expected = ElasticSearchIllegalArgumentException.class)
    public void testDeleteAll() {
        doSetUp();
        esSetup.execute(deleteAll());
    }

    /**
     * The default cluster name is "crate" if not set differently in crate settings
     */
    @Test
    public void testClusterName() {
        doSetUp();
        assertEquals("crate",
                esSetup.client().admin().cluster().prepareHealth().
                        setWaitForGreenStatus().execute().actionGet().getClusterName());
    }

    /**
     * The default cluster name is "crate" if not set differently in crate settings
     */
    @Test
    public void testClusterNameSystemProp() {
        System.setProperty("es.cluster.name", "system");
        doSetUp();
        assertEquals("system",
                esSetup.client().admin().cluster().prepareHealth().
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

        doSetUp();
        file.delete();
        custom.delete();
        System.clearProperty("es.config");

        assertEquals("custom",
                esSetup.client().admin().cluster().prepareHealth().
                        setWaitForGreenStatus().execute().actionGet().getClusterName());
    }

    /**
     * Custom implementation of EsSetup to use DefaultClientProvider
     */
    class CustomESSetup extends EsSetup {

        protected CustomESSetup() {
            super(new DefaultClientProvider());
        }

    }

    /**
     * Custom implementation of LocalClientProvider to make sure cluster.name is not set
     */
    class DefaultClientProvider extends LocalClientProvider {


        protected Settings buildNodeSettings() {
            // Build settings
            ImmutableSettings.Builder builder = ImmutableSettings.settingsBuilder()
                    .put("node.name", "node-test")
                    .put("node.data", true)
                    .put("index.store.type", "memory")
                    .put("index.store.fs.memory.enabled", "true")
                    .put("gateway.type", "none")
                    .put("path.data", "./target/elasticsearch-test/data")
                    .put("path.work", "./target/elasticsearch-test/work")
                    .put("path.logs", "./target/elasticsearch-test/logs")
                    .put("index.number_of_shards", "1")
                    .put("index.number_of_replicas", "0")
                    .put("cluster.routing.schedule", "50ms")
                    .put("node.local", true);

            return builder.build();
        }
    }
}
