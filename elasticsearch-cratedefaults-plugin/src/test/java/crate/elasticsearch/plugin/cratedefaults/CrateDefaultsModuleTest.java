package crate.elasticsearch.plugin.cratedefaults;

import static com.github.tlrx.elasticsearch.test.EsSetup.createIndex;
import static com.github.tlrx.elasticsearch.test.EsSetup.deleteAll;

import com.github.tlrx.elasticsearch.test.EsSetup;
import com.github.tlrx.elasticsearch.test.support.junit.runners.ElasticsearchRunner;
import junit.framework.Assert;
import junit.framework.TestCase;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Properties;

public class CrateDefaultsModuleTest {

    protected EsSetup esSetup;

    private void doSetUp() {
        esSetup = new EsSetup();
        esSetup.execute(createIndex("test"));
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
        Assert.assertEquals("crate",
                esSetup.client().admin().cluster().prepareHealth().
                        setWaitForGreenStatus().execute().actionGet().getClusterName());
    }

    /**
     * A crate.yml file can be used to override crate settings in YML format.
     * That way it is for example possible to override the cluster name.
     * @throws IOException
     */
    @Test
    public void testCrateYMLSettings() throws IOException {
        File file = new File("crate.yml");
        FileWriter writer = new FileWriter(file, false);
        writer.write("cluster.name: myYMLCluster");
        writer.close();
        doSetUp();
        file.delete();
        Assert.assertEquals("myYMLCluster",
                esSetup.client().admin().cluster().prepareHealth().
                        setWaitForGreenStatus().execute().actionGet().getClusterName());
    }

    /**
     * A crate.json file can be used to override settings in JSON format.
     * That way it is for example possible to override the cluster name.
     * @throws IOException
     */
    @Test
    public void testCrateJSONSettings() throws IOException {
        File file = new File("crate.json");
        FileWriter writer = new FileWriter(file, false);
        writer.write("{\"cluster\": {\"name\": \"myJSONCluster\"}}");
        writer.close();
        doSetUp();
        file.delete();
        Assert.assertEquals("myJSONCluster",
                esSetup.client().admin().cluster().prepareHealth().
                        setWaitForGreenStatus().execute().actionGet().getClusterName());
    }

    /**
     * A crate.properties file can be used to override settings in .properties format.
     * That way it is for example possible to override the cluster name.
     * @throws IOException
     */
    @Test
    public void testCratePropertiesSettings() throws IOException {
        Properties prop = new Properties();
        prop.setProperty("cluster.name", "myPropCluster");
        prop.store(new FileOutputStream("crate.properties"), null);
        doSetUp();
        new File("crate.properties").delete();
        Assert.assertEquals("myPropCluster",
                esSetup.client().admin().cluster().prepareHealth().
                        setWaitForGreenStatus().execute().actionGet().getClusterName());
    }



}