package crate.elasticsearch;

import static com.github.tlrx.elasticsearch.test.EsSetup.createIndex;
import static com.github.tlrx.elasticsearch.test.EsSetup.deleteAll;
import static com.github.tlrx.elasticsearch.test.EsSetup.fromClassPath;

import com.github.tlrx.elasticsearch.test.EsSetup;
import junit.framework.TestCase;
import org.junit.*;

public abstract class TestSetup extends TestCase {

    // two nodes to test object serialization.
    protected EsSetup esSetup, esSetup2;

    @Before
    public void setUp() {

        esSetup = new EsSetup();
        esSetup2 = new EsSetup();
        esSetup2.execute(deleteAll());

        esSetup.execute(deleteAll(), createIndex("locations").withSettings(
                fromClassPath("essetup/settings/test_a.json")).withMapping("location",
                fromClassPath("essetup/mappings/test_a.json")).withData(
                fromClassPath("essetup/data/test_a.json")));
        esSetup.client().admin().indices().prepareRefresh("locations").execute().actionGet();
    }

    @After
    public void tearDown() {
        esSetup.terminate();
        if (esSetup2 != null) {
            esSetup2.terminate();
        }
    }
}
