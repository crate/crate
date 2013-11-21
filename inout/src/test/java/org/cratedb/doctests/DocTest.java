package org.cratedb.doctests;

import com.github.tlrx.elasticsearch.test.EsSetup;
import org.apache.lucene.util.AbstractRandomizedTest;
import org.cratedb.test.integration.DoctestTestCase;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static com.github.tlrx.elasticsearch.test.EsSetup.deleteAll;
import static com.github.tlrx.elasticsearch.test.EsSetup.fromClassPath;


@AbstractRandomizedTest.IntegrationTests
public class DocTest extends DoctestTestCase {

    static {
        ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(true);
    }

    StoreEsSetup esSetup, esSetup2;

    @Before
    public void setUpNodes() throws Exception {

        Settings s1 = ImmutableSettings.settingsBuilder()
                .put("cluster.name", "a")
                .put("http.port", 9202)
                .put("transport.tcp.port", 9302)
                .put("node.local", false)
                .build();

        esSetup = new StoreEsSetup(s1);
        esSetup.execute(deleteAll(), EsSetup.createIndex("users").withSettings(
                fromClassPath("essetup/settings/test_b.json")).withMapping("d",
                fromClassPath("essetup/mappings/test_b.json")).withData(
                fromClassPath("essetup/data/test_b.json")));
        esSetup.client().admin().indices().prepareRefresh("users").execute().actionGet();

        Settings s2 = ImmutableSettings.settingsBuilder()
                .put("http.port", 9203)
                .put("transport.tcp.port", 9303)
                .put("cluster.name", "b")
                .put("node.local", false)
                .build();
        esSetup2 = new StoreEsSetup(s2);
        esSetup2.execute(deleteAll());
        Thread.sleep(100);
    }

    @After
    public void tearDownNodes() throws Exception {
        esSetup.terminate();
        esSetup2.terminate();
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
