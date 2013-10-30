package org.cratedb.service;

import org.cratedb.action.sql.ParsedStatement;
import org.cratedb.action.sql.SQLAction;
import org.cratedb.action.sql.SQLRequest;
import org.cratedb.action.sql.SQLResponse;
import org.elasticsearch.ElasticsearchTestCase;
import org.elasticsearch.common.network.NetworkUtils;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.internal.InternalNode;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

public class InformationSchemaQueryTest extends ElasticsearchTestCase {

    static {
        ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(true);
    }

    private static final Settings defaultSettings = ImmutableSettings
        .settingsBuilder()
        .put("cluster.name", "test-cluster-" + NetworkUtils.getLocalAddress().getHostName() + "CHILD_VM=[" + CHILD_VM_ID +"]")
        .build();

    private static InternalNode node;
    private static SQLParseService parseService;
    private static InformationSchemaService informationSchemaService;
    private SQLResponse response;


    @BeforeClass
    public static void setUpClass() {
        synchronized (InformationSchemaQueryTest.class) {
            String settingsSource = InformationSchemaQueryTest.class.getName().replace('.', '/') + ".yml";
            Settings finalSettings = settingsBuilder()
                .loadFromClasspath(settingsSource)
                .put(defaultSettings)
                .put("name", "node1")
                .put("discovery.id.seed", randomLong())
                .build();

            if (finalSettings.get("gateway.type") == null) {
                // default to non gateway
                finalSettings = settingsBuilder().put(finalSettings).put("gateway.type", "none").build();
            }
            if (finalSettings.get("cluster.routing.schedule") != null) {
                // decrease the routing schedule so new nodes will be added quickly
                finalSettings = settingsBuilder().put(finalSettings).put("cluster.routing.schedule", "50ms").build();
            }

            node = (InternalNode)nodeBuilder().settings(finalSettings).build();
            node.start();
            parseService = node.injector().getInstance(SQLParseService.class);
            informationSchemaService = node.injector().getInstance(InformationSchemaService.class);

            node.client().execute(SQLAction.INSTANCE,
                new SQLRequest("create table t1 (col1 integer, col2 string) clustered into 7 shards")).actionGet();
            node.client().execute(SQLAction.INSTANCE,
                new SQLRequest("create table t2 (col1 integer, col2 string) clustered into 10 shards")).actionGet();
            node.client().execute(SQLAction.INSTANCE,
                new SQLRequest("create table t3 (col1 integer, col2 string) replicas 8")).actionGet();
        }
    }


    @AfterClass
    public static void tearDownClass() {
        synchronized (InformationSchemaQueryTest.class) {
            node.close();
        }
    }

    private void exec(String statement) throws Exception {
        exec(statement, new Object[0]);
    }

    private void exec(String statement, Object[] args) throws Exception {
        ParsedStatement stmt = parseService.parse(statement, args);
        response = informationSchemaService.execute(stmt).actionGet();
    }

    @Test
    public void testSelectStar() throws Exception {
        // select *
        exec("select * from information_schema.tables");
        assertEquals(3L, response.rowCount());
    }


    @Test
    public void testNotEqualsString() throws Exception {
        exec("select * from information_schema.tables where table_name != 't1'");
        assertEquals(2L, response.rowCount());
        assertTrue(!response.rows()[0][0].equals("t1"));
        assertTrue(!response.rows()[1][0].equals("t1"));
    }

    @Test
    public void testNotEqualsNumber() throws Exception {
        exec("select * from information_schema.tables where number_of_shards != 7");
        assertEquals(2L, response.rowCount());
        assertTrue(response.rows()[0][1] != 7);
        assertTrue(response.rows()[1][1] != 7);
    }

    @Test
    public void testEqualsNumber() throws Exception {
        exec("select * from information_schema.tables where number_of_shards = 7");
        assertEquals(1L, response.rowCount());
        assertEquals("t1", response.rows()[0][0]);
    }

    @Test
    public void testEqualsString() throws Exception {
        exec("select table_name from information_schema.tables where table_name = 't1'");
        assertEquals(1L, response.rowCount());
        assertEquals("t1", response.rows()[0][0]);
    }

    @Test
    public void testGtNumber() throws Exception {
        exec("select * from information_schema.tables where number_of_shards > 7");
        assertEquals(1L, response.rowCount());
        assertEquals("t2", response.rows()[0][0]);
    }

    @Test
    public void testOrderByStringAndLimit() throws Exception {
        exec("select table_name, number_of_shards, number_of_replicas from information_schema.tables " +
                 " order by table_name desc limit 2");
        assertEquals(2L, response.rowCount());
        assertEquals("t3", response.rows()[0][0]);
        assertEquals("t2", response.rows()[1][0]);
    }

    @Test
    public void testOrderByNumberAndLimit() throws Exception {
        exec("select table_name, number_of_shards, number_of_replicas from information_schema.tables " +
                 " order by number_of_shards desc limit 2");
        assertEquals(2L, response.rowCount());
        assertEquals(10, response.rows()[0][1]);
        assertEquals("t2", response.rows()[0][0]);
        assertEquals(7, response.rows()[1][1]);
        assertEquals("t1", response.rows()[1][0]);
    }

    @Test
    public void testLimit1() throws Exception {
        exec("select * from information_schema.tables limit 1");
        assertEquals(1L, response.rowCount());
    }
}
