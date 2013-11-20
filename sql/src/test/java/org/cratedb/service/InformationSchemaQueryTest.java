package org.cratedb.service;

import org.cratedb.action.sql.ParsedStatement;
import org.cratedb.action.sql.SQLAction;
import org.cratedb.action.sql.SQLRequest;
import org.cratedb.action.sql.SQLResponse;
import org.cratedb.sql.TableUnknownException;
import org.cratedb.test.integration.AbstractCrateNodesTests;
import org.elasticsearch.common.network.NetworkUtils;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.internal.InternalNode;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;

public class InformationSchemaQueryTest extends AbstractCrateNodesTests {

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


    @Before
    public void setUpNode() throws Exception {
        synchronized (InformationSchemaQueryTest.class) {
            if (node == null) {
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

                node = (InternalNode)buildNode("node1", finalSettings);
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
    }


    @AfterClass
    public static void tearDownClass() {
        synchronized (InformationSchemaQueryTest.class) {
            parseService = null;
            informationSchemaService.doClose();
            informationSchemaService = null;
            node.close();
            node = null;
        }
    }

    private void exec(String statement) throws Exception {
        exec(statement, new Object[0]);
    }

    private void exec(String statement, Object[] args) throws Exception {
        ParsedStatement stmt = parseService.parse(statement, args);
        response = informationSchemaService.execute(stmt, System.currentTimeMillis()).actionGet();
    }

    @Test
    public void testSelectStar() throws Exception {
        exec("select * from information_schema.tables");
        assertEquals(3L, response.rowCount());
    }

    @Test
    public void testLike() throws Exception {
        exec("select * from information_schema.tables where table_name like 't%'");
        assertEquals(3L, response.rowCount());
    }

    @Test
    public void testIsNull() throws Exception {
        exec("select * from information_schema.tables where table_name is null");
        assertEquals(0L, response.rowCount());
    }

    @Test
    public void testIsNotNull() throws Exception {
        exec("select * from information_schema.tables where table_name is not null");
        assertEquals(3L, response.rowCount());
    }

    @Test
    public void testWhereAnd() throws Exception {
        exec("select * from information_schema.tables where table_name='t1' and " +
                "number_of_shards > 0");
        assertEquals(1L, response.rowCount());
        assertEquals("t1", response.rows()[0][0]);
    }

    @Test
    public void testWhereAnd2() throws Exception {
        exec("select * from information_schema.tables where number_of_shards >= 7 and " +
                "number_of_replicas < 8 order by table_name asc");
        assertEquals(2L, response.rowCount());
        assertEquals("t1", response.rows()[0][0]);
        assertEquals("t2", response.rows()[1][0]);
    }

    @Test
    public void testWhereAnd3() throws Exception {
        exec("select * from information_schema.tables where table_name is not null and " +
                "number_of_shards > 6 order by table_name asc");
        assertEquals(2L, response.rowCount());
        assertEquals("t1", response.rows()[0][0]);
        assertEquals("t2", response.rows()[1][0]);
    }

    @Test
    public void testWhereOr() throws Exception {
        exec("select * from information_schema.tables where table_name='t1' or table_name='t3' " +
                "order by table_name asc");
        assertEquals(2L, response.rowCount());
        assertEquals("t1", response.rows()[0][0]);
        assertEquals("t3", response.rows()[1][0]);
    }

    @Test
    public void testWhereOr2() throws Exception {
        exec("select * from information_schema.tables where table_name='t1' or table_name='t3' " +
                "or table_name='t2'" +
                "order by table_name desc");
        assertEquals(3L, response.rowCount());
        assertEquals("t3", response.rows()[0][0]);
        assertEquals("t2", response.rows()[1][0]);
        assertEquals("t1", response.rows()[2][0]);
    }

    @Test
    public void testWhereIn() throws Exception {
        exec("select * from information_schema.tables where table_name in ('t1', 't2') order by table_name asc");
        assertEquals(2L, response.rowCount());
        assertEquals("t1", response.rows()[0][0]);
        assertEquals("t2", response.rows()[1][0]);
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

    @Test
    public void testSelectNonExistingColumn() throws Exception {
        exec("select routine_non_existing from information_schema.routines");
        assertEquals(102L, response.rowCount());
        assertEquals("routine_non_existing", response.cols()[0]);
        for (int i=0; i<response.rows().length;i++) {
            assertNull(response.rows()[i][0]);
        }
    }

    @Test
    public void selectNonExistingAndExistingColumns() throws Exception {
        exec("select \"unknown\", routine_name from information_schema.routines order by " +
                "routine_name asc");
        assertEquals(102L, response.rowCount());
        assertEquals("unknown", response.cols()[0]);
        assertEquals("routine_name", response.cols()[1]);
        for (int i=0; i<response.rows().length;i++) {
            assertNull(response.rows()[i][0]);
        }
    }

    @Test
    public void selectWhereNonExistingColumn() throws Exception {
        exec("select * from information_schema.routines where something > 0");
        assertEquals(0L, response.rowCount());
    }

    @Test
    public void selectWhereNonExistingColumnIsNull() throws Exception {
        exec("select * from information_schema.routines where something IS NULL");
        assertEquals(102L, response.rowCount());  // something does not exist,
        // so we get all documents
    }

    @Test
    public void selectWhereNonExistingColumnWhereIn() throws Exception {
        exec("select * from information_schema.routines where something IN(1,2,3)");
        assertEquals(0L, response.rowCount());
    }

    @Test
    public void selectWhereNonExistingColumnLike() throws Exception {

        exec("select * from information_schema.routines where something Like '%bla'");
        assertEquals(0L, response.rowCount());
    }

    @Test
    public void selectWhereNonExistingColumnMatchFunction() throws Exception {
        exec("select * from information_schema.routines where match(something, 'bla')");
        assertEquals(0L, response.rowCount());
    }

    @Test
    public void selectWhereExistingColumnMatchFunction() throws Exception {
        exec("select * from information_schema.routines where match(routine_type, 'ANALYZER')");
        assertEquals(42L, response.rowCount());
    }

    @Test
    public void selectOrderByNonExistingColumn() throws Exception {
        exec("SELECT * from information_schema.routines");
        SQLResponse responseWithoutOrder = response;
        exec("SELECT * from information_schema.routines order by something");
        assertEquals(responseWithoutOrder.rowCount(), response.rowCount());
        for (int i=0;i<response.rowCount();i++) {
            assertArrayEquals(responseWithoutOrder.rows()[i], response.rows()[i]);
        }
    }

    @Test( expected = TableUnknownException.class )
    public void testSelectUnkownTableFromInformationSchema() throws Exception {
        exec("select * from information_schema.non_existent");
    }

}
