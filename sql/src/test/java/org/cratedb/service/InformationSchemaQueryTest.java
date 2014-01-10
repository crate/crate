package org.cratedb.service;

import org.cratedb.SQLTransportIntegrationTest;
import org.cratedb.action.sql.ParsedStatement;
import org.cratedb.action.sql.SQLResponse;
import org.cratedb.sql.TableUnknownException;
import org.cratedb.test.integration.CrateIntegrationTest;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;


@CrateIntegrationTest.ClusterScope(scope = CrateIntegrationTest.Scope.SUITE, numNodes = 2)
public class InformationSchemaQueryTest extends SQLTransportIntegrationTest {

    private static SQLParseService parseService;
    private static InformationSchemaService informationSchemaService;
    private SQLResponse response;
    private boolean createdTables = false;

    @Before
    public void tableCreation() throws Exception {
        synchronized (InformationSchemaQueryTest.class) {
            if (!createdTables) {

                parseService = cluster().getInstance(SQLParseService.class);
                informationSchemaService = cluster().getInstance(InformationSchemaService.class);

                execute("create table t1 (col1 integer, col2 string) clustered into 7 shards");
                execute("create table t2 (col1 integer, col2 string) clustered into 10 shards");
                execute("create table t3 (col1 integer, col2 string) replicas 8");

                createdTables = true;
            }
        }
    }


    @AfterClass
    public static void tearDownClass() {
        synchronized (InformationSchemaQueryTest.class) {
            parseService = null;
            informationSchemaService.doClose();
            informationSchemaService = null;
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
    public void testGroupByOnInformationSchema() throws Exception {
        exec("select count(*) from information_schema.columns group by table_name order by count(*) desc");
        assertEquals(3L, response.rowCount());

        exec("select count(*) from information_schema.columns group by column_name order by count(*) desc");
        assertEquals(2L, response.rowCount());
        assertEquals(3L, response.rows()[0][0]);
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
        assertArrayEquals(new String[]{"unknown", "routine_name"}, response.cols());

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

    @Test
    public void testIgnoreClosedTables() throws Exception {
        execute("drop table t1");
        execute("drop table t2");
        execute("create table t1 (col1 integer, col2 string) replicas 0");
        client().admin().indices().close(new CloseIndexRequest("t3"));
        ensureGreen();
        exec("select * from information_schema.tables");
        assertEquals(1L, response.rowCount());
        exec("select * from information_schema.columns where table_name = 't3'");
        assertEquals(0, response.rowCount());
        exec("select * from information_schema.table_constraints where table_name = 't3'");
        assertEquals(0, response.rowCount());
        exec("select * from information_schema.indices where table_name = 't3'");
        assertEquals(0, response.rowCount());
    }

}
