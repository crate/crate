package org.cratedb.service;

import org.cratedb.action.sql.ParsedStatement;
import org.cratedb.action.sql.SQLAction;
import org.cratedb.action.sql.SQLRequest;
import org.cratedb.action.sql.SQLResponse;
import org.cratedb.sql.SQLParseException;
import org.elasticsearch.cluster.AbstractZenNodesTests;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.node.internal.InternalNode;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.collection.IsArrayContainingInOrder.arrayContaining;

public class InformationSchemaServiceTest extends AbstractZenNodesTests {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    static {
        ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(true);
    }

    private InternalNode startNode() {
        return (InternalNode) startNode("node1", ImmutableSettings.EMPTY);
    }

    private InternalNode node = null;
    private InformationSchemaService informationSchemaService;
    private SQLParseService parseService;
    private SQLResponse response;

    private void serviceSetup() {
        node.client().execute(SQLAction.INSTANCE,
            new SQLRequest("create table t1 (col1 integer primary key, " +
                    "col2 string) clustered into 7 " +
                    "shards")).actionGet();
        node.client().execute(SQLAction.INSTANCE,
            new SQLRequest("create table t2 (col1 integer primary key, " +
                    "col2 string) clustered into " +
                    "10 shards")).actionGet();
        node.client().execute(SQLAction.INSTANCE,
            new SQLRequest("create table t3 (col1 integer, col2 string) replicas 8")).actionGet();
    }

    @Before
    public void before() throws Exception {
        node = startNode();
        parseService = node.injector().getInstance(SQLParseService.class);
        informationSchemaService = node.injector().getInstance(InformationSchemaService.class);
    }

    @After
    public void tearDown() throws Exception {
        node.stop();
        closeNode("node1");
        super.tearDown();
    }

    @Test
    public void testSearchInformationSchemaTablesRefresh() throws Exception {
        serviceSetup();

        exec("select * from information_schema.tables");
        assertEquals(3L, response.rowCount());

        node.client().execute(SQLAction.INSTANCE,
            new SQLRequest("create table t4 (col1 integer, col2 string)")).actionGet();

        // create table causes a cluster event that will then cause to rebuild the information schema
        // wait until it's rebuild
        Thread.sleep(10);

        exec("select * from information_schema.tables");
        assertEquals(4L, response.rowCount());
    }

    private void exec(String statement) throws Exception {
        exec(statement, new Object[0]);
    }


    /**
     * execUsingClient the statement using the informationSchemaService directly
     * @param statement
     * @param args
     * @throws Exception
     */
    private void exec(String statement, Object[] args) throws Exception {
        ParsedStatement stmt = parseService.parse(statement, args);
        response = informationSchemaService.execute(stmt).actionGet();
    }

    /**
     * execUsingClient the statement using the transportClient
     * @param statement
     * @param args
     * @throws Exception
     */
    private void execUsingClient(String statement, Object[] args) throws Exception {
        response = node.client().execute(SQLAction.INSTANCE, new SQLRequest(statement, args)).actionGet();
    }

    private void execUsingClient(String statement) throws Exception {
        execUsingClient(statement, new Object[0]);
    }



    @Test
    public void testExecuteThreadSafety() throws Exception {
        serviceSetup();
        final ParsedStatement stmt = parseService.parse("select * from information_schema.tables");

        int numThreads = 30;
        final CountDownLatch countDownLatch = new CountDownLatch(numThreads);
        ThreadPool pool = new ThreadPool();
        for (int i = 0; i < numThreads; i++) {

            if (i > 4 && i % 3 == 0) {
                node.client().execute(SQLAction.INSTANCE,
                    new SQLRequest("create table t" + i + " (col1 integer, col2 string) replicas 8")).actionGet();
            }

            pool.executor(ThreadPool.Names.GENERIC).execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        SQLResponse response = informationSchemaService.execute(stmt).actionGet();
                        assertTrue(response.rowCount() >= 3L);
                        countDownLatch.countDown();
                    } catch (IOException e) {
                        assertTrue(false); // fail test
                    }
                }
            });
        }

        countDownLatch.await(10, TimeUnit.SECONDS);
    }


    @Test
    public void testSelectStarFromInformationSchemaTableWithOrderBy() throws Exception {
        execUsingClient("create table test (col1 integer primary key, col2 string)");
        execUsingClient("create table foo (col1 integer primary key, col2 string) clustered into 3 shards");

        execUsingClient("select * from INFORMATION_SCHEMA.Tables order by table_name asc");
        assertEquals(2L, response.rowCount());
        assertEquals("foo", response.rows()[0][0]);
        assertEquals(3, response.rows()[0][1]);
        assertEquals(1, response.rows()[0][2]);

        assertEquals("test", response.rows()[1][0]);
        assertEquals(5, response.rows()[1][1]);
        assertEquals(1, response.rows()[1][2]);
    }

    @Test
    public void testSelectStarFromInformationSchemaTableWithOrderByAndLimit() throws Exception {
        execUsingClient("create table test (col1 integer primary key, col2 string)");
        execUsingClient("create table foo (col1 integer primary key, col2 string) clustered into 3 shards");

        execUsingClient("select * from INFORMATION_SCHEMA.Tables order by table_name asc limit 1");
        assertEquals(1L, response.rowCount());
        assertEquals("foo", response.rows()[0][0]);
        assertEquals(3, response.rows()[0][1]);
        assertEquals(1, response.rows()[0][2]);
    }

    @Test
    public void testUpdateInformationSchema() throws Exception {
        expectedException.expect(SQLParseException.class);
        expectedException.expectMessage(
                "INFORMATION_SCHEMA tables are virtual and read-only. Only SELECT statements are supported");
        execUsingClient("update INFORMATION_SCHEMA.Tables set table_name = 'x'");
    }

    @Test
    public void testDeleteInformationSchema() throws Exception {
        expectedException.expect(SQLParseException.class);
        expectedException.expectMessage(
                "INFORMATION_SCHEMA tables are virtual and read-only. Only SELECT statements are supported");
        execUsingClient("delete from INFORMATION_SCHEMA.Tables");
    }

    @Test
    public void testSelectStarFromInformationSchemaTableWithOrderByTwoColumnsAndLimit() throws Exception {
        execUsingClient("create table test (col1 integer primary key, col2 string) clustered into 1 shards");
        execUsingClient("create table foo (col1 integer primary key, col2 string) clustered into 3 shards");
        execUsingClient("create table bar (col1 integer primary key, col2 string) clustered into 3 shards");

        execUsingClient("select table_name, number_of_shards from INFORMATION_SCHEMA.Tables order by number_of_shards desc, table_name asc limit 2");
        assertEquals(2L, response.rowCount());

        assertEquals("bar", response.rows()[0][0]);
        assertEquals(3, response.rows()[0][1]);
        assertEquals("foo", response.rows()[1][0]);
        assertEquals(3, response.rows()[1][1]);
    }

    @Test
    public void testSelectStarFromInformationSchemaTableWithOrderByAndLimitOffset() throws Exception {
        execUsingClient("create table test (col1 integer primary key, col2 string)");
        execUsingClient("create table foo (col1 integer primary key, col2 string) clustered into 3 shards");

        execUsingClient("select * from INFORMATION_SCHEMA.Tables order by table_name asc limit 1 offset 1");
        assertEquals(1L, response.rowCount());
        assertEquals("test", response.rows()[0][0]);
        assertEquals(5, response.rows()[0][1]);
        assertEquals(1, response.rows()[0][2]);
    }

    @Test
    public void testSelectFromInformationSchemaTable() throws Exception {
        execUsingClient("select TABLE_NAME from INFORMATION_SCHEMA.Tables");
        assertEquals(0L, response.rowCount());

        execUsingClient("create table test (col1 integer primary key, col2 string)");
        Thread.sleep(10); // wait for clusterStateChanged event and index update

        execUsingClient("select table_name, number_of_shards, number_of_replicas from INFORMATION_SCHEMA.Tables");
        assertEquals(1L, response.rowCount());
        assertEquals("test", response.rows()[0][0]);
        assertEquals(5, response.rows()[0][1]);
        assertEquals(1, response.rows()[0][2]);
    }

    @Test
    public void testSelectStarFromInformationSchemaTable() throws Exception {
        execUsingClient("create table test (col1 integer primary key, col2 string)");
        execUsingClient("select * from INFORMATION_SCHEMA.Tables");
        assertEquals(1L, response.rowCount());
        assertEquals("test", response.rows()[0][0]);
        assertEquals(5, response.rows()[0][1]);
        assertEquals(1, response.rows()[0][2]);
    }

    @Test
    public void testSelectFromTableConstraints() throws Exception {

        execUsingClient("select * from INFORMATION_SCHEMA.table_constraints");
        assertEquals(0L, response.rowCount());
        assertThat(response.cols(), arrayContaining("table_name", "constraint_name",
                "constraint_type"));

        execUsingClient("create table test (col1 integer primary key, col2 string)");
        execUsingClient("select constraint_type, constraint_name, " +
                "table_name from information_schema.table_constraints");
        assertEquals(1L, response.rowCount());
        assertEquals(response.rows()[0][0], "PRIMARY_KEY");
        assertEquals(response.rows()[0][1], "col1");
        assertEquals(response.rows()[0][2], "test");
    }

    @Test
    public void testRefreshTableConstraints() throws Exception {
        execUsingClient("create table test (col1 integer primary key, col2 string)");
        execUsingClient("select table_name, constraint_name from INFORMATION_SCHEMA" +
                ".table_constraints");
        assertEquals(1L, response.rowCount());
        assertEquals(response.rows()[0][0], "test");
        assertEquals(response.rows()[0][1], "col1");

        execUsingClient("create table test2 (col1a string primary key, col2a timestamp)");
        execUsingClient("select * from INFORMATION_SCHEMA.table_constraints");

        assertEquals(2L, response.rowCount());
        assertEquals(response.rows()[1][0], "test2");
        assertEquals(response.rows()[1][1], "col1a");
    }

}
