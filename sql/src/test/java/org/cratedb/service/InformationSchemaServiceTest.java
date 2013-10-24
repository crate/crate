package org.cratedb.service;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.cratedb.action.sql.ParsedStatement;
import org.cratedb.action.sql.SQLAction;
import org.cratedb.action.sql.SQLRequest;
import org.cratedb.action.sql.SQLResponse;
import org.elasticsearch.cluster.AbstractZenNodesTests;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.node.internal.InternalNode;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.is;

public class InformationSchemaServiceTest extends AbstractZenNodesTests {

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
        node = startNode();
        node.client().execute(SQLAction.INSTANCE,
            new SQLRequest("create table t1 (col1 integer, col2 string) clustered into 7 shards")).actionGet();
        node.client().execute(SQLAction.INSTANCE,
            new SQLRequest("create table t2 (col1 integer, col2 string) clustered into 10 shards")).actionGet();
        node.client().execute(SQLAction.INSTANCE,
            new SQLRequest("create table t3 (col1 integer, col2 string) replicas 8")).actionGet();

        parseService = node.injector().getInstance(SQLParseService.class);
        informationSchemaService = node.injector().getInstance(InformationSchemaService.class);
    }

    @After
    public void tearDown() throws Exception {
        closeAllNodes();
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
        Thread.sleep(200);

        exec("select * from information_schema.tables");
        assertEquals(4L, response.rowCount());
    }

    private void exec(String statement) throws Exception {
        exec(statement, new Object[0]);
    }
    private void exec(String statement, Object[] args) throws Exception {
        ParsedStatement stmt = parseService.parse(statement, args);
        response = informationSchemaService.execute(stmt).actionGet();
    }

    @Test
    public void testQueryParserService() throws Exception {
        serviceSetup();
        exec("select * from information_schema.tables");
        assertEquals(3L, response.rowCount());
    }

    @Test
    public void testQueryParserServiceWithWhere() throws Exception {
        serviceSetup();
        exec("select table_name from information_schema.tables where table_name = 't1'");

        assertEquals(1L, response.rowCount());
        assertEquals("t1", response.rows()[0][0]);
    }

    @Test
    public void testQueryParserServiceLimit() throws Exception {
        serviceSetup();
        exec("select * from information_schema.tables limit 1");

        assertEquals(1L, response.rowCount());
    }

    @Test
    public void testQueryParserServiceLimitAndOrderBy() throws Exception {
        serviceSetup();
        exec("select table_name, number_of_shards, number_of_replicas from information_schema.tables " +
             " order by number_of_shards desc limit 2");

        assertEquals(2L, response.rowCount());
        assertEquals(10, response.rows()[0][1]);
        assertEquals("t2", response.rows()[0][0]);
        assertEquals(7, response.rows()[1][1]);
        assertEquals("t1", response.rows()[1][0]);
    }

    @Test
    public void testQueryParserServiceLimitAndOrderByStringColumn() throws Exception {
        serviceSetup();
        exec("select table_name, number_of_shards, number_of_replicas from information_schema.tables " +
            " order by table_name desc limit 2");

        assertEquals(2L, response.rowCount());
        assertEquals("t3", response.rows()[0][0]);
        assertEquals("t2", response.rows()[1][0]);
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
}
