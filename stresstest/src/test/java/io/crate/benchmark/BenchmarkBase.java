/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.benchmark;

import io.crate.action.sql.*;
import io.crate.test.integration.CrateTestCluster;
import io.crate.test.integration.NodeSettingsSource;
import io.crate.test.integration.PathAccessor;
import io.crate.testing.SQLTransportExecutor;
import org.apache.lucene.util.AbstractRandomizedTest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.IndexMissingException;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;


@RunWith(JUnit4.class)
@Ignore
public class BenchmarkBase {

    static {
        ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(true);
    }

    protected static String NODE1;
    protected static String NODE2;
    protected static CrateTestCluster cluster =
        new CrateTestCluster(
            System.nanoTime(),
            0,
            "local",
            CrateTestCluster.clusterName("benchmark",
                    Integer.toString(AbstractRandomizedTest.CHILD_JVM_ID), System.nanoTime()),
            NodeSettingsSource.EMPTY
        );

    public static final String INDEX_NAME = "countries";
    public static final String DATA = "/setup/data/bench.json";

    private Random random = new Random(System.nanoTime());

    public final ESLogger logger = Loggers.getLogger(getClass());

    protected SQLTransportExecutor sqlExecutor = new SQLTransportExecutor(
            new SQLTransportExecutor.ClientProvider() {
                @Override
                public Client client() {
                    return getClient(true);
                }
            }
    );

    @Rule
    public TestRule ruleChain = RuleChain.emptyRuleChain();

    public SQLResponse execute(String stmt, Object[] args, boolean queryPlannerEnabled) {
        return getClient(queryPlannerEnabled).execute(SQLAction.INSTANCE, new SQLRequest(stmt, args)).actionGet();
    }

    public SQLResponse execute(String stmt) {
        return execute(stmt, SQLRequest.EMPTY_ARGS);
    }

    public SQLResponse execute(String stmt, Object[] args) {
        return getClient(true).execute(SQLAction.INSTANCE, new SQLRequest(stmt, args)).actionGet();
    }

    public SQLBulkResponse execute(String stmt, Object[][] bulkArgs) {
        return sqlExecutor.exec(new SQLBulkRequest(stmt, bulkArgs));
    }

    @Before
    public void setUp() throws Exception {
        if (NODE1 == null) {
            NODE1 = cluster.startNode(getNodeSettings(1));
        }
        if (NODE2 == null) {
            NODE2 = cluster.startNode(getNodeSettings(2));
        }

        if (!indexExists()) {
            createTable();
            if (importData()) {
                doImportData();
            } else if (generateData()) {
                doGenerateData();
            }

        }
    }

    @AfterClass
    public static void tearDownClass() throws IOException {
        try {
            cluster.client().admin().indices().prepareDelete("_all").execute().actionGet();
        } catch (IndexMissingException e) {
            // fine
        }
        cluster.afterTest();
    }

    protected void createTable() {
        execute("create table \"" + INDEX_NAME + "\" (" +
                " \"areaInSqKm\" float," +
                " capital string," +
                " continent string," +
                " \"continentName\" string," +
                " \"countryCode\" string," +
                " \"countryName\" string," +
                " north float," +
                " east float," +
                " south float," +
                " west float," +
                " \"fipsCode\" string," +
                " \"currencyCode\" string," +
                " languages string," +
                " \"isoAlpha3\" string," +
                " \"isoNumeric\" string," +
                " population integer" +
                ") clustered into 2 shards with (number_of_replicas=0)", new Object[0], false);
        client().admin().cluster().prepareHealth(INDEX_NAME).setWaitForGreenStatus().execute().actionGet();
    }

    protected void doGenerateData() throws Exception {
        final String tableName = tableName();
        final int numberOfDocuments = numberOfDocuments();
        logger.info("generating {} documents...", numberOfDocuments);
        ExecutorService executor = Executors.newFixedThreadPool(4);
        for (int i=0; i<4; i++) {
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    int numDocsToCreate = numberOfDocuments/4;
                    logger.info("Generating {} Documents in Thread {}", numDocsToCreate, Thread.currentThread().getName());
                    Client client = getClient(false);
                    BulkRequest bulkRequest = new BulkRequest();

                    for (int i=0; i < numDocsToCreate; i+=1000) {
                        bulkRequest.requests().clear();
                        try {
                            byte[] source = null;
                            if (!generateNewRowForEveryDocument()) {
                                source = generateRowSource();
                            }
                            for (int j=0; j<1000;j++) {
                                if (generateNewRowForEveryDocument()) {
                                    source = generateRowSource();
                                }
                                IndexRequest indexRequest = new IndexRequest(tableName, "default", String.valueOf(i+j) + String.valueOf(Thread.currentThread().getId()));
                                indexRequest.source(source);
                                bulkRequest.add(indexRequest);
                            }
                            BulkResponse response = client.bulk(bulkRequest).actionGet();
                            Assert.assertFalse(response.hasFailures());
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }
            });
        }
        executor.shutdown();
        executor.awaitTermination(2L, TimeUnit.MINUTES);
        executor.shutdownNow();
        getClient(true).admin().indices().prepareFlush(tableName).setFull(true).execute().actionGet();
        getClient(false).admin().indices().prepareFlush(tableName).setFull(true).execute().actionGet();
        refresh(client());
        logger.info("{} documents generated.", numberOfDocuments);
    }

    protected byte[] generateRowSource() throws IOException {
        return new byte[0];
    }

    protected boolean generateNewRowForEveryDocument() {
        return false;
    }

    protected Random getRandom() {
        return random;
    }

    protected Client client() {
        return cluster.client();
    }

    protected RefreshResponse refresh(Client client) {
        return client.admin().indices().prepareRefresh().execute().actionGet();
    }

    public boolean nodesStarted() {
        return NODE1 != null && NODE2 != null;
    }

    public boolean indexExists() {
        return getClient(false).admin().indices().exists(new IndicesExistsRequest(tableName())).actionGet().isExists();
    }

    public boolean importData() {
        return false;
    }

    public boolean generateData() {
        return false;
    }

    protected int numberOfDocuments() {
        return 0;
    }

    protected String tableName() {
        return INDEX_NAME;
    }

    protected String dataPath() {
        return DATA;
    }

    public void doImportData() throws Exception {
        loadBulk(false);
        refresh(getClient(true));
        refresh(getClient(false));
    }

    /**
     * @return a random available port for binding
     */
    public int randomAvailablePort()  {
        int port;
        try {
            ServerSocket socket = new ServerSocket(0);
            port = socket.getLocalPort();
            socket.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return port;
    }

    public Settings getNodeSettings(int nodeId) {
        ImmutableSettings.Builder builder = ImmutableSettings.builder()
                .put("http.port", randomAvailablePort())
                .put("transport.tcp.port", randomAvailablePort())
                .put("index.store.type", "memory");
        return builder.build();
    }

    public Client getClient(boolean firstNode) {
        return firstNode ? cluster.client(NODE1) : cluster.client(NODE2);
    }

    public void loadBulk(boolean queryPlannerEnabled) throws Exception {
        String path = dataPath();
        byte[] bulkPayload = PathAccessor.bytesFromPath(path, this.getClass());
        BulkResponse bulk = getClient(queryPlannerEnabled).prepareBulk().add(bulkPayload, 0, bulkPayload.length, false, null, null).execute().actionGet();
        for (BulkItemResponse item : bulk.getItems()) {
            assert !item.isFailed() : String.format("unable to index data %s", item);
        }
    }
}
