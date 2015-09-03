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

import com.carrotsearch.randomizedtesting.RandomizedTest;
import com.carrotsearch.randomizedtesting.annotations.Listeners;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import io.crate.action.sql.*;
import io.crate.test.integration.ClassLifecycleIntegrationTest;
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
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.junit.listeners.LoggingListener;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;


@Ignore
@ThreadLeakScope(value = ThreadLeakScope.Scope.NONE)
@Listeners({LoggingListener.class})
public abstract class BenchmarkBase extends RandomizedTest {

    private static final long SEED = System.nanoTime();
    protected static String NODE1;
    protected static String NODE2;
    protected static InternalTestCluster CLUSTER;

    public static final String INDEX_NAME = "countries";
    public static final String DATA = "/setup/data/bench.json";

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
        return sqlExecutor.client().execute(SQLBulkAction.INSTANCE, new SQLBulkRequest(stmt, bulkArgs)).actionGet();
    }

    @BeforeClass
    public synchronized static void beforeClass() throws IOException {
        if (CLUSTER == null) {
            CLUSTER = new InternalTestCluster(
                    SEED,
                    0,
                    0,
                    InternalTestCluster.clusterName("shared", Integer.toString(AbstractRandomizedTest.CHILD_JVM_ID), SEED),
                    ClassLifecycleIntegrationTest.SETTINGS_SOURCE,
                    0,
                    false,
                    AbstractRandomizedTest.CHILD_JVM_ID,
                    "shared"
            );
        }
        CLUSTER.beforeTest(getRandom(), 0.0d);
    }

    @Before
    public void setUp() throws Exception {
        if (NODE1 == null) {
            NODE1 = CLUSTER.startNode(getNodeSettings());
        }
        if (NODE2 == null) {
            NODE2 = CLUSTER.startNode(getNodeSettings());
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
            CLUSTER.client().admin().indices().prepareDelete("_all").execute().actionGet();
        } catch (IndexMissingException e) {
            // fine
        }
        CLUSTER.afterTest();
        CLUSTER.close();
        CLUSTER = null;
        NODE1 = null;
        NODE2 = null;
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
        getClient(true).admin().indices().prepareFlush(tableName).execute().actionGet();
        getClient(false).admin().indices().prepareFlush(tableName).execute().actionGet();
        refresh(client());
        logger.info("{} documents generated.", numberOfDocuments);
    }

    protected byte[] generateRowSource() throws IOException {
        return new byte[0];
    }

    protected boolean generateNewRowForEveryDocument() {
        return false;
    }

    protected Client client() {
        return CLUSTER.client();
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

    public Settings getNodeSettings() {
        ImmutableSettings.Builder builder = ImmutableSettings.builder()
                .put("plugin.types", "io.crate.plugin.CrateCorePlugin")
                .put("transport.tcp.port", randomAvailablePort())
                .put("index.store.type", "memory");
        return builder.build();
    }

    public Client getClient(boolean firstNode) {
        return firstNode ? CLUSTER.client(NODE1) : CLUSTER.client(NODE2);
    }

    public void loadBulk(boolean queryPlannerEnabled) throws Exception {
        String path = dataPath();
        byte[] bulkPayload = PathAccessor.bytesFromPath(path, this.getClass());
        BulkResponse bulk = getClient(queryPlannerEnabled).prepareBulk().add(bulkPayload, 0, bulkPayload.length, null, null).execute().actionGet();
        for (BulkItemResponse item : bulk.getItems()) {
            assert !item.isFailed() : String.format("unable to index data %s", item);
        }
    }
}
