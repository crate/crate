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

package io.crate.module.sql.benchmark;

import io.crate.action.sql.SQLAction;
import io.crate.action.sql.SQLRequest;
import io.crate.action.sql.SQLResponse;
import io.crate.test.integration.CrateTestCluster;
import io.crate.test.integration.NodeSettingsSource;
import org.apache.lucene.util.AbstractRandomizedTest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.IndexMissingException;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.Random;

import static io.crate.test.integration.PathAccessor.bytesFromPath;


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
    public static final String DATA = "/essetup/data/bench.json";

    private Random random = new Random(System.nanoTime());

    @Rule
    public TestRule ruleChain = RuleChain.emptyRuleChain();

    public SQLResponse execute(String stmt, Object[] args, boolean queryPlannerEnabled) {
        return getClient(queryPlannerEnabled).execute(SQLAction.INSTANCE, new SQLRequest(stmt, args)).actionGet();
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
            refresh(client());
            if (loadData()) {
                doLoadData();
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
        return getClient(false).admin().indices().exists(new IndicesExistsRequest(INDEX_NAME)).actionGet().isExists();
    }

    public boolean loadData() {
        return false;
    }

    public void doLoadData() throws Exception {
        loadBulk(DATA, false);
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

    public void loadBulk(String path, boolean queryPlannerEnabled) throws Exception {
        byte[] bulkPayload = bytesFromPath(path, this.getClass());
        BulkResponse bulk = getClient(queryPlannerEnabled).prepareBulk().add(bulkPayload, 0, bulkPayload.length, false, null, null).execute().actionGet();
        for (BulkItemResponse item : bulk.getItems()) {
            assert !item.isFailed() : String.format("unable to index data %s", item);
        }
    }
}
