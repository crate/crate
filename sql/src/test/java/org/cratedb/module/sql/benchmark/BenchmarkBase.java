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

package org.cratedb.module.sql.benchmark;

import junit.framework.TestCase;
import org.cratedb.action.sql.SQLAction;
import org.cratedb.action.sql.SQLRequest;
import org.cratedb.action.sql.SQLResponse;
import org.cratedb.test.integration.CrateTestCluster;
import org.cratedb.test.integration.NodeSettingsSource;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;
import java.util.Random;

import static org.cratedb.test.integration.PathAccessor.bytesFromPath;


@RunWith(JUnit4.class)
public class BenchmarkBase extends TestCase {

    static {
        ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(true);
    }

    protected static String NODE1;
    protected static String NODE2;
    protected static CrateTestCluster cluster =
        new CrateTestCluster(
            System.nanoTime(),
            0,
            CrateTestCluster.clusterName("benchmark", ElasticsearchTestCase.CHILD_VM_ID, System.nanoTime()),
            NodeSettingsSource.EMPTY
        );

    public static final String INDEX_NAME = "countries";
    public static final String SETTINGS = "/essetup/settings/bench.json";
    public static final String MAPPING = "/essetup/mappings/bench.json";
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
            execute("create table countries (" +
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
                    ") clustered into 2 shards replicas 0", new Object[0], false);
            refresh(client());
            if (loadData()) {
                doLoadData();
            }
        }
    }

    @AfterClass
    public static void tearDownClass() throws IOException {
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

    public Settings getNodeSettings(int nodeId) {
        ImmutableSettings.Builder builder = ImmutableSettings.builder().put("index.store.type", "memory");
        return builder.build();
    }

    public Client getClient(boolean queryPlannerEnabled) {
        return queryPlannerEnabled ? cluster.client(NODE1) : cluster.client(NODE2);
    }

    public void loadBulk(String path, boolean queryPlannerEnabled) throws Exception {
        byte[] bulkPayload = bytesFromPath(path, this.getClass());
        BulkResponse bulk = getClient(queryPlannerEnabled).prepareBulk().add(bulkPayload, 0, bulkPayload.length, false, null, null).execute().actionGet();
        for (BulkItemResponse item : bulk.getItems()) {
            assert !item.isFailed() : String.format("unable to index data {}", item);
        }
    }
}
