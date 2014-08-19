/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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


import com.carrotsearch.junitbenchmarks.BenchmarkOptions;
import com.carrotsearch.junitbenchmarks.BenchmarkRule;
import com.carrotsearch.junitbenchmarks.annotation.AxisRange;
import com.carrotsearch.junitbenchmarks.annotation.BenchmarkMethodChart;
import com.google.common.collect.ImmutableList;
import io.crate.action.sql.SQLAction;
import io.crate.action.sql.SQLRequest;
import io.crate.analyze.Id;
import io.crate.metadata.ColumnIdent;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.client.Client;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.util.HashMap;

@AxisRange(min = 0)
@BenchmarkMethodChart(filePrefix = "benchmark-bulk-delete")
public class BulkDeleteBenchmark extends BenchmarkBase{

    static {
        ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(true);
    }

    @Rule
    public TestRule benchmarkRun = RuleChain.outerRule(new BenchmarkRule()).around(super.ruleChain);

    public static final String INDEX_NAME = "users";
    public static final int BENCHMARK_ROUNDS = 5;
    public static final int ROWS = 5000;

    public static final String SINGLE_INSERT_SQL_STMT = "INSERT INTO users (id, name, age) Values (?, ?, ?)";
    public static final String DELETE_SQL_STMT = "DELETE FROM users where id = ?";


    @Before
    public void setUp() throws Exception {
        if (NODE1 == null) {
            NODE1 = cluster.startNode(getNodeSettings(1));
        }
        if (NODE2 == null) {
            NODE2 = cluster.startNode(getNodeSettings(2));
        }
        if(!indexExists()){
            execute("create table users (" +
                    "    id string primary key," +
                    "    name string," +
                    "    age integer" +
                    ") clustered into 2 shards with (number_of_replicas=0)", new Object[0], false);
            refresh(client());
        }

    }

    @Override
    public boolean indexExists() {
        return getClient(false).admin().indices().exists(new IndicesExistsRequest(INDEX_NAME)).actionGet().isExists();
    }

    private HashMap<String, String> createSampleData() {
        Object[][] bulkArgs = new Object[ROWS][];
        HashMap<String, String> ids = new HashMap<>();
        for (int i = 0; i < ROWS; i++) {
            Object[] object = getRandomObject();
            bulkArgs[i]  = object;

            String id = (String)object[0];
            Id esId = new Id(ImmutableList.of(new ColumnIdent("id")), ImmutableList.of(new BytesRef(id)), new ColumnIdent("id"), true);
            ids.put(id, esId.stringValue());
        }
        SQLRequest request = new SQLRequest(SINGLE_INSERT_SQL_STMT, bulkArgs);
        client().execute(SQLAction.INSTANCE, request).actionGet();
        refresh(client());
        return ids;
    }

    private Object[] getRandomObject() {
        return new Object[]{
                RandomStringUtils.randomAlphabetic(40),  // id
                RandomStringUtils.randomAlphabetic(10),  // name
                (int)Math.random() * 100,                // age
        };
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = 1)
    @Test
    public void testESBulkDelete() {
        HashMap<String, String> ids = createSampleData();
        Client client = getClient(false);
        BulkRequestBuilder request = new BulkRequestBuilder(client);

        for(String id: ids.values()){
            DeleteRequest deleteRequest = new DeleteRequest("users", "default", id);
            request.add(deleteRequest);
        }
        request.execute().actionGet();
        refresh(client());
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = 1)
    @Test
    public void testSQLBulkDelete() {
        Object[][] bulkArgs = new Object[ROWS][];
        HashMap<String, String> ids = createSampleData();

        int i = 0;
        for(String id: ids.keySet()){
            bulkArgs[i] = new Object[]{id};
            i++;
        }
        getClient(false).execute(SQLAction.INSTANCE, new SQLRequest(DELETE_SQL_STMT, bulkArgs)).actionGet();
        refresh(client());
    }
}
