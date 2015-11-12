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

package io.crate.benchmark;


import com.carrotsearch.junitbenchmarks.BenchmarkOptions;
import com.carrotsearch.junitbenchmarks.BenchmarkRule;
import com.carrotsearch.junitbenchmarks.annotation.AxisRange;
import com.carrotsearch.junitbenchmarks.annotation.BenchmarkHistoryChart;
import com.carrotsearch.junitbenchmarks.annotation.BenchmarkMethodChart;
import com.carrotsearch.junitbenchmarks.annotation.LabelType;
import io.crate.action.sql.*;
import org.apache.commons.lang3.RandomStringUtils;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.util.HashMap;

@AxisRange(min = 0)
@BenchmarkHistoryChart(filePrefix="benchmark-bulk-delete-history", labelWith = LabelType.CUSTOM_KEY)
@BenchmarkMethodChart(filePrefix = "benchmark-bulk-delete")
public class BulkDeleteBenchmark extends BenchmarkBase{

    @Rule
    public TestRule benchmarkRun = RuleChain.outerRule(new BenchmarkRule()).around(super.ruleChain);

    public static final String INDEX_NAME = "users";
    public static final int BENCHMARK_ROUNDS = 3;
    public static final int ROWS = 5000;

    public static final String SINGLE_INSERT_SQL_STMT = "INSERT INTO users (id, name, age) Values (?, ?, ?)";
    public static final String SELECT_ALL_IDS_STMT = "SELECT id, _id FROM users";
    public static final String DELETE_SQL_STMT = "DELETE FROM users where id = ?";

    @Override
    protected String tableName() {
        return INDEX_NAME;
    }

    @Override
    public Settings getNodeSettings() {
        Settings settings = super.getNodeSettings();
        settings = ImmutableSettings.builder().put(settings).put("threadpool.index.queue_size", ROWS).build();
        return settings;

    }

    @Override
    protected void createTable() {
        execute("create table users (" +
                "    id string primary key," +
                "    name string," +
                "    age integer" +
                ") clustered into 2 shards with (number_of_replicas=0)", new Object[0]);
        client().admin().cluster().prepareHealth(INDEX_NAME).setWaitForGreenStatus().execute().actionGet();
    }


    private HashMap<String, String> createSampleData() {
        Object[][] bulkArgs = new Object[ROWS][];
        HashMap<String, String> ids = new HashMap<>();

        for (int i = 0; i < ROWS; i++) {
            Object[] object = getRandomObject();
            bulkArgs[i]  = object;
        }
        execute(SINGLE_INSERT_SQL_STMT, bulkArgs);
        refresh();

        SQLResponse response = execute(SELECT_ALL_IDS_STMT);
        for (Object[] row : response.rows()) {
            ids.put((String) row[0], (String) row[1]);
        }

        return ids;
    }

    private Object[] getRandomObject() {
        return new Object[]{
                RandomStringUtils.randomAlphabetic(40),  // id
                RandomStringUtils.randomAlphabetic(10),  // name
                (int)(Math.random() * 100),                // age
        };
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = 1)
    @Test
    public void testESBulkDelete() {
        HashMap<String, String> ids = createSampleData();
        Client client = client();
        BulkRequestBuilder request = new BulkRequestBuilder(client);

        for(String id: ids.values()){
            DeleteRequest deleteRequest = new DeleteRequest("users", "default", id);
            request.add(deleteRequest);
        }
        request.execute().actionGet();
        refresh();
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
        execute(DELETE_SQL_STMT, bulkArgs);
        refresh();
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = 1)
    @Test
    public void testSQLSingleDelete() {
        HashMap<String, String> ids = createSampleData();
        for(String id: ids.keySet()){
            execute(DELETE_SQL_STMT, new Object[]{id});
        }
    }
}
