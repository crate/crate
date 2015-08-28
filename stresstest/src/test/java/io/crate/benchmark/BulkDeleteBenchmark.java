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
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import io.crate.action.sql.SQLAction;
import io.crate.action.sql.SQLBulkAction;
import io.crate.action.sql.SQLBulkRequest;
import io.crate.action.sql.SQLRequest;
import io.crate.analyze.Id;
import io.crate.metadata.ColumnIdent;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.lucene.util.BytesRef;
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
import java.util.List;

@AxisRange(min = 0)
@BenchmarkHistoryChart(filePrefix="benchmark-bulk-delete-history", labelWith = LabelType.CUSTOM_KEY)
@BenchmarkMethodChart(filePrefix = "benchmark-bulk-delete")
public class BulkDeleteBenchmark extends BenchmarkBase{

    static {
        ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(true);
    }

    @Rule
    public TestRule benchmarkRun = RuleChain.outerRule(new BenchmarkRule()).around(super.ruleChain);

    public static final String INDEX_NAME = "users";
    public static final int BENCHMARK_ROUNDS = 3;
    public static final int ROWS = 5000;

    public static final String SINGLE_INSERT_SQL_STMT = "INSERT INTO users (id, name, age) Values (?, ?, ?)";
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
                ") clustered into 2 shards with (number_of_replicas=0)", new Object[0], false);
        client().admin().cluster().prepareHealth(INDEX_NAME).setWaitForGreenStatus().execute().actionGet();
    }


    private HashMap<String, String> createSampleData() {
        Object[][] bulkArgs = new Object[ROWS][];
        HashMap<String, String> ids = new HashMap<>();

        ColumnIdent idColumn = new ColumnIdent("id");
        Function<List<BytesRef>, String> idFunction = Id.compile(ImmutableList.of(idColumn), new ColumnIdent("id"));
        for (int i = 0; i < ROWS; i++) {
            Object[] object = getRandomObject();
            bulkArgs[i]  = object;

            String id = (String)object[0];
            String esId = idFunction.apply(ImmutableList.of(new BytesRef(id)));
            ids.put(id, esId);
        }
        SQLBulkRequest request = new SQLBulkRequest(SINGLE_INSERT_SQL_STMT, bulkArgs);
        client().execute(SQLBulkAction.INSTANCE, request).actionGet();
        refresh(client());
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
        getClient(false).execute(SQLBulkAction.INSTANCE, new SQLBulkRequest(DELETE_SQL_STMT, bulkArgs)).actionGet();
        refresh(client());
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = 1)
    @Test
    public void testSQLSingleDelete() {
        HashMap<String, String> ids = createSampleData();
        for(String id: ids.keySet()){
            getClient(false).execute(SQLAction.INSTANCE,
                                     new SQLRequest(DELETE_SQL_STMT, new Object[]{id})).actionGet();
        }
    }
}
