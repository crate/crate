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
import io.crate.action.sql.SQLBulkAction;
import io.crate.action.sql.SQLBulkRequest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

@AxisRange(min = 0)
@BenchmarkHistoryChart(filePrefix="benchmark-upsert-history", labelWith = LabelType.CUSTOM_KEY)
@BenchmarkMethodChart(filePrefix = "benchmark-upsert")
public class UpsertBenchmark extends BenchmarkBase {

    public static final String INDEX_NAME = "traffic_logs";
    public static final int BENCHMARK_ROUNDS = 120;
    public static final int BENCHMARK_ROUNDS_MANY_VALUES = 3;
    private static final String SIMPLE_UPSERT = "insert into traffic_logs (id, url, count) VALUES (?, ?, ?)" +
                                                " ON DUPLICATE KEY UPDATE " +
                                                "   count = 5";
    private static final String UPSERT_WITH_REF = "insert into traffic_logs (id, url, count) VALUES (?, ?, ?)" +
                                                  " ON DUPLICATE KEY UPDATE " +
                                                  "   count = VALUES(count) + count";
    private static Object[][] BULK_ARGS_MANY;

    @Rule
    public TestRule benchmarkRun = RuleChain.outerRule(new BenchmarkRule()).around(super.ruleChain);

    @Override
    protected String tableName() {
        return INDEX_NAME;
    }

    @Override
    protected void createTable() {
        execute("create table traffic_logs (" +
                "    id integer primary key," +
                "    url string," +
                "    count integer" +
                ") clustered into 2 shards with (number_of_replicas=0)", new Object[0], false);
        client().admin().cluster().prepareHealth(INDEX_NAME).setWaitForGreenStatus().execute().actionGet();


        BULK_ARGS_MANY = new Object[1000][];
        for (int i = 0; i < 1000; i++) {
            BULK_ARGS_MANY[i] = new Object[]{(i/100), "http://crate.io", i};
        }
    }

    @Override
    public boolean generateData() {
        return true;
    }

    @Override
    protected void doGenerateData() throws Exception {
        for(int i = 0; i < 500; i++) {
            execute("insert into traffic_logs (id, url, count) VALUES (?, ?, ?)",
                    new Object[]{i+1, "http://crate.io", 1}, true);
        }
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = 1)
    @Test
    public void testUpsertExistingDocument() {
        execute(SIMPLE_UPSERT, new Object[]{0, "http://crate.io", 1}, true);
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = 1)
    @Test
    public void testUpsertNewDocument() {
        execute(SIMPLE_UPSERT,
                new Object[]{(int)(Math.random() * 1000000) + 500, "http://crate.io", 1}, true);
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = 1)
    @Test
    public void testUpsertWithReference() {
        execute(UPSERT_WITH_REF, new Object[]{100, "http://crate.io", 1}, true);
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS_MANY_VALUES, warmupRounds = 1)
    @Test
    public void testBulkUpsert() {
        Object[][] bulkArgs = new Object[][]{
            new Object[]{0, "http://crate.io", 1},
            new Object[]{0, "http://crate.io", 1},
            new Object[]{0, "http://crate.io", 1}
        };
        SQLBulkRequest request = new SQLBulkRequest(UPSERT_WITH_REF, bulkArgs);
        getClient(false).execute(SQLBulkAction.INSTANCE, request).actionGet();
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS_MANY_VALUES, warmupRounds = 1)
    @Test
    public void testBulkUpsertManyValues() {
        SQLBulkRequest request = new SQLBulkRequest(UPSERT_WITH_REF, BULK_ARGS_MANY);
        getClient(false).execute(SQLBulkAction.INSTANCE, request).actionGet();
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS_MANY_VALUES, warmupRounds = 1)
    @Test
    public void testUpsertManyValues() {
        for (Object[] value : BULK_ARGS_MANY) {
            execute(UPSERT_WITH_REF, value, true);
        }
    }
}
