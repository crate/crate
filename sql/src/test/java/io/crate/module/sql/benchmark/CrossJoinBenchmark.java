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
import com.carrotsearch.junitbenchmarks.annotation.BenchmarkHistoryChart;
import com.carrotsearch.junitbenchmarks.annotation.BenchmarkMethodChart;
import com.carrotsearch.junitbenchmarks.annotation.LabelType;
import io.crate.action.sql.*;
import org.apache.commons.lang3.RandomStringUtils;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@AxisRange(min = 0)
@BenchmarkHistoryChart(filePrefix="benchmark-cross-joins-history", labelWith = LabelType.CUSTOM_KEY)
@BenchmarkMethodChart(filePrefix = "benchmark-cross-joins")
public class CrossJoinBenchmark extends BenchmarkBase{

    static {
        ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(true);
    }

    @Rule
    public TestRule benchmarkRun = RuleChain.outerRule(new BenchmarkRule()).around(super.ruleChain);

    public static final int BENCHMARK_ROUNDS = 3;

    public static final String ARTICLE_INSERT_SQL_STMT = "INSERT INTO articles (id, name, price) Values (?, ?, ?)";
    public static final String COLORS_INSERT_SQL_STMT = "INSERT INTO colors (id, name, coolness) Values (?, ?, ?)";

    public static final int ARTICLE_SIZE = 100000;
    public static final int COLORS_SIZE = 100000;
    public static final int SMALL_SIZE = 50000;

    @Before
    public void setUp() throws Exception {
        if (NODE1 == null) {
            NODE1 = cluster.startNode(getNodeSettings(1));
        }
        if (NODE2 == null) {
            NODE2 = cluster.startNode(getNodeSettings(2));
        }
        if(!indexExists()){
            execute("create table articles (" +
                    "    id integer primary key," +
                    "    name string," +
                    "    price float" +
                    ") clustered into 2 shards with (number_of_replicas=0, refresh_interval=0)", new Object[0], false);
            execute("create table colors (" +
                    "    id integer primary key," +
                    "    name string, " +
                    "    coolness float" +
                    ") with (refresh_interval=0)", new Object[0], false);
            execute("create table small (" +
                    "    info object as (size integer)" +
                    ") with (refresh_interval=0)", new Object[0], false);
            createSampleData(ARTICLE_INSERT_SQL_STMT, ARTICLE_SIZE);
            createSampleData(COLORS_INSERT_SQL_STMT, COLORS_SIZE);
            createSampleDataSmall(SMALL_SIZE);
            refresh(client());
        }
    }

    @AfterClass
    public static void afterClass() {
        cluster.client().admin().indices().prepareDelete("articles").execute().actionGet();
        cluster.client().admin().indices().prepareDelete("colors").execute().actionGet();
        cluster.client().admin().indices().prepareDelete("small").execute().actionGet();
    }

    @Override
    public boolean indexExists() {
        return getClient(false).admin().indices().exists(new IndicesExistsRequest("articles", "colors", "small")).actionGet().isExists();
    }

    private void createSampleData(String stmt, int rows) {
        Object[][] bulkArgs = new Object[rows][];
        for (int i = 0; i < rows; i++) {
            Object[] object = getRandomObject(rows);
            bulkArgs[i]  = object;
        }
        SQLBulkRequest request = new SQLBulkRequest(stmt, bulkArgs);
        client().execute(SQLBulkAction.INSTANCE, request).actionGet();
        refresh(client());
    }

    private Object[] getRandomObject(int numDifferent) {
        return new Object[]{
                (int)(Math.random() * numDifferent),  // id
                RandomStringUtils.randomAlphabetic(10),  // name
                (float)(Math.random() * 100),            // coolness || price
        };
    }

    private void createSampleDataSmall(int rows) {
        Object[][] bulkArgs = new Object[rows][];
        for (int i = 0; i < rows; i++) {
            bulkArgs[i] = new Object[]{new HashMap<String, Integer>(){{put("size", (int)(Math.random()*1000));}}};
        }
        SQLBulkRequest request = new SQLBulkRequest("INSERT INTO small (info) values (?)", bulkArgs);
        client().execute(SQLBulkAction.INSTANCE, request).actionGet();
        refresh(client());
    }


    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = 1)
    @Test
    public void testNoLimit() {
        execute("select articles.name, colors.name from articles, colors", new Object[0], true);
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = 1)
    @Test
    public void testLimit10() {
        SQLResponse res = execute("select articles.name, colors.name from articles, colors limit 10", new Object[0], true);
        assertThat(res.rowCount(), is(10L));
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = 1)
    @Test
    public void testLimit100() {
        SQLResponse res = execute("select articles.name, colors.name from articles, colors limit 100", new Object[0], true);
        assertThat(res.rowCount(), is(100L));
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = 1)
    @Test
    public void testLimit1000() {
        SQLResponse res = execute("select articles.name, colors.name from articles, colors limit 1000", new Object[0], true);
        assertThat(res.rowCount(), is(1000L));
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = 1)
    @Test
    public void testLimit10000() {
        SQLResponse res = execute("select articles.name, colors.name from articles, colors limit 10000", new Object[0], true);
        assertThat(res.rowCount(), is(10000L));
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = 1)
    @Test
    public void testLimit100000() {
        SQLResponse res = execute("select articles.name, colors.name from articles, colors limit 100000", new Object[0], true);
        assertThat(res.rowCount(), is(100000L));
    }


    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = 1)
    @Test
    public void test3Limit10() {
        SQLResponse res = execute("select articles.name, colors.name, info['size'] from small, articles, colors limit 10", new Object[0], true);
        assertThat(res.rowCount(), is(10L));
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = 1)
    @Test
    public void test3Limit100() {
        SQLResponse res = execute("select articles.name, colors.name, info['size'] from small, articles, colors limit 100", new Object[0], true);
        assertThat(res.rowCount(), is(100L));
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = 1)
    @Test
    public void test3Limit1000() {
        SQLResponse res = execute("select articles.name, colors.name, info['size'] from small, articles, colors limit 1000", new Object[0], true);
        assertThat(res.rowCount(), is(1000L));
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = 1)
    @Test
    public void test3Limit10000() {
        SQLResponse res = execute("select articles.name, colors.name, info['size'] from small, articles, colors limit 10000", new Object[0], true);
        assertThat(res.rowCount(), is(10000L));
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = 1)
    @Test
    public void test3Limit100000() {
        SQLResponse res = execute("select articles.name, colors.name, info['size'] from small, articles, colors limit 100000", new Object[0], true);
        assertThat(res.rowCount(), is(100000L));
    }


    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = 1)
    @Test
    public void testLimit() {
        getClient(false).execute(SQLAction.INSTANCE,
                new SQLRequest("select articles.name, colors.name from articles CROSS JOIN colors limit 1000")).actionGet();
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = 1)
    @Test
    public void testLimitThreeTables() {
        getClient(false).execute(SQLAction.INSTANCE,
                new SQLRequest("select articles.name, colors.name, info['size'] from articles CROSS JOIN colors, small limit 1000")).actionGet();
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = 1)
    @Test
    public void testLimitWithOffset() {
        getClient(false).execute(SQLAction.INSTANCE,
                new SQLRequest("select articles.name, colors.name from articles CROSS JOIN colors limit 1000 offset 10000")).actionGet();
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = 1)
    @Test
    public void testLimitWithOffsetThreeTables() {
        getClient(false).execute(SQLAction.INSTANCE,
                new SQLRequest("select articles.name, colors.name, info['size'] from articles CROSS JOIN colors, small limit 1000 offset 10000")).actionGet();
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = 1)
    @Test
    public void testWhereClause() {
        getClient(false).execute(SQLAction.INSTANCE,
                new SQLRequest("select articles.name, colors.name, articles.price from articles, colors where articles.price > 50")).actionGet();
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = 1)
    @Test
    public void testHighLimitAndOffset() throws Exception {
        getClient(false).execute(SQLAction.INSTANCE,
                new SQLRequest("select articles.name, colors.name from articles CROSS JOIN colors limit 50000 offset 40000")).actionGet();
    }

    private void executeConcurrently(int numConcurrent, final String stmt, int timeout, TimeUnit timeoutUnit) throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(numConcurrent);
        Runnable runnable = new Runnable() {
            @Override
            public void run() {
                getClient(false).execute(SQLAction.INSTANCE,
                        new SQLRequest(stmt)).actionGet();
            }
        };
        while (numConcurrent > 0) {
            executor.execute(runnable);
            numConcurrent--;
        }
        executor.shutdown();
        executor.awaitTermination(timeout, timeoutUnit);
        executor.shutdownNow();
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = 1)
    @Test
    public void test1Concurrent() throws Exception {
        executeConcurrently(1,
                "select articles.name, colors.name, articles.price from articles, colors limit 40000 offset 10000",
                2, TimeUnit.MINUTES
        );
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = 1)
    @Test
    public void test10Concurrent() throws Exception {
        executeConcurrently(10,
                "select articles.name, colors.name, articles.price from articles, colors limit 40000 offset 10000",
                2, TimeUnit.MINUTES
        );
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = 1)
    @Test
    public void test100Concurrent() throws Exception {
        executeConcurrently(100,
                "select articles.name, colors.name, articles.price from articles, colors limit 40000 offset 10000",
                4, TimeUnit.MINUTES
        );
    }
}
