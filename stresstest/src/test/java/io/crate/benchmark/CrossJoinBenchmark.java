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
import com.carrotsearch.junitbenchmarks.annotation.AxisRange;
import com.carrotsearch.junitbenchmarks.annotation.BenchmarkHistoryChart;
import com.carrotsearch.junitbenchmarks.annotation.BenchmarkMethodChart;
import com.carrotsearch.junitbenchmarks.annotation.LabelType;
import org.apache.commons.lang3.RandomStringUtils;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.junit.AfterClass;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@AxisRange(min = 0)
@BenchmarkHistoryChart(filePrefix="benchmark-cross-joins-history", labelWith = LabelType.CUSTOM_KEY)
@BenchmarkMethodChart(filePrefix = "benchmark-cross-joins")
public class CrossJoinBenchmark extends BenchmarkBase {

    public static final int BENCHMARK_ROUNDS = 3;

    public static final String ARTICLE_INSERT_SQL_STMT = "INSERT INTO articles (id, name, price) Values (?, ?, ?)";
    public static final String COLORS_INSERT_SQL_STMT = "INSERT INTO colors (id, name, coolness) Values (?, ?, ?)";

    public static final String QAF_SQL_STMT = "select articles.name as article, colors.name as color from articles, colors order by article, color limit 20000";
    public static final String QTF_WITH_OFFSET_SQL_STMT = "select * from articles CROSS JOIN colors limit 1 offset 20000";

    public static final int ARTICLE_SIZE = 100000;
    public static final int COLORS_SIZE = 100000;
    public static final int SMALL_SIZE = 50000;

    @Override
    protected void createTable() {
        execute("create table articles (" +
                "    id integer primary key," +
                "    name string," +
                "    price float" +
                ") clustered into 2 shards with (number_of_replicas=0, refresh_interval=0)", new Object[0]);
        execute("create table colors (" +
                "    id integer primary key," +
                "    name string, " +
                "    coolness float" +
                ") with (refresh_interval=0)", new Object[0]);
        execute("create table small (" +
                "    info object as (size integer)" +
                ") with (refresh_interval=0)", new Object[0]);
    }

    @AfterClass
    public static void afterClass() {
        CLUSTER.client().admin().indices().prepareDelete("articles").execute().actionGet();
        CLUSTER.client().admin().indices().prepareDelete("colors").execute().actionGet();
        CLUSTER.client().admin().indices().prepareDelete("small").execute().actionGet();
    }

    @Override
    public boolean indexExists() {
        return client().admin().indices().exists(new IndicesExistsRequest("articles", "colors", "small")).actionGet().isExists();
    }

    @Override
    public boolean generateData() {
        return true;
    }

    @Override
    protected void doGenerateData() throws Exception {
        createSampleData(ARTICLE_INSERT_SQL_STMT, ARTICLE_SIZE);
        createSampleData(COLORS_INSERT_SQL_STMT, COLORS_SIZE);
        createSampleDataSmall(SMALL_SIZE);
        refresh();
    }

    private void createSampleData(String stmt, int rows) {
        Object[][] bulkArgs = new Object[rows][];
        for (int i = 0; i < rows; i++) {
            Object[] object = getRandomObject(rows);
            bulkArgs[i]  = object;
        }
        execute(stmt, bulkArgs);
        refresh();
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
        execute("INSERT INTO small (info) values (?)", bulkArgs);
        refresh();
    }


    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = 1)
    @Test
    public void testQTF() {
        execute("select * from articles, colors", new Object[0]);
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = 1)
    @Test
    public void testQTFWithOffset() {
        execute(QTF_WITH_OFFSET_SQL_STMT);
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = 1)
    @Test
    public void testQTFWithOffset5Concurrent() throws Exception {
        executeConcurrently(5,
                QTF_WITH_OFFSET_SQL_STMT,
                2, TimeUnit.MINUTES
        );
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = 1)
    @Test
    public void testQTFWithOffset10Concurrent() throws Exception {
        executeConcurrently(10,
                QTF_WITH_OFFSET_SQL_STMT,
                2, TimeUnit.MINUTES
        );
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = 1)
    @Test
    public void testQAF() {
        execute(QAF_SQL_STMT);
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = 1)
    @Test
    public void testQAF5Concurrent() throws Exception {
        executeConcurrently(5,
                QAF_SQL_STMT,
                2, TimeUnit.MINUTES
        );
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = 1)
    @Test
    public void testQAF10Concurrent() throws Exception {
        executeConcurrently(10,
                QAF_SQL_STMT,
                2, TimeUnit.MINUTES
        );
    }

    private void executeConcurrently(int numConcurrent, final String stmt, int timeout, TimeUnit timeoutUnit) throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(numConcurrent);
        List<Callable<Object>> tasks = Collections.nCopies(numConcurrent, Executors.callable(new Runnable() {
            @Override
            public void run() {
                execute(stmt);
            }
        }));
        executor.invokeAll(tasks);
        executor.shutdown();
        executor.awaitTermination(timeout, timeoutUnit);
        executor.shutdownNow();
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = 1)
    @Test
    public void testQAF3Tables() {
        execute("select articles.name as article, colors.name as color, small.info['size'] as size " +
                "from articles, colors, small order by article, color limit 20000");
    }
}
