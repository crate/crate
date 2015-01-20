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
                    ") clustered into 2 shards with (number_of_replicas=0)", new Object[0], false);
            execute("create table colors (" +
                    "    id integer primary key," +
                    "    name string, " +
                    "    coolness float" +
                    ")", new Object[0], false);
            execute("create table small (" +
                    "    info object as (size integer)" +
                    ")", new Object[0], false);
            createSampleData(ARTICLE_INSERT_SQL_STMT, 10000);
            createSampleData(COLORS_INSERT_SQL_STMT, 10000);
            createSampleDataSmall(5000);
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
            Object[] object = getRandomObject();
            bulkArgs[i]  = object;
        }
        SQLBulkRequest request = new SQLBulkRequest(stmt, bulkArgs);
        client().execute(SQLBulkAction.INSTANCE, request).actionGet();
        refresh(client());
    }

    private Object[] getRandomObject() {
        return new Object[]{
                (int)(Math.random() * 10000),  // id
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
    public void testNoLimitThreeTables() {
        SQLResponse res = execute("select articles.name, colors.name, info['size'] from small, articles, colors", new Object[0], true);
        res.rows();
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

}
