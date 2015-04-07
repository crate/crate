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

import com.carrotsearch.junitbenchmarks.BenchmarkOptions;
import com.carrotsearch.junitbenchmarks.BenchmarkRule;
import com.carrotsearch.junitbenchmarks.annotation.AxisRange;
import com.carrotsearch.junitbenchmarks.annotation.BenchmarkHistoryChart;
import com.carrotsearch.junitbenchmarks.annotation.BenchmarkMethodChart;
import com.carrotsearch.junitbenchmarks.annotation.LabelType;
import io.crate.action.sql.SQLAction;
import io.crate.action.sql.SQLRequest;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.util.Random;

@AxisRange(min = 0)
@BenchmarkHistoryChart(filePrefix="benchmark-groupby-history", labelWith = LabelType.CUSTOM_KEY)
@BenchmarkMethodChart(filePrefix = "benchmark-groupby")
public class GroupByBenchmark extends BenchmarkBase {

    public final static ESLogger logger = Loggers.getLogger(GroupByBenchmark.class);
    public static final int NUMBER_OF_DOCUMENTS = 500_000; // was 1_000_000
    public static final int BENCHMARK_ROUNDS = 1000;

    public static SQLRequest maxRequest = new SQLRequest(String.format("select max(\"areaInSqKm\") from %s group by continent", INDEX_NAME));
    public static SQLRequest minRequest = new SQLRequest(String.format("select min(\"areaInSqKm\") from %s group by continent", INDEX_NAME));
    public static SQLRequest avgRequest = new SQLRequest(String.format("select avg(\"population\") from %s group by continent", INDEX_NAME));
    public static SQLRequest countStarRequest = new SQLRequest(String.format("select count(*) from %s group by continent", INDEX_NAME));
    public static SQLRequest countColumnRequest = new SQLRequest(String.format("select count(\"countryName\") from %s group by continent", INDEX_NAME));
    public static SQLRequest countDistinctRequest = new SQLRequest(String.format("select count(distinct \"countryName\") from %s group by continent", INDEX_NAME));
    public static SQLRequest arbitraryRequest = new SQLRequest(String.format("select arbitrary(\"countryName\") from %s group by continent", INDEX_NAME));
    public static SQLRequest groupByRoutingColumnRequest = new SQLRequest(String.format("select sum(population), type from %s group by type order by 1", INDEX_NAME));
    public static SQLRequest groupByRoutingColumnRequestHighLimit = new SQLRequest(String.format("select sum(population), type from %s group by type order by 1 limit 100000", INDEX_NAME));

    static {
        ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(true);
    }

    @Rule
    public TestRule benchmarkRun = RuleChain.outerRule(new BenchmarkRule()).around(super.ruleChain);

    @Override
    public boolean importData() {
        return false;
    }

    @Override
    public boolean generateData() {
        return true;
    }

    @Override
    protected int numberOfDocuments() {
        return NUMBER_OF_DOCUMENTS;
    }

    @Override
    protected byte[] generateRowSource() throws IOException {
        Random random = getRandom();
        byte[] buffer = new byte[32];
        random.nextBytes(buffer);
        return XContentFactory.jsonBuilder()
                .startObject()
                .field("areaInSqKm", random.nextFloat())
                .field("continent", new BytesArray(buffer, 0, 4).toUtf8())
                .field("countryCode", new BytesArray(buffer, 4, 8).toUtf8())
                .field("countryName", new BytesArray(buffer, 8, 24).toUtf8())
                .field("type", getRandom().nextInt(100_000))
                .field("population", random.nextInt(Integer.MAX_VALUE))
                .endObject()
                .bytes().toBytes();
    }


    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = 10)
    @Test
    public void testGroupByMaxPerformance() {
        getClient(false).execute(SQLAction.INSTANCE, maxRequest).actionGet();
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = 10)
    @Test
    public void testGroupByMinPerformance() {
        getClient(false).execute(SQLAction.INSTANCE, minRequest).actionGet();
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = 10)
    @Test
    public void testGroupByAvgPerformance() {
        getClient(false).execute(SQLAction.INSTANCE, avgRequest).actionGet();
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = 10)
    @Test
    public void testGroupByCountStarPerformance() {
        getClient(false).execute(SQLAction.INSTANCE, countStarRequest).actionGet();
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = 10)
    @Test
    public void testGroupByCountColumnPerformance() {
        getClient(false).execute(SQLAction.INSTANCE, countColumnRequest).actionGet();
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = 10)
    @Test
    public void testGroupByCountDistinctPerformance() {
        getClient(false).execute(SQLAction.INSTANCE, countDistinctRequest).actionGet();
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = 10)
    @Test
    public void testGroupByAnyPerformance() {
        getClient(false).execute(SQLAction.INSTANCE, arbitraryRequest).actionGet();
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = 10)
    @Test
    public void testGroupByRoutingColumn() throws Exception {
        getClient(false).execute(SQLAction.INSTANCE, groupByRoutingColumnRequest).actionGet();
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = 10)
    @Test
    public void testGroupByRoutingColumnLimit100000() throws Exception {
        getClient(false).execute(SQLAction.INSTANCE, groupByRoutingColumnRequestHighLimit).actionGet();
    }
}
