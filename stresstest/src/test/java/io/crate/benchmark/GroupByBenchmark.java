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
import com.carrotsearch.junitbenchmarks.annotation.AxisRange;
import com.carrotsearch.junitbenchmarks.annotation.BenchmarkHistoryChart;
import com.carrotsearch.junitbenchmarks.annotation.BenchmarkMethodChart;
import com.carrotsearch.junitbenchmarks.annotation.LabelType;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.junit.Test;

import java.io.IOException;
import java.util.Random;

@AxisRange(min = 0)
@BenchmarkHistoryChart(filePrefix="benchmark-groupby-history", labelWith = LabelType.CUSTOM_KEY)
@BenchmarkMethodChart(filePrefix = "benchmark-groupby")
public class GroupByBenchmark extends BenchmarkBase {

    public static final int NUMBER_OF_DOCUMENTS = 500_000; // was 1_000_000
    public static final int BENCHMARK_ROUNDS = 1000;

    public static final String MAX_STMT = String.format("select max(\"areaInSqKm\") from %s group by continent", INDEX_NAME);
    public static final String MIN_STMT = String.format("select min(\"areaInSqKm\") from %s group by continent", INDEX_NAME);
    public static final String AVG_STMT = String.format("select avg(\"population\") from %s group by continent", INDEX_NAME);
    public static final String COUNT_STAR_STMT = String.format("select count(*) from %s group by continent", INDEX_NAME);
    public static final String COUNT_COLUMN_STMT = String.format("select count(\"countryName\") from %s group by continent", INDEX_NAME);
    public static final String COUNT_DISTINCT_STMT = String.format("select count(distinct \"countryName\") from %s group by continent", INDEX_NAME);
    public static final String ARBITRARY_STMT = String.format("select arbitrary(\"countryName\") from %s group by continent", INDEX_NAME);
    public static final String GROUP_BY_ROUTING_STMT = String.format("select sum(population), type from %s group by type order by 1", INDEX_NAME);
    public static final String GROUP_BY_ROUTING_HIGH_LIMIT_STMT = String.format("select sum(population), type from %s group by type order by 1 limit 100000", INDEX_NAME);


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
        execute(MAX_STMT);
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = 10)
    @Test
    public void testGroupByMinPerformance() {
        execute(MIN_STMT);
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = 10)
    @Test
    public void testGroupByAvgPerformance() {
        execute(AVG_STMT);
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = 10)
    @Test
    public void testGroupByCountStarPerformance() {
        execute(COUNT_STAR_STMT);
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = 10)
    @Test
    public void testGroupByCountColumnPerformance() {
        execute(COUNT_COLUMN_STMT);
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = 10)
    @Test
    public void testGroupByCountDistinctPerformance() {
        execute(COUNT_DISTINCT_STMT);
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = 10)
    @Test
    public void testGroupByAnyPerformance() {
        execute(ARBITRARY_STMT);
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = 10)
    @Test
    public void testGroupByRoutingColumn() throws Exception {
        execute(GROUP_BY_ROUTING_STMT);
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = 10)
    @Test
    public void testGroupByRoutingColumnLimit100000() throws Exception {
        execute(GROUP_BY_ROUTING_HIGH_LIMIT_STMT);
    }
}
