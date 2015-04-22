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
import com.google.common.base.Joiner;
import io.crate.action.sql.SQLResponse;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.util.Random;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

@AxisRange(min = 0)
@BenchmarkHistoryChart(filePrefix="benchmark-in-numeric-history", labelWith = LabelType.CUSTOM_KEY)
@BenchmarkMethodChart(filePrefix = "benchmark-in-numeric")
public class InNumericBenchmark extends BenchmarkBase {

    public static final int BENCHMARK_ROUNDS = 100;
    public static final int NUMBER_OF_DOCUMENTS = 100_000;
    public static final String INDEX_NAME = "bench_in_numeric";

    static {
        ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(true);
    }

    static AtomicInteger VALUE = new AtomicInteger(0);
    static Vector<Integer> VALUES = new Vector<>();

    @Rule
    public TestRule benchmarkRun = RuleChain.outerRule(new BenchmarkRule()).around(super.ruleChain);


    @Override
    protected void createTable() {
        execute("create table " + INDEX_NAME +
                " (int_val int, long_val long) " +
                "clustered into 4 shards with (number_of_replicas=0)");
        client().admin().cluster().prepareHealth(INDEX_NAME).setWaitForGreenStatus().execute().actionGet();
    }

    @Override
    protected String tableName() {
        return INDEX_NAME;
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

        int value = VALUE.getAndIncrement();
        VALUES.add(value);

        return XContentFactory.jsonBuilder()
                .startObject()
                .field("int_val", value)
                .field("long_val", random.nextLong())
                .endObject()
                .bytes().toBytes();
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = 1)
    @Test
    public void testSelectIn500() throws Exception {
        String statement = "select * from " + INDEX_NAME + " where int_val in (" +
                Joiner.on(",").join(VALUES.subList(0, 500)) + ")";
        SQLResponse response = execute(statement);
        assertThat(response.rowCount(), is(500L));
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = 1)
    @Test
    public void testSelectIn2K() throws Exception {
        String statement = "select * from " + INDEX_NAME + " where int_val in (" +
                Joiner.on(",").join(VALUES.subList(0, 2000)) + ")";
        SQLResponse response = execute(statement);
        assertThat(response.rowCount(), is(2000L));
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = 1)
    @Test
    public void testSelectIn8K() throws Exception {
        String statement = "select * from " + INDEX_NAME + " where int_val in (" +
                Joiner.on(",").join(VALUES.subList(0, 8000)) + ")";
        SQLResponse response = execute(statement);
        assertThat(response.rowCount(), is(8000L));
    }
}
