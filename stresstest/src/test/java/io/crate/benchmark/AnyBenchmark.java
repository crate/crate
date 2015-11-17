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
import com.google.common.base.Joiner;
import io.crate.action.sql.SQLResponse;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.junit.Test;

import java.io.IOException;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.is;

@AxisRange(min = 0)
@BenchmarkHistoryChart(filePrefix="benchmark-any-history", labelWith = LabelType.CUSTOM_KEY)
@BenchmarkMethodChart(filePrefix = "benchmark-any")
public class AnyBenchmark extends BenchmarkBase  {

    static final int BENCHMARK_ROUNDS = 200;
    static final int NUMBER_OF_DOCUMENTS = 100_000;
    static AtomicInteger VALUE = new AtomicInteger(0);
    static Vector<String> VALUES = new Vector<>();

    static final String SELECT_ANY_PARAM = "select * from "+ INDEX_NAME + " where value = ANY (?)";

    static final String SELECT_NOT_ANY_PARAM = "select * from "+ INDEX_NAME + " where value != ANY (?)";

    @Override
    public boolean generateData() {
        return true;
    }

    @Override
    protected int numberOfDocuments() {
        return NUMBER_OF_DOCUMENTS;
    }

    @Override
    protected void createTable() {
        execute("create table " + INDEX_NAME + " (" +
                "  id int primary key, " +
                "  value string" +
                ") with (number_of_replicas=0)");
        client().admin().cluster().prepareHealth(INDEX_NAME).setWaitForGreenStatus().execute().actionGet();
    }

    @Override
    protected byte[] generateRowSource() throws IOException {
        int value = VALUE.getAndIncrement();
        String strValue = String.valueOf(value);
        VALUES.add(strValue);

        return XContentFactory.jsonBuilder()
                .startObject()
                .field("id", value)
                .field("value", strValue)
                .endObject()
                .bytes().toBytes();
    }

    protected boolean generateNewRowForEveryDocument() {
        return true;
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = 1)
    @Test
    public void testSelectAny5() throws Exception {
        String statement = "select * from "+ INDEX_NAME + " where value = ANY (['" +
                Joiner.on("','").join(VALUES.subList(0, 5)) + "'])";
        SQLResponse response = execute(statement);
        assertThat(response.rowCount(), is(5L));
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = 1)
    @Test
    public void testSelectAny500() throws Exception {
        String statement = "select * from "+ INDEX_NAME + " where value = ANY (['" +
                Joiner.on("','").join(VALUES.subList(0, 500)) + "'])";
        SQLResponse response = execute(statement);
        assertThat(response.rowCount(), is(500L));
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = 1)
    @Test
    public void testSelectAny2K() throws Exception {
        String statement = "select * from "+ INDEX_NAME + " where value = ANY (['" +
                Joiner.on("','").join(VALUES.subList(0, 2000)) + "'])";
        SQLResponse response = execute(statement);
        assertThat(response.rowCount(), is(2000L));
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = 1)
    @Test
    public void testSelectAny5Param() throws Exception {
        SQLResponse response = execute(SELECT_ANY_PARAM, new Object[]{ VALUES.subList(0, 5).toArray() });
        assertThat(response.rowCount(), is(5L));
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = 1)
    @Test
    public void testSelectAny500Param() throws Exception {
        SQLResponse response = execute(SELECT_ANY_PARAM, new Object[]{ VALUES.subList(0, 500).toArray() });
        assertThat(response.rowCount(), is(500L));
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = 1)
    @Test
    public void testSelectAny2KParam() throws Exception {
        SQLResponse response = execute(SELECT_ANY_PARAM, new Object[]{ VALUES.subList(0, 2000).toArray() });
        assertThat(response.rowCount(), is(2000L));
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = 1)
    @Test
    public void testSelectNotAny5Param() throws Exception {
        SQLResponse response = execute(SELECT_NOT_ANY_PARAM, new Object[]{ VALUES.subList(0, 5).toArray() });
        assertThat(response.rowCount(), is(10000L));
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = 1)
    @Test
    public void testSelectNotAny500Param() throws Exception {
        SQLResponse response =  execute(SELECT_NOT_ANY_PARAM, new Object[]{ VALUES.subList(0, 500).toArray() });
        assertThat(response.rowCount(), is(10000L));
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = 1)
    @Test
    public void testSelectNotAny2KParam() throws Exception {
        SQLResponse response = execute(SELECT_NOT_ANY_PARAM, new Object[]{ VALUES.subList(0, 2000).toArray() });
        assertThat(response.rowCount(), is(10000L));
    }
}
