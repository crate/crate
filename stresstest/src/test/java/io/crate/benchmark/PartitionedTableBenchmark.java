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
import com.carrotsearch.junitbenchmarks.annotation.BenchmarkHistoryChart;
import com.carrotsearch.junitbenchmarks.annotation.BenchmarkMethodChart;
import com.carrotsearch.junitbenchmarks.annotation.LabelType;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

@BenchmarkHistoryChart(filePrefix="benchmark-partitionedtablebenchmark-history", labelWith = LabelType.CUSTOM_KEY)
@BenchmarkMethodChart(filePrefix = "benchmark-partitionedtablebenchmark")
public class PartitionedTableBenchmark extends BenchmarkBase {

    static {
        ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(true);
    }

    private static boolean dataGenerated = false;

    private static final Integer NUMBER_OF_PARTITIONS = 500;
    private static final Integer NUMBER_OF_DOCS_PER_PARTITION = 1;

    public static final int BENCHMARK_ROUNDS = 100;
    public static final int WARMUP_ROUNDS = 10;

    @Rule
    public TestRule benchmarkRun = RuleChain.outerRule(new BenchmarkRule()).around(super.ruleChain);

    @Override
    public boolean generateData() {
        return !dataGenerated;
    }

    @Override
    protected void createTable() {
        execute("create table \"" + INDEX_NAME + "\" (" +
                " name string," +
                " p string) partitioned by (p) with (number_of_replicas=0, refresh_interval = 0)", new Object[0], true);
        client().admin().cluster().prepareHealth(INDEX_NAME).setWaitForGreenStatus().execute().actionGet();
    }

    @Override
    protected void doGenerateData() throws Exception {
        Object[][] bulkArgs = new Object[NUMBER_OF_PARTITIONS * NUMBER_OF_DOCS_PER_PARTITION][];
        for (int i = 0; i < NUMBER_OF_PARTITIONS * NUMBER_OF_DOCS_PER_PARTITION; i+= NUMBER_OF_DOCS_PER_PARTITION) {
            for (int j = 0; j < NUMBER_OF_DOCS_PER_PARTITION; j++) {
                bulkArgs[i+j] = new Object[]{"Marvin", i};
            }
        }
        execute("insert into " + INDEX_NAME + " (name, p) values (?, ?)", bulkArgs);
        execute("refresh table " + INDEX_NAME);
        dataGenerated = true;
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = WARMUP_ROUNDS)
    @Test
    public void testFetchAll() throws Exception {
        execute("select * from " + INDEX_NAME + " limit " + NUMBER_OF_PARTITIONS * NUMBER_OF_DOCS_PER_PARTITION);
    }
}
