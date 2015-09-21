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
import io.crate.core.collections.Row;
import io.crate.operation.projectors.*;
import io.crate.testing.CollectingRowReceiver;
import io.crate.testing.RandomRows;
import io.crate.testing.RowSender;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeoutException;

@BenchmarkHistoryChart(filePrefix="benchmark-sortingrowdownstream-history", labelWith = LabelType.CUSTOM_KEY)
@BenchmarkMethodChart(filePrefix = "benchmark-sortingrowdownstream")
public class SortingRowDownstreamBenchmark extends BenchmarkBase {

    public static final int NUMBER_OF_DOCUMENTS = 10_000_000;
    public static final int BENCHMARK_ROUNDS = 10;
    public static final int WARMUP_ROUNDS = 10;
    public static final int NUM_UPSTREAMS = 5;

    public static final int SAME_VALUES = 5; // 30 = break even to use queue

    public static final int OFFSET = 100_000;

    private RowSender[] upstreams;
    private ThreadPoolExecutor executor;

    private class NonMaterializingCollectingRowReceiver extends CollectingRowReceiver {

        @Override
        public boolean setNextRow(Row row) {
            return true;
        }
    }

    @Rule
    public TestRule benchmarkRun = RuleChain.outerRule(new BenchmarkRule()).around(super.ruleChain);


    @Override
    public boolean generateData() {
        return false;
    }

    @Override
    public void setUp() throws Exception {
        executor = EsExecutors.newFixed(NUM_UPSTREAMS, 10, EsExecutors.daemonThreadFactory(getClass().getSimpleName()));
    }

    @After
    public void tearDown() throws Exception {
        executor.shutdownNow();
    }

    @AfterClass
    public static void tearDownClass() throws IOException {
    }


    @Override
    public boolean indexExists() {
        return true; // prevent index creation
    }

    private void runPerformanceTest(RowMerger toTest) throws InterruptedException, ExecutionException, TimeoutException {

        upstreams = new RowSender[NUM_UPSTREAMS];

        for (int i = 0; i < NUM_UPSTREAMS; i++) {
            int offset = i % 2 == 0 ? OFFSET : 0;
            int numRows = NUMBER_OF_DOCUMENTS / ( NUM_UPSTREAMS * SAME_VALUES);
            upstreams[i] = new RowSender(new RandomRows(numRows, SAME_VALUES, offset), toTest.newRowReceiver(), executor);
        }
        for (int i = 0; i < NUM_UPSTREAMS; i++) {
            executor.execute(upstreams[i]);
        }
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = WARMUP_ROUNDS)
    @Test
    public void testBlockingSortingQueuedRowDownstreamBenchmark() throws Exception {
        CollectingRowReceiver downstream = new NonMaterializingCollectingRowReceiver();
        BlockingSortingQueuedRowDownstream rowMerger = new BlockingSortingQueuedRowDownstream(
                downstream,
                1,
                new int[]{0},
                new boolean[]{false},
                new Boolean[]{null}
        );
        runPerformanceTest(rowMerger);
        downstream.result();
    }

}
