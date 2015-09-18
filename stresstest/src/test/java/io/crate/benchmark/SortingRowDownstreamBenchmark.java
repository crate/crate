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
import io.crate.core.collections.RowN;
import io.crate.jobs.ExecutionState;
import io.crate.operation.RowDownstream;
import io.crate.operation.RowUpstream;
import io.crate.operation.projectors.*;
import io.crate.testing.CollectingRowReceiver;
import org.apache.commons.lang3.RandomUtils;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.junit.AfterClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import static org.mockito.Mockito.mock;

@BenchmarkHistoryChart(filePrefix="benchmark-sortingrowdownstream-history", labelWith = LabelType.CUSTOM_KEY)
@BenchmarkMethodChart(filePrefix = "benchmark-sortingrowdownstream")
public class SortingRowDownstreamBenchmark extends BenchmarkBase {

    public static final int NUMBER_OF_DOCUMENTS = 10_000_000;
    public static final int BENCHMARK_ROUNDS = 10;
    public static final int WARMUP_ROUNDS = 10;
    public static final int NUM_UPSTREAMS = 5;

    public static final int SAME_VALUES = 5; // 30 = break even to use queue

    public static final int OFFSET = 100_000;

    private PausableUpstream[] upstreams;

    private static class PausableUpstream implements Runnable, RowUpstream {

        private final RowReceiver downstreamHandle;
        private final AtomicBoolean paused;

        private int pauseCount = 0;

        private final int offset;
        private final Object[] cells = new Object[1];
        private final Row row = new RowN(cells);

        private ReentrantLock runLock = new ReentrantLock();

        public PausableUpstream(RowDownstream rowDownstream, int offset) {
            this.offset = offset;
            downstreamHandle = rowDownstream.newRowReceiver();
            downstreamHandle.setUpstream(this);
            paused = new AtomicBoolean(false);
        }

        public void run() {
            runLock.lock();
            try {
                downstreamHandle.prepare(mock(ExecutionState.class));
                int limit = offset + NUMBER_OF_DOCUMENTS / ( NUM_UPSTREAMS * SAME_VALUES);
                for (int i = offset; i < limit; i++) {
                    cells[0] = i;
                    for ( int j = 0; j < SAME_VALUES; j++) {
                        downstreamHandle.setNextRow(row);
                        if (paused.get()) {
                            return; // don't call finish
                        }
                    }
                }
                downstreamHandle.finish();
            } finally {
                runLock.unlock();
            }

        }

        public void start() {
            new Thread(this).start();
        }

        @Override
        public void pause() {
            paused.set(true);
            pauseCount++;
        }

        @Override
        public void resume(boolean async) {
            paused.set(false);
            new Thread(this).start();
        }
    }

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
    }


    @AfterClass
    public static void tearDownClass() throws IOException {
    }


    @Override
    public boolean indexExists() {
        return true; // prevent index creation
    }

    private void runPerformanceTest(RowMerger toTest) throws InterruptedException, ExecutionException, TimeoutException {

        upstreams = new PausableUpstream[NUM_UPSTREAMS];

        for (int i = 0; i < NUM_UPSTREAMS; i++) {
            upstreams[i] = new PausableUpstream(toTest, i % 2 == 0 ? OFFSET : 0);
        }
        for (int i = 0; i < NUM_UPSTREAMS; i++) {
            upstreams[i].start();
        }
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = WARMUP_ROUNDS)
    @Test
    public void testMergeProjectorPerformance() throws Exception {
        CollectingRowReceiver downstream = new NonMaterializingCollectingRowReceiver();
        SortingRowMerger rowMerger = new SortingRowMerger(
                downstream,
                new int[]{0},
                new boolean[]{false},
                new Boolean[]{null}
        );
        runPerformanceTest(rowMerger);
        downstream.result();
    }

    @TestLogging("io.crate.operation.projectors.BlockingSortingQueuedRowDownstream:TRACE")
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
