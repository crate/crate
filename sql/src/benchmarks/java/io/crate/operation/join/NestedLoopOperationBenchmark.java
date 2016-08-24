/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.operation.join;

import com.carrotsearch.junitbenchmarks.BenchmarkOptions;
import com.carrotsearch.junitbenchmarks.BenchmarkRule;
import com.carrotsearch.junitbenchmarks.annotation.AxisRange;
import com.carrotsearch.junitbenchmarks.annotation.BenchmarkHistoryChart;
import com.carrotsearch.junitbenchmarks.annotation.BenchmarkMethodChart;
import com.carrotsearch.junitbenchmarks.annotation.LabelType;
import com.google.common.base.Predicates;
import io.crate.core.collections.Bucket;
import io.crate.core.collections.Row;
import io.crate.operation.projectors.ListenableRowReceiver;
import io.crate.planner.node.dql.join.JoinType;
import io.crate.testing.RowCountRowReceiver;
import io.crate.testing.RowSender;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.concurrent.ThreadPoolExecutor;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@AxisRange(min = 0)
@BenchmarkHistoryChart(filePrefix="benchmark-nl-history", labelWith = LabelType.CUSTOM_KEY)
@BenchmarkMethodChart(filePrefix = "benchmark-nl")
public class NestedLoopOperationBenchmark {

    private ThreadPoolExecutor executor;

    static final int BENCHMARK_ROUNDS = 10;

    @Rule
    public BenchmarkRule benchmarkRule = new BenchmarkRule();

    @Before
    public void prepare() {
        executor = EsExecutors.newFixed("nl-benchmark", 5, 10, EsExecutors.daemonThreadFactory(getClass().getSimpleName()));
    }

    @After
    public void cleanup() {
        if (executor != null) {
            executor.shutdownNow();
        }
    }

    @Test
    @BenchmarkOptions(warmupRounds = 1, benchmarkRounds = BENCHMARK_ROUNDS)
    public void testPerfEqual10000() throws Exception {
        executeNestedLoop(10_000, 10_000);
    }

    @Test
    @BenchmarkOptions(warmupRounds = 1, benchmarkRounds = BENCHMARK_ROUNDS)
    public void testPerfEqual1000() throws Exception {
        executeNestedLoop(1_000, 1_000);
    }

    @Test
    @BenchmarkOptions(warmupRounds = 1, benchmarkRounds = BENCHMARK_ROUNDS)
    public void testPerfLeft100Right1000() throws Exception {
        executeNestedLoop(100, 1000);
    }

    @Test
    @BenchmarkOptions(warmupRounds = 1, benchmarkRounds = BENCHMARK_ROUNDS)
    public void testPerfLeft100Right10000() throws Exception {
        executeNestedLoop(100, 10_000);
    }

    @Test
    @BenchmarkOptions(warmupRounds = 1, benchmarkRounds = BENCHMARK_ROUNDS)
    public void testPerfLeft100Right100000() throws Exception {
        executeNestedLoop(100, 100_000);
    }

    @Test
    @BenchmarkOptions(warmupRounds = 1, benchmarkRounds = BENCHMARK_ROUNDS)
    public void testPerfLeft1000Right100() throws Exception {
        executeNestedLoop(1000, 100);
    }

    @Test
    @BenchmarkOptions(warmupRounds = 1, benchmarkRounds = BENCHMARK_ROUNDS)
    public void testPerfLeft10000Right100() throws Exception {
        executeNestedLoop(10_000, 100);
    }

    @Test
    @BenchmarkOptions(warmupRounds = 1, benchmarkRounds = BENCHMARK_ROUNDS)
    public void testPerfLeft100000Right100() throws Exception {
        executeNestedLoop(100_000, 100);
    }

    private Bucket executeNestedLoop(int leftSize, int rightSize) throws Exception {
        Iterable<Row> left = RowSender.rowRange(0, leftSize);
        Iterable<Row> right = RowSender.rowRange(0, rightSize);

        RowCountRowReceiver receiver = new RowCountRowReceiver();
        NestedLoopOperation operation = new NestedLoopOperation(
            0, receiver, Predicates.<Row>alwaysTrue(), Predicates.<Row>alwaysTrue(), JoinType.CROSS, 0, 0);
        ListenableRowReceiver leftSide = operation.leftRowReceiver();
        ListenableRowReceiver rightSide = operation.rightRowReceiver();

        RowSender leftRowSender = new RowSender(left, leftSide, executor);
        RowSender rightRowSender = new RowSender(right, rightSide, executor);

        executor.execute(leftRowSender);
        executor.execute(rightRowSender);
        Bucket result = receiver.result(TimeValue.timeValueMinutes(10));
        assertThat((Integer)result.iterator().next().get(0), is(leftSize * rightSize));
        return result;
    }

}
