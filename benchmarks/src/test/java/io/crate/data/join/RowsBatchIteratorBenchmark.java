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

package io.crate.data.join;

import io.crate.breaker.RamAccounting;
import io.crate.breaker.RowAccounting;
import io.crate.breaker.RowAccountingWithEstimators;
import io.crate.data.BatchIterator;
import io.crate.data.BatchIterators;
import io.crate.data.CloseAssertingBatchIterator;
import io.crate.data.InMemoryBatchIterator;
import io.crate.data.Input;
import io.crate.data.Row;
import io.crate.data.Row1;
import io.crate.data.RowN;
import io.crate.data.SkippingBatchIterator;
import io.crate.execution.engine.collect.InputCollectExpression;
import io.crate.execution.engine.join.HashInnerJoinBatchIterator;
import io.crate.execution.engine.window.WindowFunction;
import io.crate.execution.engine.window.WindowFunctionBatchIterator;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.Functions;
import io.crate.module.EnterpriseFunctionsModule;
import io.crate.testing.RowGenerator;
import io.crate.types.DataTypes;
import io.crate.window.NthValueFunctions;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

import static io.crate.data.SentinelRow.SENTINEL;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
public class RowsBatchIteratorBenchmark {

    private final RowAccountingWithEstimators rowAccounting = new RowAccountingWithEstimators(
        Collections.singleton(DataTypes.INTEGER), RamAccounting.NO_ACCOUNTING);

    // use materialize to not have shared row instances
    // this is done in the startup, otherwise the allocation costs will make up the majority of the benchmark.
    private List<Row> rows = StreamSupport.stream(RowGenerator.range(0, 10_000_000).spliterator(), false)
        .map(Row::materialize)
        .map(RowN::new)
        .collect(Collectors.toList());

    // used with  RowsBatchIterator without any state/error handling to establish a performance baseline.
    private final List<Row1> oneThousandRows = IntStream.range(0, 1000).mapToObj(Row1::new).collect(Collectors.toList());
    private final List<Row1> tenThousandRows = IntStream.range(0, 10000).mapToObj(Row1::new).collect(Collectors.toList());

    private WindowFunction lastValueIntFunction;

    @Setup
    public void setup() {
        Functions functions = new ModulesBuilder().add(new EnterpriseFunctionsModule())
            .createInjector().getInstance(Functions.class);
        lastValueIntFunction = (WindowFunction) functions.getQualified(
            new FunctionIdent(NthValueFunctions.LAST_VALUE_NAME, Collections.singletonList(DataTypes.INTEGER)));
    }

    @Benchmark
    public void measureConsumeBatchIterator(Blackhole blackhole) {
        BatchIterator<Row> it = new InMemoryBatchIterator<>(rows, SENTINEL, false);
        while (it.moveNext()) {
            blackhole.consume(it.currentElement().get(0));
        }
    }

    @Benchmark
    public void measureConsumeCloseAssertingIterator(Blackhole blackhole) {
        BatchIterator<Row> it = new InMemoryBatchIterator<>(rows, SENTINEL, false);
        BatchIterator<Row> itCloseAsserting = new CloseAssertingBatchIterator<>(it);
        while (itCloseAsserting.moveNext()) {
            blackhole.consume(itCloseAsserting.currentElement().get(0));
        }
    }

    @Benchmark
    public void measureConsumeSkippingBatchIterator(Blackhole blackhole) {
        BatchIterator<Row> it = new InMemoryBatchIterator<>(rows, SENTINEL, false);
        BatchIterator<Row> skippingIt = new SkippingBatchIterator<>(it, 100);
        while (skippingIt.moveNext()) {
            blackhole.consume(skippingIt.currentElement().get(0));
        }
    }

    @Benchmark
    public void measureConsumeNestedLoopJoin(Blackhole blackhole) {
        BatchIterator<Row> crossJoin = JoinBatchIterators.crossJoinNL(
            InMemoryBatchIterator.of(oneThousandRows, SENTINEL, true),
            InMemoryBatchIterator.of(tenThousandRows, SENTINEL, true),
            new CombinedRow(1, 1)
        );
        while (crossJoin.moveNext()) {
            blackhole.consume(crossJoin.currentElement().get(0));
        }
    }

    @Benchmark
    public void measureConsumeBlockNestedLoopJoin(Blackhole blackhole) {
        BatchIterator<Row> crossJoin = JoinBatchIterators.crossJoinBlockNL(
            InMemoryBatchIterator.of(oneThousandRows, SENTINEL, true),
            InMemoryBatchIterator.of(tenThousandRows, SENTINEL, true),
            new CombinedRow(1, 1),
            () -> 1000,
            new NoRowAccounting()
        );
        while (crossJoin.moveNext()) {
            blackhole.consume(crossJoin.currentElement().get(0));
        }
    }

    @Benchmark
    public void measureConsumeNestedLoopLeftJoin(Blackhole blackhole) {
        BatchIterator<Row> leftJoin = JoinBatchIterators.leftJoin(
            InMemoryBatchIterator.of(oneThousandRows, SENTINEL, true),
            InMemoryBatchIterator.of(tenThousandRows, SENTINEL, true),
            new CombinedRow(1, 1),
            row -> Objects.equals(row.get(0), row.get(1))
        );
        while (leftJoin.moveNext()) {
            blackhole.consume(leftJoin.currentElement().get(0));
        }
    }

    @Benchmark
    public void measureConsumeHashInnerJoin(Blackhole blackhole) {
        BatchIterator<Row> leftJoin = new HashInnerJoinBatchIterator(
            InMemoryBatchIterator.of(oneThousandRows, SENTINEL, true),
            InMemoryBatchIterator.of(tenThousandRows, SENTINEL, true),
            rowAccounting,
            new CombinedRow(1, 1),
            row -> Objects.equals(row.get(0), row.get(1)),
            row -> Objects.hash(row.get(0)),
            row -> Objects.hash(row.get(0)),
            () -> 1000
        );
        while (leftJoin.moveNext()) {
            blackhole.consume(leftJoin.currentElement().get(0));
        }
    }

    @Benchmark
    public void measureConsumeHashInnerJoinWithHashCollisions(Blackhole blackhole) {
        BatchIterator<Row> leftJoin = new HashInnerJoinBatchIterator(
            InMemoryBatchIterator.of(oneThousandRows, SENTINEL, true),
            InMemoryBatchIterator.of(tenThousandRows, SENTINEL, true),
            rowAccounting,
            new CombinedRow(1, 1),
            row -> Objects.equals(row.get(0), row.get(1)),
            row -> {
                // For the 0-499 records no collisions
                // For the 500-1000 records produce chains of length 5 but don't interfere with the 0-499
                Integer value = (Integer) row.get(0);
                return value < 500 ? value : (value % 100) + 500;
            },
            row -> (Integer) row.get(0) % 500,
            () -> 1000
        );
        while (leftJoin.moveNext()) {
            blackhole.consume(leftJoin.currentElement().get(0));
        }
    }

    @Benchmark
    public void measureConsumeWindowBatchIterator(Blackhole blackhole) throws Exception{
        InputCollectExpression input = new InputCollectExpression(0);
        BatchIterator<Row> batchIterator = WindowFunctionBatchIterator.of(
            new InMemoryBatchIterator<>(rows, SENTINEL, false),
            new NoRowAccounting(),
            (partitionStart, partitionEnd, currentIndex, sortedRows) -> 0,
            (partitionStart, partitionEnd, currentIndex, sortedRows) -> currentIndex,
            (arg1, arg2) -> 0,
            (arg1, arg2) -> 0,
            1,
            () -> 1,
            Runnable::run,
            List.of(lastValueIntFunction),
            List.of(),
            new Input[]{input}
        );
        BatchIterators.collect(batchIterator, Collectors.summingInt(x -> { blackhole.consume(x); return 1; })).get();
    }

    private static class NoRowAccounting implements RowAccounting<Row> {
        @Override
        public void accountForAndMaybeBreak(Row row) {
        }

        @Override
        public void release() {

        }
    }
}
