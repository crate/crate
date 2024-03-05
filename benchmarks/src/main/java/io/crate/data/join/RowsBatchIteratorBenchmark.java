/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.data.join;

import static io.crate.data.SentinelRow.SENTINEL;
import static io.crate.metadata.functions.TypeVariableConstraint.typeVariable;
import static io.crate.window.NthValueFunctions.LAST_VALUE_NAME;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.elasticsearch.common.settings.Settings;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;

import io.crate.breaker.TypedCellsAccounting;
import io.crate.data.BatchIterator;
import io.crate.data.InMemoryBatchIterator;
import io.crate.data.Input;
import io.crate.data.Row;
import io.crate.data.Row1;
import io.crate.data.RowN;
import io.crate.data.SkippingBatchIterator;
import io.crate.data.breaker.RamAccounting;
import io.crate.data.breaker.RowAccounting;
import io.crate.execution.engine.collect.RowCollectExpression;
import io.crate.execution.engine.join.HashInnerJoinBatchIterator;
import io.crate.execution.engine.window.WindowFunction;
import io.crate.execution.engine.window.WindowFunctionBatchIterator;
import io.crate.metadata.Functions;
import io.crate.metadata.functions.Signature;
import io.crate.metadata.settings.session.SessionSettingRegistry;
import io.crate.types.DataTypes;
import io.crate.types.TypeSignature;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
public class RowsBatchIteratorBenchmark {

    private final TypedCellsAccounting rowAccounting = new TypedCellsAccounting(
        List.of(DataTypes.INTEGER), RamAccounting.NO_ACCOUNTING, 0);

    private List<RowN> rows;

    // used with  RowsBatchIterator without any state/error handling to establish a performance baseline.
    private final List<Row1> oneThousandRows = IntStream.range(0, 1000).mapToObj(Row1::new).toList();
    private final List<Row1> tenThousandRows = IntStream.range(0, 10000).mapToObj(Row1::new).toList();

    private WindowFunction lastValueIntFunction;

    @Setup
    public void setup() {
        rows = IntStream.range(0, 10_000_000)
            .mapToObj(RowN::new)
            .toList();
        Functions functions = Functions.load(Settings.EMPTY, new SessionSettingRegistry(Set.of()));
        lastValueIntFunction = (WindowFunction) functions.getQualified(
            Signature.window(
                LAST_VALUE_NAME,
                TypeSignature.parse("E"),
                TypeSignature.parse("E")
            ).withTypeVariableConstraints(typeVariable("E")),
            List.of(DataTypes.INTEGER),
            DataTypes.INTEGER
        );
    }

    @Benchmark
    public void measureConsumeBatchIterator(Blackhole blackhole) {
        BatchIterator<Row> it = new InMemoryBatchIterator<>(rows, SENTINEL, false);
        while (it.moveNext()) {
            blackhole.consume(it.currentElement().get(0));
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
        BatchIterator<Row> crossJoin = new CrossJoinNLBatchIterator<>(
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
        BatchIterator<Row> crossJoin = new CrossJoinBlockNLBatchIterator(
            InMemoryBatchIterator.of(oneThousandRows, SENTINEL, true),
            InMemoryBatchIterator.of(tenThousandRows, SENTINEL, true),
            new CombinedRow(1, 1),
            ignored -> 1000,
            new NoRowAccounting<>()
        );
        while (crossJoin.moveNext()) {
            blackhole.consume(crossJoin.currentElement().get(0));
        }
    }

    @Benchmark
    public void measureConsumeNestedLoopLeftJoin(Blackhole blackhole) {
        BatchIterator<Row> leftJoin = new LeftJoinNLBatchIterator<>(
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
            ignored -> 1000
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
            ignored -> 1000
        );
        while (leftJoin.moveNext()) {
            blackhole.consume(leftJoin.currentElement().get(0));
        }
    }

    @Benchmark
    public void measureConsumeWindowBatchIterator(Blackhole blackhole) throws Exception {
        RowCollectExpression input = new RowCollectExpression(0);
        BatchIterator<Row> batchIterator = WindowFunctionBatchIterator.of(
            new InMemoryBatchIterator<>(rows, SENTINEL, false),
            new NoRowAccounting<>(),
            (partitionStart, partitionEnd, currentIndex, sortedRows) -> 0,
            (partitionStart, partitionEnd, currentIndex, sortedRows) -> currentIndex,
            (arg1, arg2) -> 0,
            (arg1, arg2) -> 0,
            1,
            () -> 1,
            Runnable::run,
            List.of(lastValueIntFunction),
            List.of(),
            new Boolean[]{null},
            new Input[]{input}
        );
        batchIterator.collect(Collectors.summingInt(x -> {
            blackhole.consume(x);
            return 1;
        })).get();
    }

    private static class NoRowAccounting<T> implements RowAccounting<T> {

        @Override
        public long accountForAndMaybeBreak(T row) {
            return 42L;
        }

        @Override
        public void release() {
        }
    }
}
