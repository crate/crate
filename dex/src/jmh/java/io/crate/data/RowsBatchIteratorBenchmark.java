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

package io.crate.data;

import io.crate.testing.RowGenerator;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
public class RowsBatchIteratorBenchmark {

    // use materialize to not have shared row instances
    // this is done in the startup, otherwise the allocation costs will make up the majority of the benchmark.
    private List<Row> rows = StreamSupport.stream(RowGenerator.range(0, 10_000_000).spliterator(), false)
        .map(Row::materialize)
        .map(RowN::new)
        .collect(Collectors.toList());

    // use RowsBatchIterator without any state/error handling to establish a performance baseline.
    private BatchIterator it = new RowsBatchIterator(rows, 1);
    private BatchIterator crossJoin = NestedLoopBatchIterator.crossJoin(
        RowsBatchIterator.newInstance(IntStream.range(0, 1000).mapToObj(Row1::new).collect(Collectors.toList()), 1),
        RowsBatchIterator.newInstance(IntStream.range(0, 10000).mapToObj(Row1::new).collect(Collectors.toList()), 1)
    );

    private BatchIterator leftJoin = NestedLoopBatchIterator.leftJoin(
        RowsBatchIterator.newInstance(IntStream.range(0, 1000).mapToObj(Row1::new).collect(Collectors.toList()), 1),
        RowsBatchIterator.newInstance(IntStream.range(0, 10000).mapToObj(Row1::new).collect(Collectors.toList()), 1),
        columns -> new BooleanSupplier() {

            Input<?> col1 = columns.get(0);
            Input<?> col2 = columns.get(1);

            @Override
            public boolean getAsBoolean() {
                return Objects.equals(col1.value(), col2.value());
            }
        }
    );

    private BatchIterator itCloseAsserting = new CloseAssertingBatchIterator(it);
    private BatchIterator skippingIt = new SkippingBatchIterator(it, 100);

    @Benchmark
    public void measureConsumeBatchIterator(Blackhole blackhole) throws Exception {
        final Input<?> input = it.rowData().get(0);
        while (it.moveNext()) {
            blackhole.consume(input.value());
        }
    }

    @Benchmark
    public void measureConsumeCloseAssertingIterator(Blackhole blackhole) throws Exception {
        final Input<?> input = itCloseAsserting.rowData().get(0);
        while (itCloseAsserting.moveNext()) {
            blackhole.consume(input.value());
        }
    }

    @Benchmark
    public void measureConsumeSkippingBatchIterator(Blackhole blackhole) throws Exception {
        final Input<?> input = skippingIt.rowData().get(0);
        while (skippingIt.moveNext()) {
            blackhole.consume(input.value());
        }
    }

    @Benchmark
    public void measureConsumeNestedLoopJoin(Blackhole blackhole) throws Exception {
        final Input<?> input = crossJoin.rowData().get(0);
        while (crossJoin.moveNext()) {
            blackhole.consume(input.value());
        }
    }

    @Benchmark
    public void measureConsumeNestedLoopLeftJoin(Blackhole blackhole) throws Exception {
        final Input<?> input = leftJoin.rowData().get(0);
        while (leftJoin.moveNext()) {
            blackhole.consume(input.value());
        }
    }
}
