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
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
public class RowsBatchIteratorBenchmark {

    // use materialize to not have shared row instances
    // this is done in the startup, otherwise the allocation costs will make up the majority of the benchmark.
    private List<Row> rows = StreamSupport.stream(RowGenerator.range(0, 10_000_000).spliterator(), false)
        .map(Row::materialize)
        .map(RowN::new)
        .collect(Collectors.toList());

    // use RowsBatchIterator without any state/error handling to establish a performance baseline.
    private BatchIterator it = new RowsBatchIterator(rows);

    private BatchIterator itCloseAsserting = new CloseAssertingBatchIterator(it);
    private BatchIterator skippingIt = new SkippingBatchIterator(it, 100);

    @Benchmark
    public void measureConsumeBatchIterator(Blackhole blackhole) throws Exception {
        while (it.moveNext()) {
            blackhole.consume(it.currentRow().get(0));
        }
        it.moveToStart();
    }

    @Benchmark
    public void measureConsumeCloseAssertingIterator(Blackhole blackhole) throws Exception {
        while (itCloseAsserting.moveNext()) {
            blackhole.consume(itCloseAsserting.currentRow().get(0));
        }
        itCloseAsserting.moveToStart();
    }

    @Benchmark
    public void measureConsumeSkippingBatchIterator(Blackhole blackhole) throws Exception {
        while (skippingIt.moveNext()) {
            blackhole.consume(skippingIt.currentRow().get(0));
        }
        skippingIt.moveToStart();
    }
}
