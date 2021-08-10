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

package io.crate.execution.engine.sort;

import io.crate.data.BatchIterator;
import io.crate.data.Bucket;
import io.crate.data.CollectingBatchIterator;
import io.crate.data.InMemoryBatchIterator;
import io.crate.data.Input;
import io.crate.data.Row;
import io.crate.data.RowN;
import io.crate.execution.engine.collect.CollectExpression;
import io.crate.execution.engine.collect.InputCollectExpression;
import io.crate.testing.RowGenerator;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static io.crate.data.SentinelRow.SENTINEL;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
public class SortingTopNCollectorBenchmark {

    private static final Comparator<Object[]> COMPARATOR = (o1, o2) -> Integer.compare((int) o2[0], (int) o1[0]);
    private static final InputCollectExpression INPUT = new InputCollectExpression(0);
    private static final List<Input<?>> INPUTS = List.of(INPUT);
    private static final Iterable<CollectExpression<Row, ?>> COLLECT_EXPRESSIONS = List.of(INPUT);

    private List<Row> rows = StreamSupport.stream(RowGenerator.range(0, 10_000_000).spliterator(), false)
        .map(Row::materialize)
        .map(RowN::new)
        .collect(Collectors.toList());

    private Collector<Row, ?, Bucket> boundedSortingCollector;
    private Collector<Row, ?, Bucket> unboundedSortingCollector;

    @Setup
    public void setUp() {
        boundedSortingCollector = new BoundedSortingTopNCollector(
            new IgnoreRowCellsAccounting(),
            INPUTS,
            COLLECT_EXPRESSIONS,
            1,
            COMPARATOR,
            10_000,
            0);

        unboundedSortingCollector = new UnboundedSortingTopNCollector(
            new IgnoreRowCellsAccounting(),
            INPUTS,
            COLLECT_EXPRESSIONS,
            1,
            COMPARATOR,
            10_000,
            10_000,
            0);
    }

    @Benchmark
    public Object measureBoundedSortingCollector() throws Exception {
        BatchIterator<Row> it = new InMemoryBatchIterator<>(rows, SENTINEL, false);
        BatchIterator<Row> sortingBatchIterator = CollectingBatchIterator.newInstance(it, boundedSortingCollector);
        return sortingBatchIterator.loadNextBatch().toCompletableFuture().join();
    }

    @Benchmark
    public Object measureUnboundedSortingCollector() throws Exception {
        BatchIterator<Row> it = new InMemoryBatchIterator<>(rows, SENTINEL, false);
        BatchIterator<Row> sortingBatchIterator = CollectingBatchIterator.newInstance(it, unboundedSortingCollector);
        return sortingBatchIterator.loadNextBatch().toCompletableFuture().join();
    }
}
