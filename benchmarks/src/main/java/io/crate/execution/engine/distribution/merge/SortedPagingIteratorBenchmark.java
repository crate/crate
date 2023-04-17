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

package io.crate.execution.engine.distribution.merge;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;

import io.crate.data.Row;
import io.crate.data.Row1;
import io.crate.execution.engine.sort.OrderingByPosition;
import io.crate.types.DataTypes;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
public class SortedPagingIteratorBenchmark {

    private Comparator<Row> compareOnFirstColumn;
    private Random rnd;
    private List<Row> unsortedSecond;
    private List<Row> unsortedFirst;
    private List<Row> sortedFirst;
    private List<Row> sortedSecond;

    @Setup
    public void prepareData() {
        rnd = new Random(42);
        compareOnFirstColumn = OrderingByPosition.rowOrdering(DataTypes.INTEGER, 0, false, false);

        unsortedFirst = IntStream.range(0, 1_000_000).mapToObj(Row1::new).collect(Collectors.toList());
        unsortedSecond = IntStream.range(500_000, 1_500_00).mapToObj(Row1::new).collect(Collectors.toList());
        Collections.shuffle(unsortedFirst, rnd);
        Collections.shuffle(unsortedSecond, rnd);

        sortedFirst = new ArrayList<>(unsortedFirst);
        sortedFirst.sort(compareOnFirstColumn);

        sortedSecond = new ArrayList<>(unsortedSecond);
        sortedSecond.sort(compareOnFirstColumn);
    }

    @Benchmark
    public void measurePagingIteratorWithRepeat(Blackhole blackhole) {
        PagingIterator<Integer, Row> pagingIterator = PagingIterator.createSorted(compareOnFirstColumn, true);
        pagingIterator.merge(Arrays.asList(new KeyIterable<>(1, sortedFirst), new KeyIterable<>(2, sortedSecond)));
        pagingIterator.finish();

        while (pagingIterator.hasNext()) {
            blackhole.consume(pagingIterator.next());
        }
    }

    @Benchmark
    public void measurePagingIteratorWithoutRepeat(Blackhole blackhole) {
        PagingIterator<Integer, Row> pagingIterator = PagingIterator.createSorted(compareOnFirstColumn, false);
        pagingIterator.merge(Arrays.asList(new KeyIterable<>(1, sortedFirst), new KeyIterable<>(2, sortedSecond)));
        pagingIterator.finish();

        while (pagingIterator.hasNext()) {
            blackhole.consume(pagingIterator.next());
        }
    }

    @Benchmark
    public void measurePagingIteratorWithSort(Blackhole blackhole) throws Exception {
        unsortedFirst.sort(compareOnFirstColumn);
        unsortedSecond.sort(compareOnFirstColumn);

        PagingIterator<Integer, Row> pagingIterator = PagingIterator.createSorted(compareOnFirstColumn, false);
        pagingIterator.merge(Arrays.asList(new KeyIterable<>(1, sortedFirst), new KeyIterable<>(2, sortedSecond)));
        pagingIterator.finish();

        while (pagingIterator.hasNext()) {
            blackhole.consume(pagingIterator.next());
        }
        Collections.shuffle(unsortedFirst, rnd);
        Collections.shuffle(unsortedSecond, rnd);
    }


    @Benchmark
    public void measureListSort(Blackhole blackhole) {
        ArrayList<Row> allRows = new ArrayList<>(unsortedFirst);
        allRows.addAll(unsortedSecond);
        allRows.sort(compareOnFirstColumn);
        for (Row row : allRows) {
            blackhole.consume(row);
        }
        // reset to avoid sorting a already sorted list in the next iteration
        Collections.shuffle(unsortedFirst, rnd);
        Collections.shuffle(unsortedSecond, rnd);
    }
}
