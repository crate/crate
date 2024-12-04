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

package io.crate.collections.accountable;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.elasticsearch.common.Randomness;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;

import io.crate.common.MutableLong;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
public class AccountableListBenchmark {

    private ArrayList<Integer> unsortedRandomNumbers;
    private ArrayList<Integer> listToSort;
    AccountableList<Integer> accountableListToSort;
    List<Integer> subListToSort;
    List<Integer> accountableSubListToSort;

    private ArrayList<Integer> addAllList;
    private Random random;

    @Setup
    public void setup() {
        addAllList = new ArrayList<>();
        for (int i = 0; i < 1_000_000; i++) {
            addAllList.add(i);
        }

        // We generate random values for benchmarking "sort" as values affect execution time.
        // Generate only once and re-use same list in all benchmarks.
        random = Randomness.get();
        unsortedRandomNumbers = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            unsortedRandomNumbers.add(random.nextInt());
        }
    }

    /**
     * listToSort becomes sorted after each iteration.
     * We reset it back to unsorted state (filled by same random numbers) after each iteration.
     * Similar to https://github.com/openjdk/jdk/blob/master/test/micro/org/openjdk/bench/java/util/ArraysSort.java#L107C5-L113
     */
    @Setup(Level.Invocation)
    public void clear() {
        listToSort = new ArrayList<>();
        listToSort.addAll(unsortedRandomNumbers);

        MutableLong mutableLong = new MutableLong(0);
        accountableListToSort = new AccountableList<>(mutableLong::add);
        accountableListToSort.addAll(unsortedRandomNumbers);

        subListToSort = listToSort.subList(0, 500);
        accountableSubListToSort = accountableListToSort.subList(0, 500);
    }

    @Benchmark
    public void measureArrayListAdd(Blackhole blackhole) {
        // We don't use random numbers as we don't want to add random generation noise to the benchmark.
        // Value doesn't matter for add().
        ArrayList<Integer> list = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            list.add(i);
        }
        blackhole.consume(list);
    }

    @Benchmark
    public void measureAccountableListAdd(Blackhole blackhole) {
        // We don't use random numbers as we don't want to add random generation noise to the benchmark.
        // Value doesn't matter for add().
        MutableLong mutableLong = new MutableLong(0);
        AccountableList<Integer> list = new AccountableList<>(mutableLong::add);
        for (int i = 0; i < 1000; i++) {
            list.add(i);
        }
        blackhole.consume(list);
    }

    @Benchmark
    public void measureArrayListAddAll(Blackhole blackhole) {
        ArrayList<Integer> list = new ArrayList<>();
        list.addAll(addAllList);
        blackhole.consume(list);
    }

    @Benchmark
    public void measureAccountableListAddAll(Blackhole blackhole) {
        MutableLong mutableLong = new MutableLong(0);
        AccountableList<Integer> list = new AccountableList<>(mutableLong::add);
        list.addAll(addAllList);
        blackhole.consume(list);
    }

    @Benchmark
    public void measureArrayListSort(Blackhole blackhole) {
        listToSort.sort(Comparator.comparingInt(x -> x));
        blackhole.consume(listToSort);
    }

    @Benchmark
    public void measureAccountableListSort(Blackhole blackhole) {
        accountableListToSort.sort(Comparator.comparingInt(x -> x));
        blackhole.consume(listToSort);
    }

    @Benchmark
    public void measureArraySubListSort(Blackhole blackhole) {
        subListToSort.sort(Comparator.comparingInt(x -> x));
        blackhole.consume(listToSort);
    }

    @Benchmark
    public void measureAccountableSubListSort(Blackhole blackhole) {
        accountableSubListToSort.sort(Comparator.comparingInt(x -> x));
        blackhole.consume(listToSort);
    }
}
