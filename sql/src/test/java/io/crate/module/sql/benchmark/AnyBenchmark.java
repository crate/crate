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

package io.crate.module.sql.benchmark;

import com.carrotsearch.junitbenchmarks.BenchmarkOptions;
import com.carrotsearch.junitbenchmarks.BenchmarkRule;
import com.carrotsearch.junitbenchmarks.annotation.AxisRange;
import com.carrotsearch.junitbenchmarks.annotation.BenchmarkHistoryChart;
import com.carrotsearch.junitbenchmarks.annotation.BenchmarkMethodChart;
import com.carrotsearch.junitbenchmarks.annotation.LabelType;
import com.carrotsearch.randomizedtesting.generators.RandomStrings;
import com.google.common.base.Joiner;
import com.google.common.collect.AbstractIterator;
import io.crate.action.sql.SQLRequest;
import io.crate.action.sql.SQLResponse;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.util.Iterator;
import java.util.Random;

@AxisRange(min = 0)
@BenchmarkHistoryChart(filePrefix="benchmark-any-history", labelWith = LabelType.CUSTOM_KEY)
@BenchmarkMethodChart(filePrefix = "benchmark-any")
public class AnyBenchmark extends BenchmarkBase {

    static final int BENCHMARK_ROUNDS = 200;
    static {
        ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(true);
    }

    private static Iterator<String> strings(final int num, final int length) {
        return new AbstractIterator<String>() {
            Random random = new Random(System.currentTimeMillis());
            int i = num;

            @Override
            protected String computeNext() {
                if (i-- <= 0) {
                    endOfData();
                    return null;
                }
                return RandomStrings.randomAsciiOfLength(random, length);
            }
        };
    }

    static final String SELECT_IN_2000_STATEMENT
             = "select * from " + INDEX_NAME + " where \"countryCode\" in ('" +
            Joiner.on("','").join(strings(2000, 2)) + "')";

    static final String COUNT_IN_2000_STATEMENT = "select * from " + INDEX_NAME + " where \"countryCode\" in ('" +
            Joiner.on("','").join(strings(2000, 2)) + "')";

    static final String SELECT_ANY_2K = "select * from "+ INDEX_NAME + " where \"countryCode\" = ANY (['" +
            Joiner.on("','").join(strings(2000, 2)) + "'])";

    private static final String SELECT_ANY_500 = "select * from "+ INDEX_NAME + " where \"countryCode\" = ANY (['" +
            Joiner.on("','").join(strings(500, 2)) + "'])";

    @Rule
    public TestRule benchmarkRun = RuleChain.outerRule(new BenchmarkRule()).around(super.ruleChain);

    @Override
    public boolean loadData() {
        return true;
    }

    public SQLResponse execute(String stmt) {
        return super.execute(stmt, SQLRequest.EMPTY_ARGS, false);
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = 1)
    @Test
    public void testSelectWhereInWith2000Items() throws Exception {
        // uses lucene query builder
        execute(SELECT_IN_2000_STATEMENT);
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = 1)
    @Test
    public void testCountWhereInWith2000Items() throws Exception {
        // uses ES query builder
        execute(COUNT_IN_2000_STATEMENT);
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = 1)
    @Test
    public void testSelectAny2K() throws Exception {
        execute(SELECT_ANY_2K);
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = 1)
    @Test
    public void testSelectAny500() throws Exception {
        execute(SELECT_ANY_500);
    }
}
