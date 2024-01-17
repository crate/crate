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

package io.crate.execution;

import java.time.Clock;
import java.time.Instant;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

import org.joda.time.DateTimeUtils;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Timeout;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.OptionsBuilder;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
@Fork(value = 1)
@Measurement(iterations = 4)
@Timeout(time = 30000, timeUnit = TimeUnit.MILLISECONDS)
@Warmup(iterations = 2, time = 10000, timeUnit = TimeUnit.MILLISECONDS)
public class TimePrecisionIncreaseTest {

    @Benchmark
    public void currentTimeMillisOriginal(Blackhole blackhole) {
        blackhole.consume(DateTimeUtils.currentTimeMillis());
    }

    @Benchmark
    public void currentTimeMillisNextGen(Blackhole blackhole) {
        LongSupplier currentTimeMillis = () -> {
            Instant i = Clock.systemUTC().instant();
            return (i.getEpochSecond() * 1000_000_000L + i.getNano()) / 1000_000L;
        };
        blackhole.consume(currentTimeMillis.getAsLong());
    }

    public static void main(String[] args) throws Exception {
        // Benchmark                                            Mode  Cnt  Score   Error  Units
        // TimeIncreasePrecisionTest.currentTimeMillisNextGen   avgt    4  0.039 ± 0.001  us/op
        // TimeIncreasePrecisionTest.currentTimeMillisOriginal  avgt    4  0.028 ± 0.001  us/op
        new Runner(
            new OptionsBuilder()
                .include(TimePrecisionIncreaseTest.class.getSimpleName())
                .build())
            .run();
    }
}
