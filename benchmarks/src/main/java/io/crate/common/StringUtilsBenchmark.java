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

package io.crate.common;

import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 2)
@Fork(value = 2)
@Measurement(iterations = 3)
public class StringUtilsBenchmark {

    @Benchmark
    public Long measure_Long_parseLong_valid() {
        try {
            return Long.parseLong("1658149133924");
        } catch (NumberFormatException ex) {
            return null;
        }
    }

    @Benchmark
    public Long measure_Long_parseLong_invalid() {
        try {
            return Long.parseLong("2022-07-18");
        } catch (NumberFormatException ex) {
            return null;
        }
    }

    @Benchmark
    public Long measure_StringUtils_tryParseLong_valid() {
        long[] out = StringUtils.PARSE_LONG_BUFFER.get();
        if (StringUtils.tryParseLong("1658149133924", out)) {
            return out[0];
        }
        return null;
    }

    @Benchmark
    public Long measure_StringUtils_tryParseLong_invalid() {
        long[] out = StringUtils.PARSE_LONG_BUFFER.get();
        if (StringUtils.tryParseLong("2022-07-18", out)) {
            return out[0];
        }
        return null;
    }
}
