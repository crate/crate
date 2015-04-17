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

package io.crate.benchmark;

import com.carrotsearch.junitbenchmarks.BenchmarkOptions;
import com.carrotsearch.junitbenchmarks.BenchmarkRule;
import com.carrotsearch.junitbenchmarks.annotation.AxisRange;
import com.carrotsearch.junitbenchmarks.annotation.BenchmarkHistoryChart;
import com.carrotsearch.junitbenchmarks.annotation.BenchmarkMethodChart;
import com.carrotsearch.junitbenchmarks.annotation.LabelType;
import io.crate.operation.scalar.SubstrFunction;
import org.apache.lucene.util.BytesRef;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import java.nio.charset.CharsetEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Random;

import static org.junit.Assert.assertEquals;

@AxisRange(min = 0)
@BenchmarkHistoryChart(filePrefix="benchmark-substr-history", labelWith = LabelType.CUSTOM_KEY)
@BenchmarkMethodChart(filePrefix = "benchmark-substr")
public class SubStrBenchmark {

    @Rule
    public TestRule benchmarkRun = new BenchmarkRule();

    static CharsetEncoder utf8Encoder = StandardCharsets.UTF_8.newEncoder();

    private static final Random RANDOM = new Random();

    static final String[] strings = getStrings(10_000, 100);
    static final BytesRef[] bytesRefs = toBytesRefs(strings);

    // helpers

    public static String getTestString(int length) {

        StringBuilder buffer = new StringBuilder();
        for (int i = 0; i < length; i++) {
            char c = (char) (RANDOM.nextInt(Character.MAX_VALUE));
            while (!utf8Encoder.canEncode(c)) {
                c = (char) (RANDOM.nextInt(Character.MAX_VALUE));
            }
            buffer.append(c);
        }
        return buffer.toString();
    }

    static String[] getStrings(int size, int stringSize) {
        String[] res = new String[size];
        for (int i = 0; i < res.length; i++) {
            res[i] = getTestString(stringSize);
        }
        return res;
    }

    private static BytesRef[] toBytesRefs(String[] strings) {
        BytesRef[] res = new BytesRef[strings.length];
        for (int i = 0; i < res.length; i++) {
            res[i] = new BytesRef(strings[i]);
        }
        return res;
    }

    // end of helpers

    @BenchmarkOptions(benchmarkRounds = 10, warmupRounds = 0)
    @Test
    public void testPos() throws Exception {
        for (int i = 0; i < strings.length; i++) {
            assertEquals(strings[i], bytesRefs[i].utf8ToString());
            assertEquals(strings[i].substring(10, 80), SubstrFunction.substring(bytesRefs[i], 10, 80).utf8ToString());
        }
    }
}
