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

package io.crate.operation.scalar;

import com.carrotsearch.junitbenchmarks.BenchmarkOptions;
import com.carrotsearch.junitbenchmarks.BenchmarkRule;
import org.apache.lucene.util.BytesRef;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import java.nio.charset.CharsetEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Random;

import static org.junit.Assert.assertEquals;

public class SubstrFunctionBytesRefTest {

    static CharsetEncoder utf8Encoder = StandardCharsets.UTF_8.newEncoder();

    private static final Random RANDOM = new Random();

    static final String[] strings = getStrings(10_000, 100);
    static final BytesRef[] bytesRefs = toBytesRefs(strings);
    static final int ITERATIONS = 200;


    @Rule
    public TestRule benchmarkRun = new BenchmarkRule();

    public static BytesRef substring(BytesRef utf8, int begin, int end) {
        int pos = utf8.offset;
        final int limit = pos + utf8.length;
        final byte[] bytes = utf8.bytes;
        int posBegin = pos;

        int codePointCount = 0;
        for (; pos < limit; codePointCount++) {
            if (codePointCount == begin) {
                posBegin = pos;
            }
            if (codePointCount == end) {
                break;
            }

            int v = bytes[pos] & 0xFF;
            if (v <   /* 0xxx xxxx */ 0x80) {
                pos += 1;
                continue;
            }
            if (v >=  /* 110x xxxx */ 0xc0) {
                if (v < /* 111x xxxx */ 0xe0) {
                    pos += 2;
                    continue;
                }
                if (v < /* 1111 xxxx */ 0xf0) {
                    pos += 3;
                    continue;
                }
                if (v < /* 1111 1xxx */ 0xf8) {
                    pos += 4;
                    continue;
                }
                // fallthrough, consider 5 and 6 byte sequences invalid.
            }

            // Anything not covered above is invalid UTF8.
            throw new IllegalArgumentException();
        }

        // Check if we didn't go over the limit on the last character.
        if (pos > limit) throw new IllegalArgumentException();
        utf8.offset = posBegin;
        utf8.length = pos - posBegin;
        return utf8;
    }


    public static String getTestString(int length) {


        StringBuilder buffer = new StringBuilder();
        for (int i = 0; i < length; i++) {
            char c = (char) (RANDOM.nextInt(Character.MAX_VALUE));
            while (!utf8Encoder.canEncode(c)){
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

    @BenchmarkOptions(benchmarkRounds = 1, warmupRounds = 0)
    @Test
    public void testPos() throws Exception {
        for (int i = 0; i < strings.length; i++) {
            assertEquals(strings[i], bytesRefs[i].utf8ToString());
            assertEquals(strings[i].substring(10, 80), substring(bytesRefs[i].clone(), 10, 80).utf8ToString());
        }
    }


    private static BytesRef[] toBytesRefs(String[] strings) {
        BytesRef[] res = new BytesRef[strings.length];
        for (int i = 0; i < res.length; i++) {
            res[i] = new BytesRef(strings[i]);
        }
        return res;
    }

    @BenchmarkOptions(benchmarkRounds = 2, warmupRounds = 1)
    @Test
    public void testStringPerf() throws Exception {
        String s = "";
        for (int n = 0; n < ITERATIONS; n++) {
            for (int i = 0; i < strings.length; i++) {
                s = strings[i].substring(4, 26);
            }
        }
        System.out.println("string: " + s);
    }


    @BenchmarkOptions(benchmarkRounds = 2, warmupRounds = 1)
    @Test
    public void testBytesRefPerf() throws Exception {

        BytesRef b = new BytesRef();
        for (int n = 0; n < ITERATIONS; n++) {
            for (int i = 0; i < strings.length; i++) {
                b = substring(bytesRefs[i].clone(), 4, 26);
            }
        }
        System.out.println("ref: " + b.utf8ToString());
    }

    @BenchmarkOptions(benchmarkRounds = 2, warmupRounds = 1)
    @Test
    public void testBytesRefToStringPerf() throws Exception {

        BytesRef b = new BytesRef();
        for (int n = 0; n < ITERATIONS; n++) {
            for (int i = 0; i < strings.length; i++) {
                b = new BytesRef(bytesRefs[i].utf8ToString().substring(4, 26));
            }
        }
        System.out.println("ref: " + b.utf8ToString());
    }
}

