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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;

public class SubstrFunctionBytesRefTest {

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

    @Rule
    public TestRule benchmarkRun = new BenchmarkRule();

    @BenchmarkOptions(benchmarkRounds = 1, warmupRounds = 0)
    @Test
    public void testPos() throws Exception {
        for (int i = 0; i < strings.length; i++) {
            assertEquals(strings[i], bytesRefs[i].utf8ToString());
            assertEquals(strings[i].substring(10, 80), SubstrFunction.substring(bytesRefs[i], 10, 80).utf8ToString());
        }
    }

    @Test
    public void testNoCopy() throws Exception {
        BytesRef ref = new BytesRef("i do not want to be copied!");
        BytesRef sub1 = SubstrFunction.substring(ref, 0, 10);
        BytesRef sub2 = SubstrFunction.substring(ref, 5, 14);
        assertThat(sub1.utf8ToString(), is("i do not w"));
        assertThat(sub2.utf8ToString(), is("not want "));
        assertThat(ref.bytes, allOf(is(sub2.bytes), is(sub1.bytes)));
    }

}

