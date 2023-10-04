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

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;


public final class StringUtilsTest {

    @Test
    public void test_split_str_by_dots() {
        var parts = StringUtils.splitToList('.', "a.b.c");
        assertThat(parts).containsExactly("a", "b", "c");
    }

    @Test
    public void test_split_str_with_repeated_delim() {
        var parts = StringUtils.splitToList('.', "a..c");
        assertThat(parts).containsExactly("a", "", "c");
    }

    @Test
    public void test_split_empty_str() {
        var parts = StringUtils.splitToList('.', "");
        assertThat(parts).containsExactly("");
    }

    @Test
    public void test_tryParseLong() {
        /* Created after reviewing https://github.com/AdoptOpenJDK/openjdk-jdk/blob/master/test/jdk/java/lang/Long/ParsingTest.java */
        boolean wasLong = false;
        long[] outputLong = new long[1];

        wasLong = StringUtils.tryParseLong("-123456", outputLong);
        assertThat(wasLong).isEqualTo(true);
        assertThat(outputLong[0]).isEqualTo(-123456);

        wasLong = StringUtils.tryParseLong("9223372036854775808", outputLong);
        assertThat(wasLong).isEqualTo(false);

        wasLong = StringUtils.tryParseLong("/", outputLong);
        assertThat(wasLong).isEqualTo(false);

        wasLong = StringUtils.tryParseLong("+", outputLong);
        assertThat(wasLong).isEqualTo(false);

        wasLong = StringUtils.tryParseLong("", outputLong);
        assertThat(wasLong).isEqualTo(false);

        wasLong = StringUtils.tryParseLong("++", outputLong);
        assertThat(wasLong).isEqualTo(false);
    }
}
