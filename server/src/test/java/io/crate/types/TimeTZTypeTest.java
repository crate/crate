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

package io.crate.types;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.function.Consumer;

import org.elasticsearch.test.ESTestCase;
import org.junit.Test;


public class TimeTZTypeTest extends ESTestCase {

    private Consumer<TimeTZ> isTZ(long value) {
        return timeTZ -> assertThat(timeTZ).isEqualTo(new TimeTZ(value, 0));
    }

    private static Consumer<TimeTZ> isTZ(long value, int zoneOffset) {
        return timeTZ -> assertThat(timeTZ).isEqualTo(new TimeTZ(value, zoneOffset));
    }

    @Test
    public void test_value_cast_null() {
        assertThat(TimeTZType.INSTANCE.implicitCast(null)).isNull();
    }

    @Test
    public void test_value_cast_not_null() {
        assertThat(TimeTZType.INSTANCE.implicitCast(new TimeTZ(3600000000L, 7200)))
            .satisfies(isTZ(3600000000L, 7200));
    }

    @Test
    public void test_implicit_cast_null() {
        assertThat(TimeTZType.INSTANCE.implicitCast(null)).isNull();
    }

    @Test
    public void test_implicit_cast_ISO_formats_with_time_zone() {
        assertThat(TimeTZType.INSTANCE.implicitCast("01:00:00     CET")).satisfiesAnyOf(
            isTZ(3600000000L, 3600),
            isTZ(3600000000L, 7200));
        assertThat(TimeTZType.INSTANCE.implicitCast("01:00:00     UTC")).satisfies(isTZ(3600000000L));
        assertThat(TimeTZType.INSTANCE.implicitCast("01:00:00     GMT")).satisfies(isTZ(3600000000L));
        assertThat(TimeTZType.INSTANCE.implicitCast("01:00:00  Z")).satisfies(isTZ(3600000000L));
        assertThat(TimeTZType.INSTANCE.implicitCast("01:00:00 +00")).satisfies(isTZ(3600000000L));
        assertThat(TimeTZType.INSTANCE.implicitCast("04:00:00-03:00")).satisfies(isTZ(14400000000L, -10800));
        assertThat(TimeTZType.INSTANCE.implicitCast("04:00:00+0300")).satisfies(isTZ(14400000000L, 10800));
        assertThat(TimeTZType.INSTANCE.implicitCast("04:00:00+03:00")).satisfies(isTZ(14400000000L, 10800));
        assertThat(TimeTZType.INSTANCE.implicitCast("04:00:00.123456789+03:00")).satisfies(isTZ(14400123456L, 10800));
        assertThat(TimeTZType.INSTANCE.implicitCast("04:00:00+0000")).satisfies(isTZ(14400000000L));
        assertThat(TimeTZType.INSTANCE.implicitCast("04:00:00.123456789-0000")).satisfies(isTZ(14400123456L));
    }

    @Test
    public void test_implicit_cast_ISO_formats_without_time_zone() {
        assertThat(TimeTZType.INSTANCE.implicitCast("01.99999")).satisfies(isTZ(3600999990L));
        assertThat(TimeTZType.INSTANCE.implicitCast("0110.99999")).satisfies(isTZ(4200999990L));
        assertThat(TimeTZType.INSTANCE.implicitCast("011101.99999")).satisfies(isTZ(4261999990L));
        assertThat(TimeTZType.INSTANCE.implicitCast("01:00:00.000")).satisfies(isTZ(3600000000L));
        assertThat(TimeTZType.INSTANCE.implicitCast("23:59:59.999998")).satisfies(isTZ(24 * 60 * 60 * 1000_000L - 2L));
    }

    @Test
    public void test_implicit_cast_short_hand_format_floating_point() {
        assertThat(TimeTZType.INSTANCE.implicitCast("010000.000")).satisfies(isTZ(3600000000L));
        assertThat(TimeTZType.INSTANCE.implicitCast("01:00:00.000")).satisfies(isTZ(3600000000L));
        assertThat(TimeTZType.INSTANCE.implicitCast("000000.000")).satisfies(isTZ(0L));
        assertThat(TimeTZType.INSTANCE.implicitCast("235959.999998")).satisfies(isTZ(24 * 60 * 60 * 1000_000L - 2L));
        assertThat(TimeTZType.INSTANCE.implicitCast("235959.998")).satisfies(isTZ(24 * 60 * 60 * 1000_000L - 2000L));
    }

    @Test
    public void test_implicit_cast_short_hand_format_long() {
        assertThat(TimeTZType.INSTANCE.implicitCast("010000")).satisfies(isTZ(3600000000L)); // same as 01:00:00.000
        assertThat(TimeTZType.INSTANCE.implicitCast("000000")).satisfies(isTZ(0L));
        assertThat(TimeTZType.INSTANCE.implicitCast("235959")).satisfies(isTZ(24 * 60 * 60 * 1000_000L - 1000_000L));
    }

    @Test
    public void test_implicit_cast_is_a_long_in_range() {
        assertThat(TimeTZType.INSTANCE.implicitCast("010000000")).satisfies(isTZ(10000000L));
        assertThat(TimeTZType.INSTANCE.implicitCast("000000000")).satisfies(isTZ(0L));
        assertThat(TimeTZType.INSTANCE.implicitCast(
            String.valueOf(24 * 60 * 60 * 1000L - 1L))).satisfies(isTZ(24 * 60 * 60 * 1000 - 1L));
    }
}
