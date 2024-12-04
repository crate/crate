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

import static io.crate.types.TimeTZParser.parse;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.function.Consumer;

import org.elasticsearch.test.ESTestCase;
import org.junit.Test;


public class TimeTZParserTest extends ESTestCase {

    private static Consumer<TimeTZ> isTZ(long value, int zoneOffset) {
        return timeTZ -> assertThat(timeTZ).isEqualTo(new TimeTZ(value, zoneOffset));
    }

    @Test
    public void test_parse_time_correct_syntax_no_colon() {
        assertThat(parse("12")).satisfies(isTZ(43200000000L, 0));
        assertThat(parse("12.000001")).satisfies(isTZ(43200000001L, 0));
        assertThat(parse("1200")).satisfies(isTZ(43200000000L, 0));
        assertThat(parse("1200.002")).satisfies(isTZ(43200002000L, 0));
        assertThat(parse("120000")).satisfies(isTZ(43200000000L, 0));
        assertThat(parse("120000.000003")).satisfies(isTZ(43200000003L, 0));
    }

    @Test
    public void test_parse_time_correct_syntax_colon() {
        assertThat(parse("12:00")).satisfies(isTZ(43200000000L, 0));
        assertThat(parse("12:00.999")).satisfies(isTZ(43200999000L, 0));
        assertThat(parse("12:00:00")).satisfies(isTZ(43200000000L, 0));
        assertThat(parse("12:00:00.003")).satisfies(isTZ(43200003000L, 0));
    }

    @Test
    public void test_parse_time_correct_syntax_tz() {
        assertThat(parse("00+12")).satisfies(isTZ(0L, 12 * 3600));
        assertThat(parse("00+1200")).satisfies(isTZ(0L, 12 * 3600));
        assertThat(parse("00-12:00")).satisfies(isTZ(0L, -12 * 3600));
        assertThat(parse("00.0+12")).satisfies(isTZ(0L, 12 * 3600));
        assertThat(parse("00.0+1200")).satisfies(isTZ(0L, 12 * 3600));
        assertThat(parse("00.000001  +12:00")).satisfies(isTZ(1L, 12 * 3600));
    }

    @Test
    public void test_parse_time_range_overflow() {
        assertThatThrownBy(() -> parse("24:00:00.000001"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Text '24:00:00.000001' could not be parsed: Invalid value for HourOfDay (valid values 0 - 23): 24");
    }

    @Test
    public void test_parse_time_unsupported_literal_long() {
        assertThatThrownBy(() -> parse("234"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Text '234' could not be parsed, unparsed text found at index 2");
    }

    @Test
    public void test_parse_time_unsupported_literal_floating_point() {
        assertThatThrownBy(() -> parse("234.9999"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Text '234.9999' could not be parsed, unparsed text found at index 2");
    }

    @Test
    public void test_parse_time_out_of_range_hh() {
        assertThatThrownBy(() -> parse("25"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Text '25' could not be parsed: Invalid value for HourOfDay (valid values 0 - 23): 25");
    }

    @Test
    public void test_parse_time_out_of_range_hhmm() {
        assertThatThrownBy(() -> parse("1778"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Text '1778' could not be parsed: Invalid value for MinuteOfHour (valid values 0 - 59): 78");
    }

    @Test
    public void test_parse_time_out_of_range_hhmmss() {
        assertThatThrownBy(() -> parse("175978"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Text '175978' could not be parsed: Invalid value for SecondOfMinute (valid values 0 - 59): 78");
    }

    @Test
    public void test_parse_time_out_of_range_hh_floating_point() {
        assertThatThrownBy(() -> parse("25.999999"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Invalid value for HourOfDay (valid values 0 - 23): 25");
    }

    @Test
    public void test_parse_time_out_of_range_hhmm_floating_point() {
        assertThatThrownBy(() -> parse("1778.999999"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Invalid value for MinuteOfHour (valid values 0 - 59): 78");
    }

    @Test
    public void test_parse_time_out_of_range_hhmmss_floating_point() {
        assertThatThrownBy(() -> parse("175978.999999"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Text '175978.999999' could not be parsed: Invalid value for SecondOfMinute (valid values 0 - 59): 78");
    }

    @Test
    public void test_parse_time() {
        assertThat(parse("04")).satisfies(isTZ(4 * 60 * 60 * 1000_000L, 0));
        assertThat(parse("0400")).isEqualTo(parse("04"));
        assertThat(parse("04:00")).isEqualTo(parse("04"));
        assertThat(parse("040000")).isEqualTo(parse("04"));
        assertThat(parse("04:00:00")).isEqualTo(parse("04"));
        assertThat(parse("040000.0")).isEqualTo(parse("04"));
        assertThat(parse("04:00:00.0")).isEqualTo(parse("04"));
        assertThat(parse("04:00:00.0+00")).isEqualTo(parse("04+00"));
        assertThat(parse("04:00:00.0+0000")).isEqualTo(parse("04+00"));
        assertThat(parse("04:00:00.0+00:00")).isEqualTo(parse("04+00"));
    }

    @Test
    public void test_format_time() {
        assertThat(TimeTZParser.formatTime(new TimeTZ(14400000000L, 0))).isEqualTo("04:00:00");
        assertThat(TimeTZParser.formatTime(new TimeTZ(14400123000L, 0))).isEqualTo("04:00:00.123");
        assertThat(TimeTZParser.formatTime(new TimeTZ(14400123666L, 65))).isEqualTo("04:00:00.123666+00:01:05");
    }

    @Test
    public void test_format_time_with_tz() {
        assertThat(TimeTZParser.formatTime(new TimeTZ(14400123000L, 123))).isEqualTo("04:00:00.123+00:02:03");
        assertThat(TimeTZParser.formatTime(new TimeTZ(14400123666L, 14 * 3600 + 59 * 60))).isEqualTo("04:00:00.123666+14:59");
    }
}
