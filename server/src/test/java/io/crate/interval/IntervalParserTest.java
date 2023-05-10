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

package io.crate.interval;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.elasticsearch.test.ESTestCase;
import org.joda.time.Period;
import org.joda.time.PeriodType;
import org.junit.Test;

public class IntervalParserTest extends ESTestCase {

    @Test
    public void parse_year_month_day_hours_minutes() {
        Period result = IntervalParser.apply("120-1 1 15:30");
        assertThat(result).isEqualTo(
            new Period().withYears(120).withMonths(1).withDays(1).withHours(15).withMinutes(30)
                .withPeriodType(PeriodType.yearMonthDayTime()));
    }

    @Test
    public void negative_parse_year_month_negative_day_negative_hours_minutes() {
        Period result = IntervalParser.apply("-120-1 -1 -15:30");
        assertThat(result).isEqualTo(
            new Period().withYears(-120).withMonths(-1).withDays(-1).withHours(-15).withMinutes(-30)
                .withPeriodType(PeriodType.yearMonthDayTime()));
    }

    @Test
    public void parse_seconds() {
        Period result = IntervalParser.apply("1");
        assertThat(result).isEqualTo(new Period().withSeconds(1).withPeriodType(PeriodType.yearMonthDayTime()));
    }

    @Test
    public void parse_year_month_day() {
        Period result = IntervalParser.apply("120-1 1");
        assertThat(result).isEqualTo(new Period().withYears(120).withMonths(1).withDays(1)
                                         .withPeriodType(PeriodType.yearMonthDayTime()));
    }

    @Test
    public void parse_negative_year_month_negative_day() {
        Period result = IntervalParser.apply("-120-1 -1");
        assertThat(result).isEqualTo(new Period().withYears(-120).withMonths(-1).withDays(-1)
                                         .withPeriodType(PeriodType.yearMonthDayTime()));
    }

    @Test
    public void parse_year_month() {
        Period result = IntervalParser.apply("120-1");
        assertThat(result).isEqualTo(new Period().withYears(120).withMonths(1)
                                         .withPeriodType(PeriodType.yearMonthDayTime()));
    }

    @Test
    public void parse_negative_year_month() {
        Period result = IntervalParser.apply("-120-1");
        assertThat(result).isEqualTo(
            new Period().withYears(-120).withMonths(-1).withPeriodType(PeriodType.yearMonthDayTime()));
    }

    @Test
    public void parse_year_month_hours_minutes() {
        Period result = IntervalParser.apply("120-1 15:30");
        assertThat(result).isEqualTo(
            new Period().withYears(120).withMonths(1).withHours(15).withMinutes(30)
                .withPeriodType(PeriodType.yearMonthDayTime()));
    }

    @Test
    public void parse_hours_minutes() {
        Period result = IntervalParser.apply("15:30");
        assertThat(result).isEqualTo(new Period().withHours(15).withMinutes(30)
                                         .withPeriodType(PeriodType.yearMonthDayTime()));
    }

    @Test
    public void parse_negative_hours_minutes() {
        Period result = IntervalParser.apply("-15:30");
        assertThat(result).isEqualTo(new Period().withHours(-15).withMinutes(-30)
                                         .withPeriodType(PeriodType.yearMonthDayTime()));
    }

    @Test
    public void parse_hours_minutes_seconds() {
        Period result = IntervalParser.apply("15:30:10");
        assertThat(result).isEqualTo(new Period().withHours(15).withMinutes(30).withSeconds(10)
                                         .withPeriodType(PeriodType.yearMonthDayTime()));
    }

    @Test
    public void parse_days_hours_minutes_seconds() {
        Period result = IntervalParser.apply("1 15:30:10");
        assertThat(result).isEqualTo(new Period().withDays(1).withHours(15).withMinutes(30).withSeconds(10)
                                         .withPeriodType(PeriodType.yearMonthDayTime()));
    }

    @Test
    public void parse_negative_days_negative_hours_minutes_seconds() {
        Period result = IntervalParser.apply("-1 -15:30:10");
        assertThat(result).isEqualTo(new Period().withDays(-1).withHours(-15).withMinutes(-30).withSeconds(-10)
                                         .withPeriodType(PeriodType.yearMonthDayTime()));
    }

    @Test
    public void parse_days_seconds() {
        assertThatThrownBy(() -> IntervalParser.apply("1 1"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Invalid interval format 1 1");
    }

    @Test
    public void parse_negative_days_negative_seconds() {
        assertThatThrownBy(() -> IntervalParser.apply("-1 -1"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Invalid interval format -1 -1");
    }

    @Test
    public void parse_invalid_input_0() {
        assertThatThrownBy(() -> IntervalParser.apply("10-1-1-1-1-1"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Invalid interval format 10-1-1-1-1-1");
    }

    @Test
    public void parse_invalid_input_1() {
        assertThatThrownBy(() -> IntervalParser.apply("10:1:1:1:N1:1"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Invalid interval format 10:1:1:1:N1:1");
    }

    @Test
    public void parse_invalid_input_2() {
        assertThatThrownBy(() -> IntervalParser.apply("1-2 3 4-5-6"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Invalid interval format 1-2 3 4-5-6");
    }

    @Test
    public void parse_invalid_input_3() {
        Period result = IntervalParser.apply("0-0 0 0:0:0");
        assertThat(result).isEqualTo(Period.ZERO.withPeriodType(PeriodType.yearMonthDayTime()));
    }

    @Test
    public void parse_invalid_input_4() {
        assertThatThrownBy(() -> IntervalParser.apply("A-B C D:E:F"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Invalid interval format A-B C D:E:F");
    }

    @Test
    public void test_psql_format_from_string() {
        Period period = PGIntervalParser.apply("@ 1 year 1 mon 1 day 1 hour 1 minute 1 secs");
        assertThat(period).isEqualTo(new Period().withYears(1).withMonths(1).withDays(1).withHours(1).withMinutes(1).withSeconds(1));
    }

    @Test
    public void test_psql_verbose_format_from_string_with_ago() {
        Period period = IntervalParser.apply("@ 1 year 1 mon 1 day 1 hour 1 minute 1 secs ago");
        assertThat(period).isEqualTo(
            new Period().withYears(-1).withMonths(-1).withDays(-1).withHours(-1).withMinutes(-1).withSeconds(-1)
                .withPeriodType(PeriodType.yearMonthDayTime()));
    }

    @Test
    public void test_psql_verbose_format_from_string_with_negative_values() {
        Period period = IntervalParser.apply("@ 1 year -23 hours -3 mins -3.30 secs");
        assertThat(period).isEqualTo(
            new Period().withYears(1).withHours(-23).withMinutes(-3).withSeconds(-3).withMillis(-300)
                .withPeriodType(PeriodType.yearMonthDayTime()));
    }

    @Test
    public void test_psql_verbose_format_from_string_with_negative_values_and_ago() {
        Period period = IntervalParser.apply("@ 1 year -23 hours -3 mins -3.30 secs ago");
        assertThat(period).isEqualTo(
            new Period().withYears(-1).withHours(23).withMinutes(3).withSeconds(3).withMillis(300)
                .withPeriodType(PeriodType.yearMonthDayTime()));
    }

    @Test
    public void test_psql_compact_format_from_string() {
        Period period = IntervalParser.apply("6 years 5 mons 4 days 03:02:01");
        assertThat(period).isEqualTo(
            new Period().withYears(6).withMonths(5).withDays(4).withHours(3).withMinutes(2).withSeconds(1)
                .withPeriodType(PeriodType.yearMonthDayTime()));
    }

    @Test
    public void test_weeks() {
        Period period = IntervalParser.apply("1 week");
        assertThat(period).isEqualTo(new Period().withDays(7).withPeriodType(PeriodType.yearMonthDayTime()));
    }

    @Test
    public void test_characters() {
        assertThatThrownBy(() -> IntervalParser.apply("a week b mons c days"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Invalid interval format a week b mons c days");
    }

    @Test
    public void test_normalization() {
        Period period = IntervalParser.apply("1 year 1 month 763 days 1204 hours 642 mins 7123 secs");
        assertThat(period).isEqualTo(
            new Period(1, 1, 0, 813, 16, 40, 43, 0)
                .withPeriodType(PeriodType.yearMonthDayTime()));
    }
}
