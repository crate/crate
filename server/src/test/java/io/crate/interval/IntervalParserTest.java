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

import java.util.Locale;

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
            .hasMessage("Invalid interval format: 1 1");
    }

    @Test
    public void parse_negative_days_negative_seconds() {
        assertThatThrownBy(() -> IntervalParser.apply("-1 -1"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Invalid interval format: -1 -1");
    }

    @Test
    public void parse_invalid_input_0() {
        assertThatThrownBy(() -> IntervalParser.apply("10-1-1-1-1-1"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Invalid interval format: 10-1-1-1-1-1");
    }

    @Test
    public void parse_invalid_input_1() {
        assertThatThrownBy(() -> IntervalParser.apply("10:1:1:1:N1:1"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Invalid interval format: 10:1:1:1:N1:1");
    }

    @Test
    public void parse_invalid_input_2() {
        assertThatThrownBy(() -> IntervalParser.apply("1-2 3 4-5-6"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Invalid interval format: 1-2 3 4-5-6");
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
            .hasMessage("Invalid interval format: A-B C D:E:F");
    }

    @Test
    public void test_psql_format_from_string() {
        Period period = PGIntervalParser.apply("@ 1 year 1 mon 1 day 1 hour 1 minute 1 secs  ");
        assertThat(period).isEqualTo(new Period().withYears(1).withMonths(1).withDays(1).withHours(1).withMinutes(1).withSeconds(1));
    }

    @Test
    public void test_psql_verbose_format_from_string_with_ago() {
        Period period = IntervalParser.apply("  @ 1 year 1 mon 1 day 1 hour 1 minute 1 secs ago  ");
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
        Period period = IntervalParser.apply("@ 1 year -23 hours -3 mins -3.30 secs AGO");
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
    public void test_invalid_values() {
        assertThatThrownBy(() -> IntervalParser.apply("a week b mons c days"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Invalid interval format: a week b mons c days");
    }

    @Test
    public void test_invalid_types() {
        assertThatThrownBy(() -> IntervalParser.apply("1 week 2 monthss 3 days"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Invalid interval format: 1 week 2 monthss 3 days");
    }

    @Test
    public void test_invalid_duplicate_units() {
        assertThatThrownBy(() -> IntervalParser.apply("1 week 2 mons 3 days 4w"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Invalid interval format: 1 week 2 mons 3 days 4w");
        assertThatThrownBy(() -> IntervalParser.apply("1y 11:22:33 11:22:33"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Invalid interval format: 1y 11:22:33 11:22:33");
        assertThatThrownBy(() -> IntervalParser.apply("2sec 11:22:33"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Invalid interval format: 2sec 11:22:33");
        assertThatThrownBy(() -> IntervalParser.apply("1 years 2 mons 3 days 2 years"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Invalid interval format: 1 years 2 mons 3 days 2 years");
    }

    @Test
    public void test_normalization() {
        var expected = new Period(1, 2, 0, 827, 4, 40, 43, 0)
            .withPeriodType(PeriodType.yearMonthDayTime());


        var year = randomWhiteSpaces() + randomFrom("year", "years");
        var month = randomWhiteSpaces() + randomFrom("month", "months", "mon", "mons");
        var week = randomWhiteSpaces() + randomFrom("week", "weeks");
        var day = randomWhiteSpaces() + randomFrom("day", "days");
        var hour = randomWhiteSpaces() + randomFrom("hour", "hours");
        var minute = randomWhiteSpaces() + randomFrom("minute", "minutes", "min", "mins");
        var second = randomWhiteSpaces() + randomFrom("second", "seconds", "sec", "secs");

        year = randomBoolean() ? year : year.toUpperCase(Locale.ENGLISH);
        month = randomBoolean() ? month : month.toUpperCase(Locale.ENGLISH);
        week = randomBoolean() ? week : week.toUpperCase(Locale.ENGLISH);
        day = randomBoolean() ? day : day.toUpperCase(Locale.ENGLISH);
        hour = randomBoolean() ? hour : hour.toUpperCase(Locale.ENGLISH);
        minute = randomBoolean() ? minute : minute.toUpperCase(Locale.ENGLISH);
        second = randomBoolean() ? second : second.toUpperCase(Locale.ENGLISH);

        assertThat(IntervalParser.apply("1" + year + " 2" + month + " 3" + week + " 763" + day + " 1024" + hour +
                                        " 642" + minute + " 7123" + second)).isEqualTo(expected);
    }

    private static String randomWhiteSpaces() {
        var numWhitespaces = randomInt(5);
        StringBuilder whiteSpaces = new StringBuilder();
        for (int i = 0; i < numWhitespaces; i++) {
            whiteSpaces.append(randomFrom(" ", "\t"));
        }
        return whiteSpaces.toString();
    }
}
