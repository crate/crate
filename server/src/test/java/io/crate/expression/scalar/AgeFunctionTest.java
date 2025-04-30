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

package io.crate.expression.scalar;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.LocalDate;
import java.time.Month;
import java.time.ZoneOffset;

import org.joda.time.Period;
import org.joda.time.PeriodType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.crate.exceptions.UnsupportedFunctionException;
import io.crate.metadata.SystemClock;

public class AgeFunctionTest extends ScalarTestCase {

    private static final long FIRST_JAN_2021_MIDNIGHT_UTC_AS_MILLIS =
        LocalDate.of(2021, Month.JANUARY, 1).atStartOfDay().toInstant(ZoneOffset.UTC).toEpochMilli();

    @Before
    public void prepare() {
        SystemClock.setCurrentMillisFixedUTC(FIRST_JAN_2021_MIDNIGHT_UTC_AS_MILLIS);
    }

    @After
    public void cleanUp() {
        SystemClock.setCurrentMillisSystemUTC();
    }

    @Test
    public void test_calls_within_statement_are_idempotent() {
        assertEvaluate("age('2019-01-01' :: TIMESTAMP) = age('2019-01-01' :: TIMESTAMP)", true);
    }

    @Test
    public void test_1_argument_before_curdate_age_is_positive() {
        // 1 second before the current_date (manually set 2021.01.01 midnight)
        assertEvaluate("pg_catalog.age('2020-12-31T23:59:59'::timestamp)",
                       Period.seconds(1).normalizedStandard(PeriodType.yearMonthDayTime()));
    }

    @Test
    public void test_1_argument_after_curdate_age_is_negative() {
        // 1 second before the current_date (manually set 2021.01.01 midnight)
        assertEvaluate("pg_catalog.age('2021-01-01T00:00:01'::timestamp)",
                       Period.seconds(1).normalizedStandard(PeriodType.yearMonthDayTime()).negated());
    }

    @Test
    public void test_floating_point_arguments() {
        // We treat float and double values as seconds with milliseconds as fractions.
        assertEvaluate("pg_catalog.age(1.2, 1.1) ", Period.millis(100).normalizedStandard(PeriodType.yearMonthDayTime()));
        assertEvaluate("pg_catalog.age(1.1, 1.2) ", Period.millis(100).normalizedStandard(PeriodType.yearMonthDayTime()).negated());
    }

    @Test
    public void test_2_arguments_more_than_7_days_no_weeks() {
        // Postgres returns 8 days.
        // If we remove PeriodType.yearMonthDayTime() normalization in the implementation, age returns 1 week 1 day.
        var expectedInterval = Period.days(8).normalizedStandard(PeriodType.yearMonthDayTime());
        assertEvaluate("pg_catalog.age('2021-01-09T00:00:00'::timestamp, '2021-01-01T00:00:00'::timestamp)", expectedInterval);
    }

    @Test
    public void test_age_is_1_month_regardless_of_days_in_month() {
        var expectedInterval = Period.months(1).normalizedStandard(PeriodType.yearMonthDayTime());
        assertEvaluate("pg_catalog.age('2021-02-01T00:00:00'::timestamp, '2021-01-01T00:00:00'::timestamp)", expectedInterval); // 31 days but 1 month
        assertEvaluate("pg_catalog.age('2021-03-01T00:00:00'::timestamp, '2021-02-01T00:00:00'::timestamp)", expectedInterval); // 28 days but 1 month
        assertEvaluate("pg_catalog.age('2021-05-01T00:00:00'::timestamp, '2021-04-01T00:00:00'::timestamp)", expectedInterval); // 30 days but 1 month
    }

    @Test
    public void test_2_arguments_positive() {
        // 1 year 2 months 3 days 4 hours 5 minutes 6 seconds 7 milliseconds before the current_date (manually set 2021.01.01 midnight)
        var expectedInterval = Period.years(1).plusMonths(2).plusDays(3).plusHours(4).plusMinutes(5).plusSeconds(6)
            .plusMillis(7).normalizedStandard(PeriodType.yearMonthDayTime());
        assertEvaluate("pg_catalog.age('2021-01-01T00:00:00'::timestamp, '2019-10-28T19:54:53.993'::timestamp)", expectedInterval);
    }

    @Test
    public void test_2_arguments_negative() {
        // 1 year 2 months 3 days 4 hours 5 minutes 6 seconds 7 milliseconds after the current_date (manually set 2021.01.01 midnight)
        var expectedInterval = Period.years(1).plusMonths(2).plusDays(3).plusHours(4).plusMinutes(5).plusSeconds(6)
            .plusMillis(7).normalizedStandard(PeriodType.yearMonthDayTime()).negated();
        assertEvaluate("pg_catalog.age('2021-01-01T00:00:00'::timestamp, '2022-03-04T04:05:06.007'::timestamp)", expectedInterval);
    }

    @Test
    public void test_at_least_one_arg_is_null_returns_null() {
        assertEvaluateNull("age(null)");
        assertEvaluateNull("age(null, '2019-01-02'::TIMESTAMP)");
        assertEvaluateNull("age('2019-01-02'::TIMESTAMP, null)");
        assertEvaluateNull("age(null, null)");
    }

    @Test
    public void test_2_arguments_with_timezone() throws Exception {
        assertEvaluate("pg_catalog.age(CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)", Period.seconds(0).normalizedStandard(PeriodType.yearMonthDayTime()));
    }

    @Test
    public void test_3_args_throws_exception() {
        assertThatThrownBy(
            () -> assertEvaluate("pg_catalog.age(null, null, null)", null))
            .isExactlyInstanceOf(UnsupportedFunctionException.class)
            .hasMessage(
                "Invalid arguments in: pg_catalog.age(NULL, NULL, NULL) with (undefined, undefined, undefined). "
                + "Valid types: (timestamp without time zone), (timestamp without time zone, timestamp without time zone)");
    }
}
