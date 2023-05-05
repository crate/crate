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

import static io.crate.testing.Asserts.assertThrowsMatches;
import static io.crate.testing.Asserts.isNotSameInstance;
import static io.crate.testing.Asserts.isSameInstance;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Month;
import java.time.ZoneOffset;

import org.junit.Test;

public class DateBinFunctionTest extends ScalarTestCase {

    private static final LocalDateTime FIRST_JAN_2001_MIDNIGHT_UTC_AS_DATE = LocalDate.of(2001, Month.JANUARY, 1).atStartOfDay();
    private static final LocalDateTime FIRST_JAN_1969_MIDNIGHT_UTC_AS_DATE = LocalDate.of(1969, Month.JANUARY, 1).atStartOfDay();

    @Test
    public void test_interval_is_value_compile_gets_new_instance() {
        assertCompile("date_bin('1 day' :: INTERVAL, timestamp, timestamp)", isNotSameInstance());
    }

    @Test
    public void compile_on_null_interval_gets_same_instance() {
        assertCompile("date_bin(null, timestamp, timestamp)", isSameInstance());
    }

    @Test
    public void test_at_least_one_arg_is_null_returns_null() {
        assertEvaluateNull("date_bin(null, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)");
        assertEvaluateNull("date_bin('1 day' :: INTERVAL , null, CURRENT_TIMESTAMP)");
        assertEvaluateNull("date_bin('1 day' :: INTERVAL , CURRENT_TIMESTAMP, null)");
    }

    @Test
    public void test_interval_with_years_or_months_exception_thrown() {
        assertThatThrownBy(() -> assertEvaluate("date_bin('2 mons' :: INTERVAL, CURRENT_TIMESTAMP, 0)", null))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Cannot use intervals containing months or years");
        assertThatThrownBy(() -> assertEvaluate("date_bin('2 years' :: INTERVAL, CURRENT_TIMESTAMP, 0)", null))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Cannot use intervals containing months or years");
    }

    @Test
    public void test_interval_is_zero_exception_thrown() {
        assertThrowsMatches(() -> assertEvaluate("date_bin('0 days' :: INTERVAL, CURRENT_TIMESTAMP, 0)", null),
                            IllegalArgumentException.class,
                            "Interval cannot be zero");
    }

    @Test
    public void test_same_value_as_date_trunc() {
        assertEvaluate("date_bin('1 day' :: INTERVAL, CURRENT_TIMESTAMP, 0) = DATE_TRUNC('day', CURRENT_TIMESTAMP)", true);
        assertEvaluate("date_bin('1 week' :: INTERVAL, CURRENT_TIMESTAMP, '2001-01-01T00:00:00Z'::timestamp without time zone) " +
            " = DATE_TRUNC('week', CURRENT_TIMESTAMP)", true);
    }

    @Test
    public void test_called_twice_same_result() {
        assertEvaluate("date_bin('1 day' :: INTERVAL, CURRENT_TIMESTAMP, 0) = date_bin('1 day' :: INTERVAL, CURRENT_TIMESTAMP, 0)",
            true);
    }

    @Test
    public void test_diff_magnitude_smaller_than_interval_magnitude_returns_beginning_of_the_bin() {
        // This test checks case when abs(origin-ts) < abs(interval).

        // origin < ts, beginning of the bin is origin for any sign of interval
        long expected = FIRST_JAN_2001_MIDNIGHT_UTC_AS_DATE.toEpochSecond(ZoneOffset.UTC) * 1000;
        assertEvaluate("date_bin('8 days'::interval, '2001-01-04 00:00:00' :: timestamp without time zone, " +
            " '2001-01-01 00:00:00' :: timestamp without time zone)", expected);
        assertEvaluate("date_bin('-8 days'::interval, '2001-01-04 00:00:00' :: timestamp without time zone, " +
            " '2001-01-01 00:00:00' :: timestamp without time zone)", expected);

        // ts < origin, beginning of the bin is origin - abs(interval)
        long expected1 = LocalDate.of(2001, Month.JANUARY, 4).atStartOfDay().minusDays(8).toEpochSecond(ZoneOffset.UTC) * 1000;
        assertEvaluate("date_bin('8 days'::interval, '2001-01-01 00:00:00' :: timestamp without time zone," +
            " '2001-01-04 00:00:00' :: timestamp without time zone)", expected1);
        assertEvaluate("date_bin('-8 days'::interval, '2001-01-01 00:00:00' :: timestamp without time zone," +
            " '2001-01-04 00:00:00' :: timestamp without time zone)", expected1);
    }

    @Test
    public void test_interval_any_sign_source_equal_to_origin_returns_origin() {
        long expected = FIRST_JAN_2001_MIDNIGHT_UTC_AS_DATE.toEpochSecond(ZoneOffset.UTC) * 1000;
        assertEvaluate("date_bin('7 weeks' :: INTERVAL, '2001-01-01 00:00:00' :: timestamp without time zone," +
            " '2001-01-01 00:00:00' :: timestamp without time zone)", expected);
        assertEvaluate("date_bin('-7 weeks' :: INTERVAL, '2001-01-01 00:00:00' :: timestamp without time zone ," +
            " '2001-01-01 00:00:00' :: timestamp without time zone)", expected);
    }

    @Test
    public void test_interval_any_sign_timestamp_bigint() {
        assertEvaluate("date_bin('3 days' :: INTERVAL, 86400000*4, 0)", 86400000 * 3L);
        assertEvaluate("date_bin('-3 days' :: INTERVAL, 86400000*4, 0)", 86400000 * 3L);
    }

    @Test
    public void test_interval_any_sign_timestamp_without_zone() {
        // Timeline is split by 2 min intervals.
        // Timestamp is midnight + 7 min and interval begin is midnight + 6 min
        long expected = FIRST_JAN_1969_MIDNIGHT_UTC_AS_DATE.plusMinutes(6).toEpochSecond(ZoneOffset.UTC) * 1000;
        assertEvaluate("date_bin('2 minutes' :: INTERVAL, '1969-01-01T00:07:00Z'::timestamp without time zone, 0)", expected);
        assertEvaluate("date_bin('-2 minutes' :: INTERVAL, '1969-01-01T00:07:00Z'::timestamp without time zone, 0)", expected);
    }

    @Test
    public void test_interval_any_sign_timestamp_with_zone() {
        // Timeline is split by 4 hours intervals.
        // Timestamp is 9 AM at UTC+2 and interval begin is 8 AM in UTC+2.
        long expected = FIRST_JAN_1969_MIDNIGHT_UTC_AS_DATE
            .plusHours(8) // 4k interval begin is 8AM
            .toEpochSecond(ZoneOffset.ofHours(2)) * 1000;
        assertEvaluate("date_bin('4 hours' :: INTERVAL, '1969-01-01T09:00:00+0200'::timestamp with time zone , TIMEZONE('+02:00', 0))", expected);
        assertEvaluate("date_bin('-4 hours' :: INTERVAL, '1969-01-01T09:00:00+0200'::timestamp with time zone , TIMEZONE('+02:00', 0))", expected);
    }
}
