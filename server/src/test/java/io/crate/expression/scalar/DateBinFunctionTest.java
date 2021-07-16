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

import io.crate.expression.symbol.Literal;
import io.crate.types.DataTypes;
import org.hamcrest.core.IsSame;
import org.junit.Test;

import java.time.*;

import static io.crate.testing.Asserts.assertThrowsMatches;
import static org.hamcrest.core.IsNot.not;

public class DateBinFunctionTest extends ScalarTestCase {

    private static final long FIRST_JAN_2020_MIDNIGHT_UTC = LocalDate.of(2020, Month.JANUARY, 1)
                                                            .atStartOfDay()
                                                            .toEpochSecond(ZoneOffset.UTC) * 1000L;

    // For testing negative timestamps
    private static final LocalDateTime FIRST_JAN_1969_MIDNIGHT_UTC_AS_DATE =  LocalDate.of(1969, Month.JANUARY, 1).atStartOfDay();

    @Test
    public void test_interval_is_value_compile_gets_new_instance() {
        assertCompile("date_bin('1 day' :: INTERVAL, timestamp)", (s) -> not(IsSame.sameInstance(s)));
    }

    @Test
    public void test_interval_is_ref_compile_gets_same_instance() {
        assertCompile("date_bin(x, timestamp)", IsSame::sameInstance);
    }

    @Test
    public void test_interval_is_null_returns_null() {
        assertEvaluate("date_bin(null, 1401777485000)", null);
    }

    @Test
    public void test_timestamp_is_null_returns_null() {
        assertEvaluate("date_bin('1 day' :: INTERVAL , null)", null);
    }

    @Test
    public void test_interval_and_timestamp_are_null_returns_null() {
        assertEvaluate("date_bin(null, null)", null);
    }

    @Test
    public void test_interval_is_zero_exception_thrown() {
        assertThrowsMatches(() ->  assertEvaluate("date_bin(0, 1401777485000)", null),
            IllegalArgumentException.class,
            "Interval cannot be negative or equal to zero");
    }

    @Test
    public void test_interval_is_negative_exception_thrown() {
        assertThrowsMatches(() ->  assertEvaluate("date_bin(-1, 1401777485000)", null),
            IllegalArgumentException.class,
            "Interval cannot be negative or equal to zero");
    }

    @Test
    public void test_bigint_same_value_as_date_trunc() {
        assertEvaluate("date_bin('1 day' :: INTERVAL, CURRENT_TIMESTAMP) = DATE_TRUNC('day', CURRENT_TIMESTAMP)", true);
        assertEvaluate("date_bin('1 week' :: INTERVAL, CURRENT_TIMESTAMP) = DATE_TRUNC('week', CURRENT_TIMESTAMP)", true);
    }

    @Test
    public void test_called_twice_same_result() {
        assertEvaluate("date_bin('1 day' :: INTERVAL, CURRENT_TIMESTAMP) = date_bin('1 day' :: INTERVAL, CURRENT_TIMESTAMP)",
            true);
    }

    @Test
    public void test_interval_greater_than_positive_timestamp_returns_zero() {
        assertEvaluate("date_bin(10, 2)", 0L);
    }

    @Test
    public void test_interval_greater_than_negative_timestamp_returns_zero() {
        assertEvaluate("date_bin(10, -2)", -10L);
    }

    @Test
    public void test_negative_timestamp() {
        // -104, 100, -96,...,        -8,            0,            8, ...,        96, 100, 104
        // |.............|.............|.............|.............|.............|.............|
        // in [-104, -96], -104 is earlier timestamp, farther from 1970 (0) than -96.
        assertEvaluate("date_bin(8, -100)", -104L);
    }

    @Test
    public void test_bigint_bigint_works() {
        assertEvaluate("date_bin(999, 1000)", 999L);
    }

    @Test
    public void test_bigint_timestamp_without_zone_works() {
        // Timeline is split by 1 min intervals.
        // Timestamp is midnight + 30 seconds  (middle of the interval) and interval begin is midnight
        assertEvaluate("date_bin(60000, timestamp)",
            FIRST_JAN_2020_MIDNIGHT_UTC,
            Literal.of(DataTypes.TIMESTAMP, FIRST_JAN_2020_MIDNIGHT_UTC + 30000L)
        );
    }

    @Test
    public void test_bigint_timestamp_with_zone_works() {
        // Timeline is split by 1 min intervals.
        // Timestamp is midnight + 30 seconds (middle of the interval) and interval begin is midnight in UTC+1.
        // We need to subtract from UTC hours difference as same epoch in UTC+1 stands for later date.
        assertEvaluate("date_bin(60000, '2020-01-01T00:00:30+0100' :: timestamp with time zone)",
            FIRST_JAN_2020_MIDNIGHT_UTC - 60 * 60 * 1000);
    }

    @Test
    public void test_interval_timestamp_bigint_works() {
        assertEvaluate("date_bin('3 days' :: INTERVAL, 86400000*4)", 86400000 * 3L);
    }

    @Test
    public void test_interval_timestamp_without_zone_works() {
        // Timeline is split by 2 min intervals.
        // Timestamp is midnight + 7 min and interval begin is midnight + 6 min

        long expected = FIRST_JAN_1969_MIDNIGHT_UTC_AS_DATE.plusMinutes(6).toEpochSecond(ZoneOffset.UTC) * 1000;
        assertEvaluate("date_bin('2 minutes' :: INTERVAL, '1969-01-01T00:07:00Z'::timestamp without time zone)", expected);
    }

    @Test
    public void test_interval_timestamp_without_zone_works2() {
        // Timeline is split by 2 min intervals.
        // Timestamp is midnight + 7 min and interval begin is midnight + 6 min

        long expected = FIRST_JAN_1969_MIDNIGHT_UTC_AS_DATE.plusMinutes(6).toEpochSecond(ZoneOffset.UTC) * 1000;
        assertEvaluate("date_bin('1 week' :: INTERVAL, '1969-01-09T00:00:00Z'::timestamp without time zone)", expected);
    }

    @Test
    public void test_interval_timestamp_with_zone_works() {
        // Timeline is split by 4 hours intervals.
        // Timestamp is 9 AM at UTC+2 and interval begin is 8 AM in UTC+2.
        // We need to subtract from UTC hours difference as same epoch in UTC+2 stands for later date.
        long expected = FIRST_JAN_1969_MIDNIGHT_UTC_AS_DATE
            .plusHours(8) // 4k interval begin is 8AM
            .minusHours(2) // time zone difference, we need to subtract from UTC hours difference as same epoch in UTC+2 stands for later date.
            .toEpochSecond(ZoneOffset.UTC) * 1000;
        assertEvaluate("date_bin('4 hours' :: INTERVAL, '1969-01-01T09:00:00+0200' :: timestamp with time zone)", expected);
    }





}
