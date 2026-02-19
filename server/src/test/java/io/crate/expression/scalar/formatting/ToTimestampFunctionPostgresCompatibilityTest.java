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

package io.crate.expression.scalar.formatting;

import org.junit.Test;

import io.crate.expression.scalar.ScalarTestCase;

/**
 * PostgreSQL compatibility tests for to_timestamp function.
 * These tests verify that to_timestamp behaves consistently with PostgreSQL.
 *
 * Note: Missing date components default to year=1, month=1, day=1.
 * Year 1 (0001-01-01 00:00:00 UTC) = -62135596800000 ms from epoch.
 */
public class ToTimestampFunctionPostgresCompatibilityTest extends ScalarTestCase {

    // Base timestamp for year 1, month 1, day 1 = -62135596800000 ms
    private static final long YEAR_1_EPOCH = -62135596800000L;

    @Test
    public void testPostgresHourOfDayCompatibility() {
        // HH and HH12 are 12-hour format, HH24 is 24-hour format
        // Results are relative to year 1, Jan 1
        assertEvaluate("to_timestamp('17', 'HH24')", YEAR_1_EPOCH + 61200000L);
        assertEvaluate("to_timestamp('05', 'HH')", YEAR_1_EPOCH + 18000000L);
        assertEvaluate("to_timestamp('05', 'HH12')", YEAR_1_EPOCH + 18000000L);
        assertEvaluate("to_timestamp('05 PM', 'HH12 PM')", YEAR_1_EPOCH + 61200000L);
    }

    @Test
    public void testPostgresMinuteCompatibility() {
        assertEvaluate("to_timestamp('05', 'MI')", YEAR_1_EPOCH + 300000L);
        assertEvaluate("to_timestamp('30', 'MI')", YEAR_1_EPOCH + 1800000L);
    }

    @Test
    public void testPostgresSecondCompatibility() {
        assertEvaluate("to_timestamp('05', 'SS')", YEAR_1_EPOCH + 5000L);
        assertEvaluate("to_timestamp('45', 'SS')", YEAR_1_EPOCH + 45000L);
    }

    @Test
    public void testPostgresMillisecondCompatibility() {
        assertEvaluate("to_timestamp('123', 'MS')", YEAR_1_EPOCH + 123L);
        assertEvaluate("to_timestamp('003', 'MS')", YEAR_1_EPOCH + 3L);
        assertEvaluate("to_timestamp('300', 'MS')", YEAR_1_EPOCH + 300L);
    }

    @Test
    public void testPostgresMicrosecondCompatibility() {
        // Microseconds are truncated to milliseconds in the result
        assertEvaluate("to_timestamp('123456', 'US')", YEAR_1_EPOCH + 123L);
        assertEvaluate("to_timestamp('000500', 'US')", YEAR_1_EPOCH + 0L);
    }

    @Test
    public void testPostgresFractionOfSecondCompatibility() {
        assertEvaluate("to_timestamp('1', 'FF1')", YEAR_1_EPOCH + 100L);
        assertEvaluate("to_timestamp('12', 'FF2')", YEAR_1_EPOCH + 120L);
        assertEvaluate("to_timestamp('123', 'FF3')", YEAR_1_EPOCH + 123L);
        assertEvaluate("to_timestamp('1234', 'FF4')", YEAR_1_EPOCH + 123L);
        assertEvaluate("to_timestamp('12345', 'FF5')", YEAR_1_EPOCH + 123L);
        assertEvaluate("to_timestamp('123456', 'FF6')", YEAR_1_EPOCH + 123L);
    }

    @Test
    public void testPostgresSecondsPastMidnightCompatibility() {
        // 17:31:12 = 17*3600 + 31*60 + 12 = 63072 seconds past midnight
        assertEvaluate("to_timestamp('63072', 'SSSS')", YEAR_1_EPOCH + 63072000L);
        assertEvaluate("to_timestamp('63072', 'SSSSS')", YEAR_1_EPOCH + 63072000L);
    }

    @Test
    public void testPostgresMeridiemIndicatorCompatibility() {
        // AM/PM parsing
        assertEvaluate("to_timestamp('05 PM', 'HH12 PM')", YEAR_1_EPOCH + 61200000L);
        assertEvaluate("to_timestamp('05 AM', 'HH12 AM')", YEAR_1_EPOCH + 18000000L);
        assertEvaluate("to_timestamp('05 pm', 'HH12 pm')", YEAR_1_EPOCH + 61200000L);
        assertEvaluate("to_timestamp('05 am', 'HH12 am')", YEAR_1_EPOCH + 18000000L);
        assertEvaluate("to_timestamp('05 P.M.', 'HH12 P.M.')", YEAR_1_EPOCH + 61200000L);
        assertEvaluate("to_timestamp('05 A.M.', 'HH12 A.M.')", YEAR_1_EPOCH + 18000000L);
    }

    @Test
    public void testPostgresYearCompatibility() {
        // 1970-01-01 00:00:00 UTC = 0 ms
        assertEvaluate("to_timestamp('1970', 'YYYY')", 0L);
        assertEvaluate("to_timestamp('1,970', 'Y,YYY')", 0L);
        // YYY, YY, Y parse partial years
        assertEvaluate("to_timestamp('970', 'YYY')", -31556908800000L);  // year 970
        assertEvaluate("to_timestamp('70', 'YY')", 0L);  // 70 -> 1970 (nearest to 2020 rule)
        assertEvaluate("to_timestamp('0', 'Y')", -62167219200000L);  // year 0
    }

    @Test
    public void testPostgresTwoDigitYearCompatibility() {
        // PostgreSQL uses "nearest to 2020" rule for two-digit years
        // 00-69 -> 2000-2069, 70-99 -> 1970-1999
        assertEvaluate("to_timestamp('00', 'YY')", 946684800000L);  // 2000-01-01
        assertEvaluate("to_timestamp('23', 'YY')", 1672531200000L);  // 2023-01-01
        assertEvaluate("to_timestamp('69', 'YY')", 3124224000000L);  // 2069-01-01
        assertEvaluate("to_timestamp('70', 'YY')", 0L);  // 1970-01-01
        assertEvaluate("to_timestamp('99', 'YY')", 915148800000L);  // 1999-01-01
    }

    @Test
    public void testPostgresISOYearCompatibility() {
        assertEvaluate("to_timestamp('1970', 'IYYY')", 0L);
        assertEvaluate("to_timestamp('2023', 'IYYY')", 1672531200000L);
    }

    @Test
    public void testPostgresMonthNameCompatibility() {
        // Full month names - case insensitive
        // January of year 1 = YEAR_1_EPOCH
        assertEvaluate("to_timestamp('JANUARY', 'MONTH')", YEAR_1_EPOCH);
        assertEvaluate("to_timestamp('January', 'Month')", YEAR_1_EPOCH);
        assertEvaluate("to_timestamp('january', 'month')", YEAR_1_EPOCH);
        // December of year 1
        assertEvaluate("to_timestamp('DECEMBER', 'MONTH')", YEAR_1_EPOCH + 28857600000L);
    }

    @Test
    public void testPostgresAbbreviatedMonthNameCompatibility() {
        assertEvaluate("to_timestamp('JAN', 'MON')", YEAR_1_EPOCH);
        assertEvaluate("to_timestamp('Jan', 'Mon')", YEAR_1_EPOCH);
        assertEvaluate("to_timestamp('jan', 'mon')", YEAR_1_EPOCH);
        assertEvaluate("to_timestamp('DEC', 'MON')", YEAR_1_EPOCH + 28857600000L);
    }

    @Test
    public void testPostgresMonthNumberCompatibility() {
        assertEvaluate("to_timestamp('01', 'MM')", YEAR_1_EPOCH);
        assertEvaluate("to_timestamp('11', 'MM')", YEAR_1_EPOCH + 26265600000L);
        assertEvaluate("to_timestamp('12', 'MM')", YEAR_1_EPOCH + 28857600000L);
    }

    @Test
    public void testPostgresDayNameCompatibility() {
        // Day names are parsed but don't affect the result (informational only)
        assertEvaluate("to_timestamp('1970-01-01 THURSDAY', 'YYYY-MM-DD DAY')", 0L);
        assertEvaluate("to_timestamp('1970-01-01 Thursday', 'YYYY-MM-DD Day')", 0L);
        assertEvaluate("to_timestamp('1970-01-01 thursday', 'YYYY-MM-DD day')", 0L);
    }

    @Test
    public void testPostgresAbbreviatedDayNameCompatibility() {
        assertEvaluate("to_timestamp('1970-01-01 THU', 'YYYY-MM-DD DY')", 0L);
        assertEvaluate("to_timestamp('1970-01-01 Thu', 'YYYY-MM-DD Dy')", 0L);
        assertEvaluate("to_timestamp('1970-01-01 thu', 'YYYY-MM-DD dy')", 0L);
    }

    @Test
    public void testPostgresDayOfYearCompatibility() {
        // Day 1 of 1970 = 1970-01-01
        assertEvaluate("to_timestamp('1970 001', 'YYYY DDD')", 0L);
        // Day 213 of 1970 = 1970-08-01
        assertEvaluate("to_timestamp('1970 213', 'YYYY DDD')", 18316800000L);
        // Day 365 of 1970 = 1970-12-31
        assertEvaluate("to_timestamp('1970 365', 'YYYY DDD')", 31449600000L);
    }

    @Test
    public void testPostgresDayOfMonthCompatibility() {
        // Day of month with defaults for year 1, month 1
        assertEvaluate("to_timestamp('01', 'DD')", YEAR_1_EPOCH);
        assertEvaluate("to_timestamp('17', 'DD')", YEAR_1_EPOCH + 1382400000L);
        assertEvaluate("to_timestamp('31', 'DD')", YEAR_1_EPOCH + 2592000000L);
    }

    @Test
    public void testPostgresDayOfWeekCompatibility() {
        // D and ID are parsed but don't affect result
        assertEvaluate("to_timestamp('1970-01-01 5', 'YYYY-MM-DD D')", 0L);
        assertEvaluate("to_timestamp('1970-01-01 4', 'YYYY-MM-DD ID')", 0L);
    }

    @Test
    public void testPostgresWeekOfMonthCompatibility() {
        // W is parsed but doesn't affect result
        assertEvaluate("to_timestamp('1970-01-15 3', 'YYYY-MM-DD W')", 1209600000L);
    }

    @Test
    public void testPostgresWeekOfYearCompatibility() {
        // WW and IW are parsed but don't affect result
        assertEvaluate("to_timestamp('1970 01', 'YYYY WW')", 0L);
        assertEvaluate("to_timestamp('1970 01', 'YYYY IW')", 0L);
    }

    @Test
    public void testPostgresCenturyCompatibility() {
        // CC is parsed but doesn't set year (defaults to year 1)
        assertEvaluate("to_timestamp('20', 'CC')", YEAR_1_EPOCH);
        assertEvaluate("to_timestamp('21', 'CC')", YEAR_1_EPOCH);
    }

    @Test
    public void testPostgresQuarterCompatibility() {
        // Q is parsed but doesn't affect result
        assertEvaluate("to_timestamp('1970 1', 'YYYY Q')", 0L);
        assertEvaluate("to_timestamp('1970 2', 'YYYY Q')", 0L);
    }

    @Test
    public void testPostgresEraIndicatorCompatibility() {
        // BC/AD are parsed but don't affect result in current implementation
        assertEvaluate("to_timestamp('1970-01-01 AD', 'YYYY-MM-DD AD')", 0L);
        assertEvaluate("to_timestamp('1970-01-01 ad', 'YYYY-MM-DD ad')", 0L);
        assertEvaluate("to_timestamp('1970-01-01 BC', 'YYYY-MM-DD BC')", 0L);
        assertEvaluate("to_timestamp('1970-01-01 A.D.', 'YYYY-MM-DD A.D.')", 0L);
        assertEvaluate("to_timestamp('1970-01-01 B.C.', 'YYYY-MM-DD B.C.')", 0L);
    }

    @Test
    public void testPostgresFlexibleSeparatorCompatibility() {
        // PostgreSQL allows any separator to match any separator
        assertEvaluate("to_timestamp('1970-01-01', 'YYYY-MM-DD')", 0L);
        assertEvaluate("to_timestamp('1970/01/01', 'YYYY-MM-DD')", 0L);
        assertEvaluate("to_timestamp('1970.01.01', 'YYYY-MM-DD')", 0L);
        assertEvaluate("to_timestamp('1970 01 01', 'YYYY-MM-DD')", 0L);
    }

    @Test
    public void testPostgresLeadingWhitespaceCompatibility() {
        // PostgreSQL skips leading whitespace
        assertEvaluate("to_timestamp('  1970-01-01', 'YYYY-MM-DD')", 0L);
        assertEvaluate("to_timestamp('   1970-01-01', 'YYYY-MM-DD')", 0L);
    }

    @Test
    public void testPostgresFullDateTimeCompatibility() {
        // Full date/time parsing
        // 1970-01-01 17:31:12 UTC = 63072000 ms
        assertEvaluate("to_timestamp('1970-01-01 17:31:12', 'YYYY-MM-DD HH24:MI:SS')", 63072000L);
        // With milliseconds: 1970-01-01 17:31:12.123 UTC = 63072123 ms
        assertEvaluate("to_timestamp('1970-01-01 17:31:12.123', 'YYYY-MM-DD HH24:MI:SS.MS')", 63072123L);
    }

    @Test
    public void testPostgresRoundTripCompatibility() {
        // Round-trip: to_timestamp(to_char(ts, fmt), fmt) should return original timestamp
        assertEvaluate(
            "to_timestamp(to_char(timestamp '1970-01-01 17:31:12', 'YYYY-MM-DD HH24:MI:SS'), 'YYYY-MM-DD HH24:MI:SS')",
            63072000L
        );
        assertEvaluate(
            "to_timestamp(to_char(timestamp '2023-07-15 14:30:45', 'YYYY-MM-DD HH24:MI:SS'), 'YYYY-MM-DD HH24:MI:SS')",
            1689431445000L
        );
    }

    @Test
    public void testPostgresNullHandlingCompatibility() {
        // PostgreSQL returns null for null input
        assertEvaluateNull("to_timestamp(null, 'YYYY-MM-DD')");
        assertEvaluateNull("to_timestamp('1970-01-01', null)");
        assertEvaluateNull("to_timestamp(null, null)");
    }

    @Test
    public void testPostgresCaseInsensitivePatternCompatibility() {
        // Pattern keywords are case-insensitive
        assertEvaluate("to_timestamp('1970-01-01', 'yyyy-mm-dd')", 0L);
        assertEvaluate("to_timestamp('1970-01-01', 'YYYY-MM-DD')", 0L);
        assertEvaluate("to_timestamp('1970-01-01 17:31:12', 'yyyy-mm-dd hh24:mi:ss')", 63072000L);
        assertEvaluate("to_timestamp('1970-01-01 17:31:12', 'YYYY-MM-DD HH24:MI:SS')", 63072000L);
    }

    @Test
    public void testPostgres12HourClockEdgeCasesCompatibility() {
        // 12 AM = midnight (00:00)
        assertEvaluate("to_timestamp('12:00:00 AM', 'HH12:MI:SS AM')", YEAR_1_EPOCH);
        // 12 PM = noon (12:00)
        assertEvaluate("to_timestamp('12:00:00 PM', 'HH12:MI:SS PM')", YEAR_1_EPOCH + 43200000L);
        // 12:30 AM = 00:30
        assertEvaluate("to_timestamp('12:30:00 AM', 'HH12:MI:SS AM')", YEAR_1_EPOCH + 1800000L);
        // 12:30 PM = 12:30
        assertEvaluate("to_timestamp('12:30:00 PM', 'HH12:MI:SS PM')", YEAR_1_EPOCH + 45000000L);
    }

    @Test
    public void testPostgresDefaultValuesCompatibility() {
        // Missing components default to: year=1, month=1, day=1, time=00:00:00
        assertEvaluate("to_timestamp('2023', 'YYYY')", 1672531200000L);
        // July of year 1
        assertEvaluate("to_timestamp('07', 'MM')", YEAR_1_EPOCH + 15638400000L);
        // Jan 15 of year 1
        assertEvaluate("to_timestamp('15', 'DD')", YEAR_1_EPOCH + 1209600000L);
        // 14:30:45 of year 1, Jan 1
        assertEvaluate("to_timestamp('14:30:45', 'HH24:MI:SS')", YEAR_1_EPOCH + 52245000L);
    }
}
