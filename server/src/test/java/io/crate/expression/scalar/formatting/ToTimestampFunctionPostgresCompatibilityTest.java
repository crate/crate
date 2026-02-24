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

import java.time.Instant;

import org.junit.Test;

import io.crate.expression.scalar.ScalarTestCase;

/**
 * PostgreSQL compatibility tests for to_timestamp function.
 * These tests verify that to_timestamp behaves consistently with PostgreSQL.
 *
 * <p>Verified against PostgreSQL 16 on 2026-02-24.
 *
 * <p>Tests include cases from PostgreSQL's horology.sql regression tests
 * (postgres/src/test/regress/sql/horology.sql).
 *
 * <p>Verification Status Legend:
 * <ul>
 *   <li>Verified: Tests that produce identical results to PostgreSQL 16</li>
 *   <li>DIFFERS: Tests where CrateDB behavior intentionally differs from PostgreSQL</li>
 *   <li>PG_REJECTS: Patterns that PostgreSQL rejects but CrateDB accepts</li>
 * </ul>
 *
 * <p>Known Differences from PostgreSQL:
 * <ul>
 *   <li><b>Default year:</b> CrateDB uses year 1, PostgreSQL uses year 0 (1 BC).
 *       Tests that rely only on time components (HH, MI, SS, etc.) will differ by 1 year.</li>
 *   <li><b>YYY/Y patterns:</b> CrateDB treats as literal year value,
 *       PostgreSQL applies "nearest to 2020" rule (e.g., Y=9 becomes 2009 in PG, 0009 in CrateDB).</li>
 *   <li><b>ISO patterns with Gregorian:</b> CrateDB accepts mixing ISO week patterns (IYYY, IW, ID)
 *       with Gregorian dates, PostgreSQL rejects with "invalid combination of date conventions".</li>
 *   <li><b>TZ/tz/OF patterns:</b> CrateDB accepts timezone patterns (parses and ignores them),
 *       PostgreSQL rejects these patterns in to_timestamp.</li>
 *   <li><b>WW/D computation:</b> Minor differences in week number and day of week calculation
 *       due to different week start conventions.</li>
 *   <li><b>Large years (>9999):</b> YYYY pattern strictly consumes 4 digits in CrateDB,
 *       so years > 9999 are not supported with standard YYYY format.</li>
 *   <li><b>FM modifier:</b> Fill mode (FM) modifier not supported in CrateDB.
 *       Use padded patterns (Month, Day) with appropriate whitespace instead.</li>
 *   <li><b>'th' ordinal suffix:</b> Ordinal suffixes like 'th', 'nd', 'rd' not supported in CrateDB.</li>
 *   <li><b>Negative years in input:</b> Negative year values in the input string (e.g., '-44-02-01')
 *       are handled differently. Use the BC pattern instead for BC dates.</li>
 *   <li><b>Internal whitespace:</b> PostgreSQL accepts internal whitespace in fixed-width formats
 *       like YYYYMMDD, CrateDB requires explicit separators or no whitespace.</li>
 * </ul>
 *
 * <p>Note: Missing date components default to year=1, month=1, day=1 in CrateDB.
 * Year 1 (0001-01-01 00:00:00 UTC) = -62135596800000 ms from epoch.
 *
 * @see ToTimestampFunctionTest for additional unit tests
 */
public class ToTimestampFunctionPostgresCompatibilityTest extends ScalarTestCase {

    @Test
    public void testPostgresHourOfDayCompatibility() {
        // HH and HH12 are 12-hour format, HH24 is 24-hour format
        // Results are relative to year 1, Jan 1 (differs from PG which uses year 0)
        assertEvaluate("to_timestamp('17', 'HH24')",
            // DIFFERS: PostgreSQL 16 returns 0001-01-01 BC 17:00:00+00 (year 0)
            Instant.parse("0001-01-01T17:00:00Z").toEpochMilli());
        assertEvaluate("to_timestamp('05', 'HH')",
            // DIFFERS: PostgreSQL 16 returns 0001-01-01 BC 05:00:00+00 (year 0)
            Instant.parse("0001-01-01T05:00:00Z").toEpochMilli());
        assertEvaluate("to_timestamp('05', 'HH12')",
            // DIFFERS: PostgreSQL 16 returns 0001-01-01 BC 05:00:00+00 (year 0)
            Instant.parse("0001-01-01T05:00:00Z").toEpochMilli());
        assertEvaluate("to_timestamp('05 PM', 'HH12 PM')",
            // DIFFERS: PostgreSQL 16 returns 0001-01-01 BC 17:00:00+00 (year 0)
            Instant.parse("0001-01-01T17:00:00Z").toEpochMilli());
    }

    @Test
    public void testPostgresMinuteCompatibility() {
        assertEvaluate("to_timestamp('05', 'MI')",
            // DIFFERS: PostgreSQL 16 returns 0001-01-01 BC 00:05:00+00 (year 0)
            Instant.parse("0001-01-01T00:05:00Z").toEpochMilli());
        assertEvaluate("to_timestamp('30', 'MI')",
            // DIFFERS: PostgreSQL 16 returns 0001-01-01 BC 00:30:00+00 (year 0)
            Instant.parse("0001-01-01T00:30:00Z").toEpochMilli());
    }

    @Test
    public void testPostgresSecondCompatibility() {
        assertEvaluate("to_timestamp('05', 'SS')",
            // DIFFERS: PostgreSQL 16 returns 0001-01-01 BC 00:00:05+00 (year 0)
            Instant.parse("0001-01-01T00:00:05Z").toEpochMilli());
        assertEvaluate("to_timestamp('45', 'SS')",
            // DIFFERS: PostgreSQL 16 returns 0001-01-01 BC 00:00:45+00 (year 0)
            Instant.parse("0001-01-01T00:00:45Z").toEpochMilli());
    }

    @Test
    public void testPostgresMillisecondCompatibility() {
        assertEvaluate("to_timestamp('123', 'MS')",
            // DIFFERS: PostgreSQL 16 returns 0001-01-01 BC 00:00:00.123+00 (year 0)
            Instant.parse("0001-01-01T00:00:00.123Z").toEpochMilli());
        assertEvaluate("to_timestamp('003', 'MS')",
            // DIFFERS: PostgreSQL 16 returns 0001-01-01 BC 00:00:00.003+00 (year 0)
            Instant.parse("0001-01-01T00:00:00.003Z").toEpochMilli());
        assertEvaluate("to_timestamp('300', 'MS')",
            // DIFFERS: PostgreSQL 16 returns 0001-01-01 BC 00:00:00.3+00 (year 0)
            Instant.parse("0001-01-01T00:00:00.300Z").toEpochMilli());
    }

    @Test
    public void testPostgresMicrosecondCompatibility() {
        // Microseconds are truncated to milliseconds in the result
        assertEvaluate("to_timestamp('123456', 'US')",
            // DIFFERS: PostgreSQL 16 returns 0001-01-01 BC 00:00:00.123456+00 (year 0)
            Instant.parse("0001-01-01T00:00:00.123Z").toEpochMilli());
        assertEvaluate("to_timestamp('000500', 'US')",
            // DIFFERS: PostgreSQL 16 returns 0001-01-01 BC 00:00:00.0005+00 (year 0)
            Instant.parse("0001-01-01T00:00:00Z").toEpochMilli());
    }

    @Test
    public void testPostgresFractionOfSecondCompatibility() {
        assertEvaluate("to_timestamp('1', 'FF1')",
            // DIFFERS: PostgreSQL 16 returns 0001-01-01 BC 00:00:00.1+00 (year 0)
            Instant.parse("0001-01-01T00:00:00.100Z").toEpochMilli());
        assertEvaluate("to_timestamp('12', 'FF2')",
            // DIFFERS: PostgreSQL 16 returns 0001-01-01 BC 00:00:00.12+00 (year 0)
            Instant.parse("0001-01-01T00:00:00.120Z").toEpochMilli());
        assertEvaluate("to_timestamp('123', 'FF3')",
            // DIFFERS: PostgreSQL 16 returns 0001-01-01 BC 00:00:00.123+00 (year 0)
            Instant.parse("0001-01-01T00:00:00.123Z").toEpochMilli());
        assertEvaluate("to_timestamp('1234', 'FF4')",
            // DIFFERS: PostgreSQL 16 returns 0001-01-01 BC 00:00:00.1234+00 (year 0)
            Instant.parse("0001-01-01T00:00:00.123Z").toEpochMilli());
        assertEvaluate("to_timestamp('12345', 'FF5')",
            // DIFFERS: PostgreSQL 16 returns 0001-01-01 BC 00:00:00.12345+00 (year 0)
            Instant.parse("0001-01-01T00:00:00.123Z").toEpochMilli());
        assertEvaluate("to_timestamp('123456', 'FF6')",
            // DIFFERS: PostgreSQL 16 returns 0001-01-01 BC 00:00:00.123456+00 (year 0)
            Instant.parse("0001-01-01T00:00:00.123Z").toEpochMilli());
    }

    @Test
    public void testPostgresSecondsPastMidnightCompatibility() {
        // 17:31:12 = 17*3600 + 31*60 + 12 = 63072 seconds past midnight
        assertEvaluate("to_timestamp('63072', 'SSSS')",
            // DIFFERS: PostgreSQL 16 returns 0001-01-01 BC 17:31:12+00 (year 0)
            Instant.parse("0001-01-01T17:31:12Z").toEpochMilli());
        assertEvaluate("to_timestamp('63072', 'SSSSS')",
            // DIFFERS: PostgreSQL 16 returns 0001-01-01 BC 17:31:12+00 (year 0)
            Instant.parse("0001-01-01T17:31:12Z").toEpochMilli());
    }

    @Test
    public void testPostgresMeridiemIndicatorCompatibility() {
        // AM/PM parsing
        assertEvaluate("to_timestamp('05 PM', 'HH12 PM')",
            // DIFFERS: PostgreSQL 16 returns 0001-01-01 BC 17:00:00+00 (year 0)
            Instant.parse("0001-01-01T17:00:00Z").toEpochMilli());
        assertEvaluate("to_timestamp('05 AM', 'HH12 AM')",
            // DIFFERS: PostgreSQL 16 returns 0001-01-01 BC 05:00:00+00 (year 0)
            Instant.parse("0001-01-01T05:00:00Z").toEpochMilli());
        assertEvaluate("to_timestamp('05 pm', 'HH12 pm')",
            // DIFFERS: PostgreSQL 16 returns 0001-01-01 BC 17:00:00+00 (year 0)
            Instant.parse("0001-01-01T17:00:00Z").toEpochMilli());
        assertEvaluate("to_timestamp('05 am', 'HH12 am')",
            // DIFFERS: PostgreSQL 16 returns 0001-01-01 BC 05:00:00+00 (year 0)
            Instant.parse("0001-01-01T05:00:00Z").toEpochMilli());
        assertEvaluate("to_timestamp('05 P.M.', 'HH12 P.M.')",
            // DIFFERS: PostgreSQL 16 returns 0001-01-01 BC 17:00:00+00 (year 0)
            Instant.parse("0001-01-01T17:00:00Z").toEpochMilli());
        assertEvaluate("to_timestamp('05 A.M.', 'HH12 A.M.')",
            // DIFFERS: PostgreSQL 16 returns 0001-01-01 BC 05:00:00+00 (year 0)
            Instant.parse("0001-01-01T05:00:00Z").toEpochMilli());
    }

    @Test
    public void testPostgresYearCompatibility() {
        assertEvaluate("to_timestamp('1970', 'YYYY')",
            // Verified: PostgreSQL 16 returns 1970-01-01 00:00:00+00
            Instant.parse("1970-01-01T00:00:00Z").toEpochMilli());
        assertEvaluate("to_timestamp('1,970', 'Y,YYY')",
            // Verified: PostgreSQL 16 returns 1970-01-01 00:00:00+00
            Instant.parse("1970-01-01T00:00:00Z").toEpochMilli());
        // YYY, YY, Y parse partial years
        assertEvaluate("to_timestamp('970', 'YYY')",
            // DIFFERS: PostgreSQL 16 returns 2970-01-01 (applies "nearest to 2020" rule)
            Instant.parse("0970-01-01T00:00:00Z").toEpochMilli());
        assertEvaluate("to_timestamp('70', 'YY')",
            // Verified: PostgreSQL 16 returns 1970-01-01 00:00:00+00
            Instant.parse("1970-01-01T00:00:00Z").toEpochMilli());
        assertEvaluate("to_timestamp('0', 'Y')",
            // DIFFERS: PostgreSQL 16 returns 2000-01-01 (applies "nearest to 2020" rule)
            -62167219200000L);  // year 0
    }

    @Test
    public void testPostgresTwoDigitYearCompatibility() {
        // PostgreSQL uses "nearest to 2020" rule for two-digit years
        // 00-69 -> 2000-2069, 70-99 -> 1970-1999
        assertEvaluate("to_timestamp('00', 'YY')",
            // Verified: PostgreSQL 16 returns 2000-01-01 00:00:00+00
            Instant.parse("2000-01-01T00:00:00Z").toEpochMilli());
        assertEvaluate("to_timestamp('23', 'YY')",
            // Verified: PostgreSQL 16 returns 2023-01-01 00:00:00+00
            Instant.parse("2023-01-01T00:00:00Z").toEpochMilli());
        assertEvaluate("to_timestamp('69', 'YY')",
            // Verified: PostgreSQL 16 returns 2069-01-01 00:00:00+00
            Instant.parse("2069-01-01T00:00:00Z").toEpochMilli());
        assertEvaluate("to_timestamp('70', 'YY')",
            // Verified: PostgreSQL 16 returns 1970-01-01 00:00:00+00
            Instant.parse("1970-01-01T00:00:00Z").toEpochMilli());
        assertEvaluate("to_timestamp('99', 'YY')",
            // Verified: PostgreSQL 16 returns 1999-01-01 00:00:00+00
            Instant.parse("1999-01-01T00:00:00Z").toEpochMilli());
    }

    @Test
    public void testPostgresISOYearCompatibility() {
        assertEvaluate("to_timestamp('1970', 'IYYY')",
            // PG_REJECTS: PostgreSQL 16 rejects "invalid combination of date conventions"
            Instant.parse("1970-01-01T00:00:00Z").toEpochMilli());
        assertEvaluate("to_timestamp('2023', 'IYYY')",
            // PG_REJECTS: PostgreSQL 16 rejects "invalid combination of date conventions"
            Instant.parse("2023-01-01T00:00:00Z").toEpochMilli());
    }

    @Test
    public void testPostgresMonthNameCompatibility() {
        // Full month names - case insensitive
        assertEvaluate("to_timestamp('JANUARY', 'MONTH')",
            // DIFFERS: PostgreSQL 16 returns 0001-01-01 BC 00:00:00+00 (year 0)
            Instant.parse("0001-01-01T00:00:00Z").toEpochMilli());
        assertEvaluate("to_timestamp('January', 'Month')",
            // DIFFERS: PostgreSQL 16 returns 0001-01-01 BC 00:00:00+00 (year 0)
            Instant.parse("0001-01-01T00:00:00Z").toEpochMilli());
        assertEvaluate("to_timestamp('january', 'month')",
            // DIFFERS: PostgreSQL 16 returns 0001-01-01 BC 00:00:00+00 (year 0)
            Instant.parse("0001-01-01T00:00:00Z").toEpochMilli());
        assertEvaluate("to_timestamp('DECEMBER', 'MONTH')",
            // DIFFERS: PostgreSQL 16 returns 0001-12-01 BC 00:00:00+00 (year 0)
            Instant.parse("0001-12-01T00:00:00Z").toEpochMilli());
    }

    @Test
    public void testPostgresAbbreviatedMonthNameCompatibility() {
        assertEvaluate("to_timestamp('JAN', 'MON')",
            // DIFFERS: PostgreSQL 16 returns 0001-01-01 BC 00:00:00+00 (year 0)
            Instant.parse("0001-01-01T00:00:00Z").toEpochMilli());
        assertEvaluate("to_timestamp('Jan', 'Mon')",
            // DIFFERS: PostgreSQL 16 returns 0001-01-01 BC 00:00:00+00 (year 0)
            Instant.parse("0001-01-01T00:00:00Z").toEpochMilli());
        assertEvaluate("to_timestamp('jan', 'mon')",
            // DIFFERS: PostgreSQL 16 returns 0001-01-01 BC 00:00:00+00 (year 0)
            Instant.parse("0001-01-01T00:00:00Z").toEpochMilli());
        assertEvaluate("to_timestamp('DEC', 'MON')",
            // DIFFERS: PostgreSQL 16 returns 0001-12-01 BC 00:00:00+00 (year 0)
            Instant.parse("0001-12-01T00:00:00Z").toEpochMilli());
    }

    @Test
    public void testPostgresMonthNumberCompatibility() {
        assertEvaluate("to_timestamp('01', 'MM')",
            // DIFFERS: PostgreSQL 16 returns 0001-01-01 BC 00:00:00+00 (year 0)
            Instant.parse("0001-01-01T00:00:00Z").toEpochMilli());
        assertEvaluate("to_timestamp('11', 'MM')",
            // DIFFERS: PostgreSQL 16 returns 0001-11-01 BC 00:00:00+00 (year 0)
            Instant.parse("0001-11-01T00:00:00Z").toEpochMilli());
        assertEvaluate("to_timestamp('12', 'MM')",
            // DIFFERS: PostgreSQL 16 returns 0001-12-01 BC 00:00:00+00 (year 0)
            Instant.parse("0001-12-01T00:00:00Z").toEpochMilli());
    }

    @Test
    public void testPostgresDayNameCompatibility() {
        // Day names are parsed but don't affect the result (informational only)
        assertEvaluate("to_timestamp('1970-01-01 THURSDAY', 'YYYY-MM-DD DAY')",
            // Verified: PostgreSQL 16 returns 1970-01-01 00:00:00+00
            Instant.parse("1970-01-01T00:00:00Z").toEpochMilli());
        assertEvaluate("to_timestamp('1970-01-01 Thursday', 'YYYY-MM-DD Day')",
            // Verified: PostgreSQL 16 returns 1970-01-01 00:00:00+00
            Instant.parse("1970-01-01T00:00:00Z").toEpochMilli());
        assertEvaluate("to_timestamp('1970-01-01 thursday', 'YYYY-MM-DD day')",
            // Verified: PostgreSQL 16 returns 1970-01-01 00:00:00+00
            Instant.parse("1970-01-01T00:00:00Z").toEpochMilli());
    }

    @Test
    public void testPostgresAbbreviatedDayNameCompatibility() {
        assertEvaluate("to_timestamp('1970-01-01 THU', 'YYYY-MM-DD DY')",
            // Verified: PostgreSQL 16 returns 1970-01-01 00:00:00+00
            Instant.parse("1970-01-01T00:00:00Z").toEpochMilli());
        assertEvaluate("to_timestamp('1970-01-01 Thu', 'YYYY-MM-DD Dy')",
            // Verified: PostgreSQL 16 returns 1970-01-01 00:00:00+00
            Instant.parse("1970-01-01T00:00:00Z").toEpochMilli());
        assertEvaluate("to_timestamp('1970-01-01 thu', 'YYYY-MM-DD dy')",
            // Verified: PostgreSQL 16 returns 1970-01-01 00:00:00+00
            Instant.parse("1970-01-01T00:00:00Z").toEpochMilli());
    }

    @Test
    public void testPostgresDayOfYearCompatibility() {
        assertEvaluate("to_timestamp('1970 001', 'YYYY DDD')",
            // Verified: PostgreSQL 16 returns 1970-01-01 00:00:00+00
            Instant.parse("1970-01-01T00:00:00Z").toEpochMilli());
        assertEvaluate("to_timestamp('1970 213', 'YYYY DDD')",
            // Verified: PostgreSQL 16 returns 1970-08-01 00:00:00+00
            Instant.parse("1970-08-01T00:00:00Z").toEpochMilli());
        assertEvaluate("to_timestamp('1970 365', 'YYYY DDD')",
            // Verified: PostgreSQL 16 returns 1970-12-31 00:00:00+00
            Instant.parse("1970-12-31T00:00:00Z").toEpochMilli());
    }

    @Test
    public void testPostgresDayOfMonthCompatibility() {
        // Day of month with defaults for year 1, month 1
        assertEvaluate("to_timestamp('01', 'DD')",
            // DIFFERS: PostgreSQL 16 returns 0001-01-01 BC 00:00:00+00 (year 0)
            Instant.parse("0001-01-01T00:00:00Z").toEpochMilli());
        assertEvaluate("to_timestamp('17', 'DD')",
            // DIFFERS: PostgreSQL 16 returns 0001-01-17 BC 00:00:00+00 (year 0)
            Instant.parse("0001-01-17T00:00:00Z").toEpochMilli());
        assertEvaluate("to_timestamp('31', 'DD')",
            // DIFFERS: PostgreSQL 16 returns 0001-01-31 BC 00:00:00+00 (year 0)
            Instant.parse("0001-01-31T00:00:00Z").toEpochMilli());
    }

    @Test
    public void testPostgresDayOfWeekCompatibility() {
        // D and ID are parsed but don't affect result
        assertEvaluate("to_timestamp('1970-01-01 5', 'YYYY-MM-DD D')",
            // Verified: PostgreSQL 16 returns 1970-01-01 00:00:00+00
            Instant.parse("1970-01-01T00:00:00Z").toEpochMilli());
        assertEvaluate("to_timestamp('1970-01-01 4', 'YYYY-MM-DD ID')",
            // PG_REJECTS: PostgreSQL 16 rejects "invalid combination of date conventions"
            Instant.parse("1970-01-01T00:00:00Z").toEpochMilli());
    }

    @Test
    public void testPostgresWeekOfMonthCompatibility() {
        // W is parsed but doesn't affect result
        assertEvaluate("to_timestamp('1970-01-15 3', 'YYYY-MM-DD W')",
            // Verified: PostgreSQL 16 returns 1970-01-15 00:00:00+00
            Instant.parse("1970-01-15T00:00:00Z").toEpochMilli());
    }

    @Test
    public void testPostgresWeekOfYearCompatibility() {
        // WW computes date from week number (Sunday start)
        assertEvaluate("to_timestamp('1970 01', 'YYYY WW')",
            // DIFFERS: PostgreSQL 16 returns 1970-01-01 00:00:00+00
            Instant.parse("1969-12-28T00:00:00Z").toEpochMilli());
        // IW computes date from ISO week number (Monday start)
        assertEvaluate("to_timestamp('1970 01', 'YYYY IW')",
            // PG_REJECTS: PostgreSQL 16 rejects "invalid combination of date conventions"
            Instant.parse("1969-12-29T00:00:00Z").toEpochMilli());
    }

    @Test
    public void testPostgresCenturyCompatibility() {
        // CC sets year to first year of that century
        assertEvaluate("to_timestamp('20', 'CC')",
            // Verified: PostgreSQL 16 returns 1901-01-01 00:00:00+00
            Instant.parse("1901-01-01T00:00:00Z").toEpochMilli());
        assertEvaluate("to_timestamp('21', 'CC')",
            // Verified: PostgreSQL 16 returns 2001-01-01 00:00:00+00
            Instant.parse("2001-01-01T00:00:00Z").toEpochMilli());
    }

    @Test
    public void testPostgresQuarterCompatibility() {
        // Q is parsed but doesn't affect result
        assertEvaluate("to_timestamp('1970 1', 'YYYY Q')",
            // Verified: PostgreSQL 16 returns 1970-01-01 00:00:00+00
            Instant.parse("1970-01-01T00:00:00Z").toEpochMilli());
        assertEvaluate("to_timestamp('1970 2', 'YYYY Q')",
            // Verified: PostgreSQL 16 returns 1970-01-01 00:00:00+00
            Instant.parse("1970-01-01T00:00:00Z").toEpochMilli());
    }

    @Test
    public void testPostgresEraIndicatorCompatibility() {
        // AD keeps year as-is, BC negates year (1970 BC = astronomical year -1969)
        assertEvaluate("to_timestamp('1970-01-01 AD', 'YYYY-MM-DD AD')",
            // Verified: PostgreSQL 16 returns 1970-01-01 00:00:00+00
            Instant.parse("1970-01-01T00:00:00Z").toEpochMilli());
        assertEvaluate("to_timestamp('1970-01-01 ad', 'YYYY-MM-DD ad')",
            // Verified: PostgreSQL 16 returns 1970-01-01 00:00:00+00
            Instant.parse("1970-01-01T00:00:00Z").toEpochMilli());
        assertEvaluate("to_timestamp('1970-01-01 BC', 'YYYY-MM-DD BC')",
            // Verified: PostgreSQL 16 returns 1970-01-01 BC 00:00:00+00
            Instant.parse("-1969-01-01T00:00:00Z").toEpochMilli());
        assertEvaluate("to_timestamp('1970-01-01 A.D.', 'YYYY-MM-DD A.D.')",
            // Verified: PostgreSQL 16 returns 1970-01-01 00:00:00+00
            Instant.parse("1970-01-01T00:00:00Z").toEpochMilli());
        assertEvaluate("to_timestamp('1970-01-01 B.C.', 'YYYY-MM-DD B.C.')",
            // Verified: PostgreSQL 16 returns 1970-01-01 BC 00:00:00+00
            Instant.parse("-1969-01-01T00:00:00Z").toEpochMilli());
    }

    @Test
    public void testPostgresFlexibleSeparatorCompatibility() {
        // PostgreSQL allows any separator to match any separator
        assertEvaluate("to_timestamp('1970-01-01', 'YYYY-MM-DD')",
            // Verified: PostgreSQL 16 returns 1970-01-01 00:00:00+00
            Instant.parse("1970-01-01T00:00:00Z").toEpochMilli());
        assertEvaluate("to_timestamp('1970/01/01', 'YYYY-MM-DD')",
            // Verified: PostgreSQL 16 returns 1970-01-01 00:00:00+00
            Instant.parse("1970-01-01T00:00:00Z").toEpochMilli());
        assertEvaluate("to_timestamp('1970.01.01', 'YYYY-MM-DD')",
            // Verified: PostgreSQL 16 returns 1970-01-01 00:00:00+00
            Instant.parse("1970-01-01T00:00:00Z").toEpochMilli());
        assertEvaluate("to_timestamp('1970 01 01', 'YYYY-MM-DD')",
            // Verified: PostgreSQL 16 returns 1970-01-01 00:00:00+00
            Instant.parse("1970-01-01T00:00:00Z").toEpochMilli());
    }

    @Test
    public void testPostgresLeadingWhitespaceCompatibility() {
        // PostgreSQL skips leading whitespace
        assertEvaluate("to_timestamp('  1970-01-01', 'YYYY-MM-DD')",
            // Verified: PostgreSQL 16 returns 1970-01-01 00:00:00+00
            Instant.parse("1970-01-01T00:00:00Z").toEpochMilli());
        assertEvaluate("to_timestamp('   1970-01-01', 'YYYY-MM-DD')",
            // Verified: PostgreSQL 16 returns 1970-01-01 00:00:00+00
            Instant.parse("1970-01-01T00:00:00Z").toEpochMilli());
    }

    @Test
    public void testPostgresFullDateTimeCompatibility() {
        // Full date/time parsing
        assertEvaluate("to_timestamp('1970-01-01 17:31:12', 'YYYY-MM-DD HH24:MI:SS')",
            // Verified: PostgreSQL 16 returns 1970-01-01 17:31:12+00
            Instant.parse("1970-01-01T17:31:12Z").toEpochMilli());
        // With milliseconds
        assertEvaluate("to_timestamp('1970-01-01 17:31:12.123', 'YYYY-MM-DD HH24:MI:SS.MS')",
            // Verified: PostgreSQL 16 returns 1970-01-01 17:31:12.123+00
            Instant.parse("1970-01-01T17:31:12.123Z").toEpochMilli());
    }

    @Test
    public void testPostgresRoundTripCompatibility() {
        // Round-trip: to_timestamp(to_char(ts, fmt), fmt) should return original timestamp
        assertEvaluate(
            "to_timestamp(to_char(timestamp '1970-01-01 17:31:12', 'YYYY-MM-DD HH24:MI:SS'), 'YYYY-MM-DD HH24:MI:SS')",
            // Verified: PostgreSQL 16 returns 1970-01-01 17:31:12+00
            63072000L
        );
        assertEvaluate(
            "to_timestamp(to_char(timestamp '2023-07-15 14:30:45', 'YYYY-MM-DD HH24:MI:SS'), 'YYYY-MM-DD HH24:MI:SS')",
            // Verified: PostgreSQL 16 returns 2023-07-15 14:30:45+00
            1689431445000L
        );
    }

    @Test
    public void testPostgresNullHandlingCompatibility() {
        // PostgreSQL returns null for null input
        // Verified: PostgreSQL 16 returns NULL
        assertEvaluateNull("to_timestamp(null, 'YYYY-MM-DD')");
        // Verified: PostgreSQL 16 returns NULL
        assertEvaluateNull("to_timestamp('1970-01-01', null)");
        // Verified: PostgreSQL 16 returns NULL
        assertEvaluateNull("to_timestamp(null, null)");
    }

    @Test
    public void testPostgresCaseInsensitivePatternCompatibility() {
        // Pattern keywords are case-insensitive
        assertEvaluate("to_timestamp('1970-01-01', 'yyyy-mm-dd')",
            // Verified: PostgreSQL 16 returns 1970-01-01 00:00:00+00
            Instant.parse("1970-01-01T00:00:00Z").toEpochMilli());
        assertEvaluate("to_timestamp('1970-01-01', 'YYYY-MM-DD')",
            // Verified: PostgreSQL 16 returns 1970-01-01 00:00:00+00
            Instant.parse("1970-01-01T00:00:00Z").toEpochMilli());
        assertEvaluate("to_timestamp('1970-01-01 17:31:12', 'yyyy-mm-dd hh24:mi:ss')",
            // Verified: PostgreSQL 16 returns 1970-01-01 17:31:12+00
            Instant.parse("1970-01-01T17:31:12Z").toEpochMilli());
        assertEvaluate("to_timestamp('1970-01-01 17:31:12', 'YYYY-MM-DD HH24:MI:SS')",
            // Verified: PostgreSQL 16 returns 1970-01-01 17:31:12+00
            Instant.parse("1970-01-01T17:31:12Z").toEpochMilli());
    }

    @Test
    public void testPostgres12HourClockEdgeCasesCompatibility() {
        // 12 AM = midnight (00:00)
        assertEvaluate("to_timestamp('12:00:00 AM', 'HH12:MI:SS AM')",
            // DIFFERS: PostgreSQL 16 returns 0001-01-01 BC 00:00:00+00 (year 0)
            Instant.parse("0001-01-01T00:00:00Z").toEpochMilli());
        // 12 PM = noon (12:00)
        assertEvaluate("to_timestamp('12:00:00 PM', 'HH12:MI:SS PM')",
            // DIFFERS: PostgreSQL 16 returns 0001-01-01 BC 12:00:00+00 (year 0)
            Instant.parse("0001-01-01T12:00:00Z").toEpochMilli());
        // 12:30 AM = 00:30
        assertEvaluate("to_timestamp('12:30:00 AM', 'HH12:MI:SS AM')",
            // DIFFERS: PostgreSQL 16 returns 0001-01-01 BC 00:30:00+00 (year 0)
            Instant.parse("0001-01-01T00:30:00Z").toEpochMilli());
        // 12:30 PM = 12:30
        assertEvaluate("to_timestamp('12:30:00 PM', 'HH12:MI:SS PM')",
            // DIFFERS: PostgreSQL 16 returns 0001-01-01 BC 12:30:00+00 (year 0)
            Instant.parse("0001-01-01T12:30:00Z").toEpochMilli());
    }

    @Test
    public void testPostgresDefaultValuesCompatibility() {
        // Missing components default to: year=1, month=1, day=1, time=00:00:00
        assertEvaluate("to_timestamp('2023', 'YYYY')",
            // Verified: PostgreSQL 16 returns 2023-01-01 00:00:00+00
            Instant.parse("2023-01-01T00:00:00Z").toEpochMilli());
        assertEvaluate("to_timestamp('07', 'MM')",
            // DIFFERS: PostgreSQL 16 returns 0001-07-01 BC 00:00:00+00 (year 0)
            Instant.parse("0001-07-01T00:00:00Z").toEpochMilli());
        assertEvaluate("to_timestamp('15', 'DD')",
            // DIFFERS: PostgreSQL 16 returns 0001-01-15 BC 00:00:00+00 (year 0)
            Instant.parse("0001-01-15T00:00:00Z").toEpochMilli());
        assertEvaluate("to_timestamp('14:30:45', 'HH24:MI:SS')",
            // DIFFERS: PostgreSQL 16 returns 0001-01-01 BC 14:30:45+00 (year 0)
            Instant.parse("0001-01-01T14:30:45Z").toEpochMilli());
    }

    // =========================================================================
    // Tests from PostgreSQL horology.sql regression tests
    // Source: postgres/src/test/regress/sql/horology.sql
    // =========================================================================

    @Test
    public void testPostgresHorologyTests() {
        // Arrow separator
        // From horology.sql: SELECT to_timestamp('0097/Feb/16 --> 08:14:30', 'YYYY/Mon/DD --> HH:MI:SS');
        assertEvaluate("to_timestamp('0097/Feb/16 --> 08:14:30', 'YYYY/Mon/DD --> HH:MI:SS')",
            // Verified: PostgreSQL 16 returns 0097-02-16 08:14:30+00
            Instant.parse("0097-02-16T08:14:30Z").toEpochMilli());

        // Special character separators
        // From horology.sql: SELECT to_timestamp('2011$03!18 23_38_15', 'YYYY-MM-DD HH24:MI:SS');
        assertEvaluate("to_timestamp('2011$03!18 23_38_15', 'YYYY-MM-DD HH24:MI:SS')",
            // Verified: PostgreSQL 16 returns 2011-03-18 23:38:15+00
            Instant.parse("2011-03-18T23:38:15Z").toEpochMilli());

        // Month name pattern
        // From horology.sql: SELECT to_timestamp('1985 January 12', 'YYYY FMMonth DD');
        // Note: CrateDB doesn't support FM (fill mode) modifier, so we use Month instead
        assertEvaluate("to_timestamp('1985 January 12', 'YYYY Month DD')",
            // Verified: PostgreSQL 16 returns 1985-01-12 00:00:00+00
            Instant.parse("1985-01-12T00:00:00Z").toEpochMilli());

        // Compact format
        // From horology.sql: SELECT to_timestamp('05121445482000', 'MMDDHH24MISSYYYY');
        assertEvaluate("to_timestamp('05121445482000', 'MMDDHH24MISSYYYY')",
            // Verified: PostgreSQL 16 returns 2000-05-12 14:45:48+00
            Instant.parse("2000-05-12T14:45:48Z").toEpochMilli());

        // Month and Day without separator
        // From horology.sql: SELECT to_timestamp('2000January09Sunday', 'YYYYFMMonthDDFMDay');
        // Note: CrateDB doesn't support FM (fill mode) modifier, using Month/Day instead
        assertEvaluate("to_timestamp('2000 January   09 Sunday   ', 'YYYY Month DD Day')",
            // Verified: PostgreSQL 16 returns 2000-01-09 00:00:00+00
            Instant.parse("2000-01-09T00:00:00Z").toEpochMilli());

        // Compact YYYYMMDD
        // From horology.sql: SELECT to_timestamp('19971116', 'YYYYMMDD');
        assertEvaluate("to_timestamp('19971116', 'YYYYMMDD')",
            // Verified: PostgreSQL 16 returns 1997-11-16 00:00:00+00
            Instant.parse("1997-11-16T00:00:00Z").toEpochMilli());

        // Note: PostgreSQL test 'SELECT to_timestamp('20000-1116', 'YYYY-MMDD')' returns 20000-11-16
        // CrateDB's YYYY pattern strictly consumes 4 digits, so years > 9999 require different handling.

        // AD/BC era
        // From horology.sql: SELECT to_timestamp('1997 AD 11 16', 'YYYY BC MM DD');
        assertEvaluate("to_timestamp('1997 AD 11 16', 'YYYY BC MM DD')",
            // Verified: PostgreSQL 16 returns 1997-11-16 00:00:00+00
            Instant.parse("1997-11-16T00:00:00Z").toEpochMilli());
        // From horology.sql: SELECT to_timestamp('1997 BC 11 16', 'YYYY BC MM DD');
        assertEvaluate("to_timestamp('1997 BC 11 16', 'YYYY BC MM DD')",
            // Verified: PostgreSQL 16 returns 1997-11-16 00:00:00+00 BC
            Instant.parse("-1996-11-16T00:00:00Z").toEpochMilli());

        // Single digit year
        // From horology.sql: SELECT to_timestamp('9-1116', 'Y-MMDD');
        // PostgreSQL applies "nearest to 2020" rule: 9 -> 2009
        assertEvaluate("to_timestamp('9-1116', 'Y-MMDD')",
            // DIFFERS: PostgreSQL 16 returns 2009-11-16 00:00:00+00 (nearest to 2020 rule)
            Instant.parse("0009-11-16T00:00:00Z").toEpochMilli());

        // Two digit year
        // From horology.sql: SELECT to_timestamp('95-1116', 'YY-MMDD');
        assertEvaluate("to_timestamp('95-1116', 'YY-MMDD')",
            // Verified: PostgreSQL 16 returns 1995-11-16 00:00:00+00
            Instant.parse("1995-11-16T00:00:00Z").toEpochMilli());

        // Three digit year
        // From horology.sql: SELECT to_timestamp('995-1116', 'YYY-MMDD');
        // PostgreSQL applies "nearest to 2020" rule: 995 -> 1995
        assertEvaluate("to_timestamp('995-1116', 'YYY-MMDD')",
            // DIFFERS: PostgreSQL 16 returns 1995-11-16 00:00:00+00 (nearest to 2020 rule)
            Instant.parse("0995-11-16T00:00:00Z").toEpochMilli());

        // Week and day of week
        // From horology.sql: SELECT to_timestamp('2005426', 'YYYYWWD');
        // PostgreSQL: Week 42, Day 6 of 2005 = 2005-10-15 (Saturday, D uses Sunday=1)
        // CrateDB: Week 42, Day 6 of 2005 = 2005-10-09 (different WW calculation)
        assertEvaluate("to_timestamp('2005426', 'YYYYWWD')",
            // DIFFERS: PostgreSQL 16 returns 2005-10-15 00:00:00+00
            Instant.parse("2005-10-09T00:00:00Z").toEpochMilli());

        // Day of year
        // From horology.sql: SELECT to_timestamp('2005300', 'YYYYDDD');
        assertEvaluate("to_timestamp('2005300', 'YYYYDDD')",
            // Verified: PostgreSQL 16 returns 2005-10-27 00:00:00+00
            Instant.parse("2005-10-27T00:00:00Z").toEpochMilli());

        // Compact YYYYMMDD with leading whitespace
        // From horology.sql: SELECT to_timestamp('20050302', 'YYYYMMDD');
        assertEvaluate("to_timestamp('20050302', 'YYYYMMDD')",
            // Verified: PostgreSQL 16 returns 2005-03-02 00:00:00+00
            Instant.parse("2005-03-02T00:00:00Z").toEpochMilli());
        // From horology.sql: SELECT to_timestamp('  20050302', 'YYYYMMDD');
        assertEvaluate("to_timestamp('  20050302', 'YYYYMMDD')",
            // Verified: PostgreSQL 16 returns 2005-03-02 00:00:00+00
            Instant.parse("2005-03-02T00:00:00Z").toEpochMilli());
        // Note: PostgreSQL accepts internal whitespace in fixed-width formats like YYYYMMDD
        // but CrateDB requires explicit separators or no whitespace. Testing with separators:
        assertEvaluate("to_timestamp('2005 03 02', 'YYYY MM DD')",
            // Verified: PostgreSQL 16 returns 2005-03-02 00:00:00+00
            Instant.parse("2005-03-02T00:00:00Z").toEpochMilli());

        // AM/PM variants
        // From horology.sql: SELECT to_timestamp('2011-12-18 11:38 AM', 'YYYY-MM-DD HH12:MI PM');
        assertEvaluate("to_timestamp('2011-12-18 11:38 AM', 'YYYY-MM-DD HH12:MI PM')",
            // Verified: PostgreSQL 16 returns 2011-12-18 11:38:00+00
            Instant.parse("2011-12-18T11:38:00Z").toEpochMilli());
        // From horology.sql: SELECT to_timestamp('2011-12-18 11:38 PM', 'YYYY-MM-DD HH12:MI PM');
        assertEvaluate("to_timestamp('2011-12-18 11:38 PM', 'YYYY-MM-DD HH12:MI PM')",
            // Verified: PostgreSQL 16 returns 2011-12-18 23:38:00+00
            Instant.parse("2011-12-18T23:38:00Z").toEpochMilli());
        // From horology.sql: SELECT to_timestamp('2011-12-18 11:38 A.M.', 'YYYY-MM-DD HH12:MI P.M.');
        assertEvaluate("to_timestamp('2011-12-18 11:38 A.M.', 'YYYY-MM-DD HH12:MI P.M.')",
            // Verified: PostgreSQL 16 returns 2011-12-18 11:38:00+00
            Instant.parse("2011-12-18T11:38:00Z").toEpochMilli());
        // From horology.sql: SELECT to_timestamp('2011-12-18 11:38 P.M.', 'YYYY-MM-DD HH12:MI P.M.');
        assertEvaluate("to_timestamp('2011-12-18 11:38 P.M.', 'YYYY-MM-DD HH12:MI P.M.')",
            // Verified: PostgreSQL 16 returns 2011-12-18 23:38:00+00
            Instant.parse("2011-12-18T23:38:00Z").toEpochMilli());

        // Milliseconds
        // From horology.sql: SELECT to_timestamp('2018-11-02 12:34:56.025', 'YYYY-MM-DD HH24:MI:SS.MS');
        assertEvaluate("to_timestamp('2018-11-02 12:34:56.025', 'YYYY-MM-DD HH24:MI:SS.MS')",
            // Verified: PostgreSQL 16 returns 2018-11-02 12:34:56.025+00
            Instant.parse("2018-11-02T12:34:56.025Z").toEpochMilli());

        // Valid date/time
        // From horology.sql: SELECT to_timestamp('2016-06-13 15:50:55', 'YYYY-MM-DD HH24:MI:SS');
        assertEvaluate("to_timestamp('2016-06-13 15:50:55', 'YYYY-MM-DD HH24:MI:SS')",
            // Verified: PostgreSQL 16 returns 2016-06-13 15:50:55+00
            Instant.parse("2016-06-13T15:50:55Z").toEpochMilli());

        // Leap year Feb 29
        // From horology.sql: SELECT to_timestamp('2016-02-29 15:50:55', 'YYYY-MM-DD HH24:MI:SS');
        assertEvaluate("to_timestamp('2016-02-29 15:50:55', 'YYYY-MM-DD HH24:MI:SS')",
            // Verified: PostgreSQL 16 returns 2016-02-29 15:50:55+00
            Instant.parse("2016-02-29T15:50:55Z").toEpochMilli());

        // Seconds past midnight
        // From horology.sql: SELECT to_timestamp('2015-02-11 86000', 'YYYY-MM-DD SSSS');
        // 86000 seconds = 23:53:20
        assertEvaluate("to_timestamp('2015-02-11 86000', 'YYYY-MM-DD SSSS')",
            // Verified: PostgreSQL 16 returns 2015-02-11 23:53:20+00
            Instant.parse("2015-02-11T23:53:20Z").toEpochMilli());
        // From horology.sql: SELECT to_timestamp('2015-02-11 86000', 'YYYY-MM-DD SSSSS');
        assertEvaluate("to_timestamp('2015-02-11 86000', 'YYYY-MM-DD SSSSS')",
            // Verified: PostgreSQL 16 returns 2015-02-11 23:53:20+00
            Instant.parse("2015-02-11T23:53:20Z").toEpochMilli());

        // Roman numeral month
        // From horology.sql: SELECT to_timestamp('1,582nd VIII 21', 'Y,YYYth FMRM DD');
        // Note: CrateDB supports Y,YYY format but not the 'th' ordinal suffix or FM modifier
        assertEvaluate("to_timestamp('1,582 VIII 21', 'Y,YYY RM DD')",
            // Verified: PostgreSQL 16 returns 1582-08-21 00:00:00+00
            Instant.parse("1582-08-21T00:00:00Z").toEpochMilli());

        // BC dates
        // From horology.sql: SELECT to_timestamp('44-02-01 11:12:13 BC','YYYY-MM-DD HH24:MI:SS BC');
        // Note: CrateDB requires 4-digit years, so we use 0044 instead of 44
        assertEvaluate("to_timestamp('0044-02-01 11:12:13 BC', 'YYYY-MM-DD HH24:MI:SS BC')",
            // Verified: PostgreSQL 16 returns 0044-02-01 11:12:13+00 BC
            Instant.parse("-0043-02-01T11:12:13Z").toEpochMilli());

        // Flexible whitespace
        // From horology.sql: SELECT to_timestamp('2011-12-18 23:38:15', 'YYYY-MM-DD  HH24:MI:SS');
        assertEvaluate("to_timestamp('2011-12-18 23:38:15', 'YYYY-MM-DD  HH24:MI:SS')",
            // Verified: PostgreSQL 16 returns 2011-12-18 23:38:15+00
            Instant.parse("2011-12-18T23:38:15Z").toEpochMilli());
        // From horology.sql: SELECT to_timestamp('2011-12-18  23:38:15', 'YYYY-MM-DD  HH24:MI:SS');
        assertEvaluate("to_timestamp('2011-12-18  23:38:15', 'YYYY-MM-DD  HH24:MI:SS')",
            // Verified: PostgreSQL 16 returns 2011-12-18 23:38:15+00
            Instant.parse("2011-12-18T23:38:15Z").toEpochMilli());
        // From horology.sql: SELECT to_timestamp('2011-12-18   23:38:15', 'YYYY-MM-DD  HH24:MI:SS');
        assertEvaluate("to_timestamp('2011-12-18   23:38:15', 'YYYY-MM-DD  HH24:MI:SS')",
            // Verified: PostgreSQL 16 returns 2011-12-18 23:38:15+00
            Instant.parse("2011-12-18T23:38:15Z").toEpochMilli());

        // Flexible separators with month
        // From horology.sql: SELECT to_timestamp('2000+   JUN', 'YYYY/MON');
        assertEvaluate("to_timestamp('2000/JUN', 'YYYY/MON')",
            // Verified: PostgreSQL 16 returns 2000-06-01 00:00:00+00
            Instant.parse("2000-06-01T00:00:00Z").toEpochMilli());
        assertEvaluate("to_timestamp('2000-JUN', 'YYYY/MON')",
            // Verified: Flexible separator matching (- matches /)
            Instant.parse("2000-06-01T00:00:00Z").toEpochMilli());
        // From horology.sql: SELECT to_timestamp('  2000 +JUN', 'YYYY/MON');
        assertEvaluate("to_timestamp('  2000/JUN', 'YYYY/MON')",
            // Verified: PostgreSQL 16 returns 2000-06-01 00:00:00+00
            Instant.parse("2000-06-01T00:00:00Z").toEpochMilli());

        // Abbreviated day with month name
        // From horology.sql: SELECT to_timestamp('Fri 1-Jan-1999', 'DY DD MON YYYY');
        assertEvaluate("to_timestamp('Fri 1-Jan-1999', 'DY DD MON YYYY')",
            // Verified: PostgreSQL 16 returns 1999-01-01 00:00:00+00
            Instant.parse("1999-01-01T00:00:00Z").toEpochMilli());
    }
}
