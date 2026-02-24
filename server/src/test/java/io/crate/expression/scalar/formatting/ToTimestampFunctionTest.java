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

import static io.crate.testing.Asserts.isNotSameInstance;
import static io.crate.testing.Asserts.isSameInstance;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;

import org.junit.Test;

import io.crate.expression.scalar.ScalarTestCase;
import io.crate.expression.symbol.Literal;

/**
 * Unit tests for the to_timestamp(text, text) function.
 *
 * <p>Verified against PostgreSQL 16 on 2026-02-24.
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
 *   <li><b>Microsecond precision:</b> CrateDB truncates microseconds (US, FF4-FF6) to milliseconds
 *       due to TIMESTAMPTZ storage precision. PostgreSQL preserves full microsecond precision.</li>
 *   <li><b>Large years (>9999):</b> YYYY pattern strictly consumes 4 digits in CrateDB,
 *       so years > 9999 are not supported with standard YYYY format.</li>
 *   <li><b>FM modifier:</b> Fill mode (FM) modifier not supported in CrateDB.
 *       Use padded patterns (Month, Day) with appropriate whitespace instead.</li>
 *   <li><b>'th' ordinal suffix:</b> Ordinal suffixes like 'th', 'nd', 'rd' not supported.</li>
 *   <li><b>Negative years in input:</b> Negative year values in the input string (e.g., '-44-02-01')
 *       are handled differently. Use the BC pattern instead for BC dates.</li>
 *   <li><b>Internal whitespace:</b> PostgreSQL accepts internal whitespace in fixed-width formats
 *       like YYYYMMDD, CrateDB requires explicit separators or no whitespace.</li>
 * </ul>
 *
 * @see ToTimestampFunctionPostgresCompatibilityTest for additional PostgreSQL compatibility tests
 */
public class ToTimestampFunctionTest extends ScalarTestCase {

    @Test
    public void testBasicDateParsingYyyyMmDd() {
        assertEvaluate(
            "to_timestamp('2023-07-15', 'YYYY-MM-DD')",
            // Verified: PostgreSQL 16 returns 2023-07-15 00:00:00+00
            Instant.parse("2023-07-15T00:00:00Z").toEpochMilli()
        );
    }

    @Test
    public void testDateWithTimeParsing() {
        assertEvaluate(
            "to_timestamp('2023-07-15 14:30:45', 'YYYY-MM-DD HH24:MI:SS')",
            // Verified: PostgreSQL 16 returns 2023-07-15 14:30:45+00
            Instant.parse("2023-07-15T14:30:45Z").toEpochMilli()
        );
    }

    @Test
    public void test12HourFormatWithAm() {
        assertEvaluate(
            "to_timestamp('2023-07-15 02:30:45 AM', 'YYYY-MM-DD HH12:MI:SS AM')",
            // Verified: PostgreSQL 16 returns 2023-07-15 02:30:45+00
            Instant.parse("2023-07-15T02:30:45Z").toEpochMilli()
        );
    }

    @Test
    public void test12HourFormatWithPm() {
        assertEvaluate(
            "to_timestamp('2023-07-15 02:30:45 PM', 'YYYY-MM-DD HH12:MI:SS PM')",
            // Verified: PostgreSQL 16 returns 2023-07-15 14:30:45+00
            Instant.parse("2023-07-15T14:30:45Z").toEpochMilli()
        );
    }

    @Test
    public void testFullMonthNameParsing() {
        assertEvaluate(
            "to_timestamp('15 July 2023', 'DD Month YYYY')",
            // Verified: PostgreSQL 16 returns 2023-07-15 00:00:00+00
            Instant.parse("2023-07-15T00:00:00Z").toEpochMilli()
        );
    }

    @Test
    public void testAbbreviatedMonthNameParsing() {
        assertEvaluate(
            "to_timestamp('15 Jul 2023', 'DD Mon YYYY')",
            // Verified: PostgreSQL 16 returns 2023-07-15 00:00:00+00
            Instant.parse("2023-07-15T00:00:00Z").toEpochMilli()
        );
    }

    @Test
    public void testMillisecondsParsing() {
        assertEvaluate(
            "to_timestamp('2023-07-15 14:30:45.123', 'YYYY-MM-DD HH24:MI:SS.MS')",
            // Verified: PostgreSQL 16 returns 2023-07-15 14:30:45.123+00
            Instant.parse("2023-07-15T14:30:45.123Z").toEpochMilli()
        );
    }

    @Test
    public void testNullInputReturnsNull() {
        // Verified: PostgreSQL 16 returns NULL
        assertEvaluateNull("to_timestamp(null, 'YYYY-MM-DD')");
    }

    @Test
    public void testNullInputViaReferenceReturnsNull() {
        // Test null input through a reference to cover the runtime null check branch
        assertEvaluateNull("to_timestamp(name, 'YYYY-MM-DD')", Literal.of((String) null));
    }

    @Test
    public void testNullPatternReturnsNull() {
        // Verified: PostgreSQL 16 returns NULL
        assertEvaluateNull("to_timestamp('2023-07-15', null)");
    }

    @Test
    public void testNullPatternViaReferenceReturnsNull() {
        // Test null pattern through a reference to cover the runtime null check branch
        assertEvaluateNull("to_timestamp('2023-07-15', name)", Literal.of((String) null));
    }

    @Test
    public void testBothNullReturnsNull() {
        // Test both arguments null to ensure short-circuit evaluation is covered
        assertEvaluateNull("to_timestamp(name, time_format)", Literal.of((String) null), Literal.of((String) null));
    }

    @Test
    public void testLowercasePatternWorks() {
        assertEvaluate(
            "to_timestamp('2023-07-15', 'yyyy-mm-dd')",
            // Verified: PostgreSQL 16 returns 2023-07-15 00:00:00+00
            Instant.parse("2023-07-15T00:00:00Z").toEpochMilli()
        );
    }

    @Test
    public void testTwoDigitYear00To69MapsTo2000s() {
        // PostgreSQL uses "nearest to 2020" rule: 00-69 -> 2000-2069
        assertEvaluate(
            "to_timestamp('23-07-15', 'YY-MM-DD')",
            // Verified: PostgreSQL 16 returns 2023-07-15 00:00:00+00
            Instant.parse("2023-07-15T00:00:00Z").toEpochMilli()
        );
        assertEvaluate(
            "to_timestamp('69-07-15', 'YY-MM-DD')",
            // Verified: PostgreSQL 16 returns 2069-07-15 00:00:00+00
            Instant.parse("2069-07-15T00:00:00Z").toEpochMilli()
        );
    }

    @Test
    public void testTwoDigitYear70To99MapsTo1900s() {
        // PostgreSQL uses "nearest to 2020" rule: 70-99 -> 1970-1999
        assertEvaluate(
            "to_timestamp('70-07-15', 'YY-MM-DD')",
            // Verified: PostgreSQL 16 returns 1970-07-15 00:00:00+00
            Instant.parse("1970-07-15T00:00:00Z").toEpochMilli()
        );
        assertEvaluate(
            "to_timestamp('99-07-15', 'YY-MM-DD')",
            // Verified: PostgreSQL 16 returns 1999-07-15 00:00:00+00
            Instant.parse("1999-07-15T00:00:00Z").toEpochMilli()
        );
    }

    @Test
    public void testCompileWithLiteralPatternReturnsOptimizedInstance() {
        assertCompile("to_timestamp(name, 'YYYY-MM-DD')", isNotSameInstance());
    }

    @Test
    public void testCompileWithRefPatternReturnsSameInstance() {
        assertCompile("to_timestamp(name, time_format)", isSameInstance());
    }

    @Test
    public void testCompileWithNullLiteralPatternReturnsSameInstance() {
        // When pattern is a null literal, compile should return same instance (no optimization possible)
        assertCompile("to_timestamp(name, null)", isSameInstance());
    }

    @Test
    public void testRoundTripWithToChar() {
        assertEvaluate(
            "to_timestamp(to_char(timestamp '2023-07-15 14:30:45', 'YYYY-MM-DD HH24:MI:SS'), 'YYYY-MM-DD HH24:MI:SS')",
            // Verified: PostgreSQL 16 returns 2023-07-15 14:30:45+00
            Instant.parse("2023-07-15T14:30:45Z").toEpochMilli()
        );
    }

    @Test
    public void testDefaultsMissingComponentsToEpochStart() {
        assertEvaluate(
            "to_timestamp('2023', 'YYYY')",
            // Verified: PostgreSQL 16 returns 2023-01-01 00:00:00+00
            Instant.parse("2023-01-01T00:00:00Z").toEpochMilli()
        );
    }

    @Test
    public void testFullDayNameIgnoredDuringParsing() {
        assertEvaluate(
            "to_timestamp('Saturday 2023-07-15', 'Day YYYY-MM-DD')",
            // Verified: PostgreSQL 16 returns 2023-07-15 00:00:00+00
            Instant.parse("2023-07-15T00:00:00Z").toEpochMilli()
        );
    }

    @Test
    public void testAbbreviatedDayNameIgnoredDuringParsing() {
        assertEvaluate(
            "to_timestamp('Sat 2023-07-15', 'Dy YYYY-MM-DD')",
            // Verified: PostgreSQL 16 returns 2023-07-15 00:00:00+00
            Instant.parse("2023-07-15T00:00:00Z").toEpochMilli()
        );
    }

    @Test
    public void testMonthNameCaseInsensitive() {
        assertEvaluate(
            "to_timestamp('15 JULY 2023', 'DD Month YYYY')",
            // Verified: PostgreSQL 16 returns 2023-07-15 00:00:00+00
            Instant.parse("2023-07-15T00:00:00Z").toEpochMilli()
        );
        assertEvaluate(
            "to_timestamp('15 july 2023', 'DD Month YYYY')",
            // Verified: PostgreSQL 16 returns 2023-07-15 00:00:00+00
            Instant.parse("2023-07-15T00:00:00Z").toEpochMilli()
        );
    }

    @Test
    public void testAmPmCaseInsensitive() {
        assertEvaluate(
            "to_timestamp('2023-07-15 02:30:45 am', 'YYYY-MM-DD HH12:MI:SS AM')",
            // Verified: PostgreSQL 16 returns 2023-07-15 02:30:45+00
            Instant.parse("2023-07-15T02:30:45Z").toEpochMilli()
        );
        assertEvaluate(
            "to_timestamp('2023-07-15 02:30:45 pm', 'YYYY-MM-DD HH12:MI:SS AM')",
            // Verified: PostgreSQL 16 returns 2023-07-15 14:30:45+00
            Instant.parse("2023-07-15T14:30:45Z").toEpochMilli()
        );
    }

    @Test
    public void testWithPeriodsAmPm() {
        assertEvaluate(
            "to_timestamp('2023-07-15 02:30:45 A.M.', 'YYYY-MM-DD HH12:MI:SS A.M.')",
            // Verified: PostgreSQL 16 returns 2023-07-15 02:30:45+00
            Instant.parse("2023-07-15T02:30:45Z").toEpochMilli()
        );
        assertEvaluate(
            "to_timestamp('2023-07-15 02:30:45 P.M.', 'YYYY-MM-DD HH12:MI:SS P.M.')",
            // Verified: PostgreSQL 16 returns 2023-07-15 14:30:45+00
            Instant.parse("2023-07-15T14:30:45Z").toEpochMilli()
        );
    }

    @Test
    public void testFourDigitYearYYyyFormat() {
        assertEvaluate(
            "to_timestamp('2,023-07-15', 'Y,YYY-MM-DD')",
            // Verified: PostgreSQL 16 returns 2023-07-15 00:00:00+00
            Instant.parse("2023-07-15T00:00:00Z").toEpochMilli()
        );
    }

    @Test
    public void testHourWithoutSuffixDefaultsTo12h() {
        assertEvaluate(
            "to_timestamp('2023-07-15 02:30:45', 'YYYY-MM-DD HH:MI:SS')",
            // Verified: PostgreSQL 16 returns 2023-07-15 02:30:45+00
            Instant.parse("2023-07-15T02:30:45Z").toEpochMilli()
        );
    }

    @Test
    public void testSecondsPastMidnight() {
        assertEvaluate(
            "to_timestamp('2023-07-15 52245', 'YYYY-MM-DD SSSS')",
            // Verified: PostgreSQL 16 returns 2023-07-15 14:30:45+00
            Instant.parse("2023-07-15T14:30:45Z").toEpochMilli()
        );
    }

    @Test
    public void testMicrosecondsParsing() {
        assertEvaluate(
            "to_timestamp('2023-07-15 14:30:45.123456', 'YYYY-MM-DD HH24:MI:SS.US')",
            // DIFFERS: PostgreSQL 16 returns 2023-07-15 14:30:45.123456+00 (full microsecond precision)
            // CrateDB truncates to milliseconds due to TIMESTAMPTZ storage precision
            Instant.parse("2023-07-15T14:30:45.123Z").toEpochMilli()
        );
    }

    @Test
    public void testDayOfYearParsing() {
        assertEvaluate(
            "to_timestamp('2023 196', 'YYYY DDD')",
            // Verified: PostgreSQL 16 returns 2023-07-15 00:00:00+00
            Instant.parse("2023-07-15T00:00:00Z").toEpochMilli()
        );
    }

    @Test
    public void testFlexibleSeparatorMatching() {
        // PostgreSQL allows any separator to match any separator
        assertEvaluate(
            "to_timestamp('2023/07/15', 'YYYY-MM-DD')",
            // Verified: PostgreSQL 16 returns 2023-07-15 00:00:00+00
            Instant.parse("2023-07-15T00:00:00Z").toEpochMilli()
        );
        assertEvaluate(
            "to_timestamp('2023.07.15', 'YYYY-MM-DD')",
            // Verified: PostgreSQL 16 returns 2023-07-15 00:00:00+00
            Instant.parse("2023-07-15T00:00:00Z").toEpochMilli()
        );
        assertEvaluate(
            "to_timestamp('2023 07 15', 'YYYY-MM-DD')",
            // Verified: PostgreSQL 16 returns 2023-07-15 00:00:00+00
            Instant.parse("2023-07-15T00:00:00Z").toEpochMilli()
        );
    }

    @Test
    public void testSkipsLeadingWhitespace() {
        // PostgreSQL skips multiple blanks at start
        assertEvaluate(
            "to_timestamp('  2023-07-15', 'YYYY-MM-DD')",
            // Verified: PostgreSQL 16 returns 2023-07-15 00:00:00+00
            Instant.parse("2023-07-15T00:00:00Z").toEpochMilli()
        );
    }

    @Test
    public void testThreeDigitYearYyy() {
        // YYY parses 3 digits and applies "nearest to 2020" rule
        assertEvaluate(
            "to_timestamp('023-07-15', 'YYY-MM-DD')",
            // Verified: PostgreSQL 16 returns 2023-07-15 00:00:00+00
            Instant.parse("2023-07-15T00:00:00Z").toEpochMilli()
        );
    }

    @Test
    public void testSingleDigitYearY() {
        // Y parses 1 digit and applies "nearest to 2020" rule
        assertEvaluate(
            "to_timestamp('3-07-15', 'Y-MM-DD')",
            // Verified: PostgreSQL 16 returns 2003-07-15 00:00:00+00
            Instant.parse("2003-07-15T00:00:00Z").toEpochMilli()
        );
    }

    @Test
    public void testFf1TenthOfSecond() {
        assertEvaluate(
            "to_timestamp('2023-07-15 14:30:45.1', 'YYYY-MM-DD HH24:MI:SS.FF1')",
            // Verified: PostgreSQL 16 returns 2023-07-15 14:30:45.1+00
            Instant.parse("2023-07-15T14:30:45.100Z").toEpochMilli()
        );
    }

    @Test
    public void testFf2HundredthOfSecond() {
        assertEvaluate(
            "to_timestamp('2023-07-15 14:30:45.12', 'YYYY-MM-DD HH24:MI:SS.FF2')",
            // Verified: PostgreSQL 16 returns 2023-07-15 14:30:45.12+00
            Instant.parse("2023-07-15T14:30:45.120Z").toEpochMilli()
        );
    }

    @Test
    public void testFf3Millisecond() {
        assertEvaluate(
            "to_timestamp('2023-07-15 14:30:45.123', 'YYYY-MM-DD HH24:MI:SS.FF3')",
            // Verified: PostgreSQL 16 returns 2023-07-15 14:30:45.123+00
            Instant.parse("2023-07-15T14:30:45.123Z").toEpochMilli()
        );
    }

    @Test
    public void testFf4TenthOfMillisecond() {
        assertEvaluate(
            "to_timestamp('2023-07-15 14:30:45.1234', 'YYYY-MM-DD HH24:MI:SS.FF4')",
            // DIFFERS: PostgreSQL 16 returns 2023-07-15 14:30:45.1234+00 (4 decimal places)
            // CrateDB truncates to milliseconds due to TIMESTAMPTZ storage precision
            Instant.parse("2023-07-15T14:30:45.123Z").toEpochMilli()
        );
    }

    @Test
    public void testFf5HundredthOfMillisecond() {
        assertEvaluate(
            "to_timestamp('2023-07-15 14:30:45.12345', 'YYYY-MM-DD HH24:MI:SS.FF5')",
            // DIFFERS: PostgreSQL 16 returns 2023-07-15 14:30:45.12345+00 (5 decimal places)
            // CrateDB truncates to milliseconds due to TIMESTAMPTZ storage precision
            Instant.parse("2023-07-15T14:30:45.123Z").toEpochMilli()
        );
    }

    @Test
    public void testFf6Microsecond() {
        assertEvaluate(
            "to_timestamp('2023-07-15 14:30:45.123456', 'YYYY-MM-DD HH24:MI:SS.FF6')",
            // DIFFERS: PostgreSQL 16 returns 2023-07-15 14:30:45.123456+00 (full microsecond precision)
            // CrateDB truncates to milliseconds due to TIMESTAMPTZ storage precision
            Instant.parse("2023-07-15T14:30:45.123Z").toEpochMilli()
        );
    }

    @Test
    public void testSssssSecondsPastMidnightVariant() {
        assertEvaluate(
            "to_timestamp('2023-07-15 52245', 'YYYY-MM-DD SSSSS')",
            // Verified: PostgreSQL 16 returns 2023-07-15 14:30:45+00
            Instant.parse("2023-07-15T14:30:45Z").toEpochMilli()
        );
    }

    @Test
    public void testHour12AtNoonWithPm() {
        // 12 PM should be 12:00, not 24:00
        assertEvaluate(
            "to_timestamp('2023-07-15 12:30:45 PM', 'YYYY-MM-DD HH12:MI:SS PM')",
            // Verified: PostgreSQL 16 returns 2023-07-15 12:30:45+00
            Instant.parse("2023-07-15T12:30:45Z").toEpochMilli()
        );
    }

    @Test
    public void testHour12AtMidnightWithAm() {
        // 12 AM should be 00:00
        assertEvaluate(
            "to_timestamp('2023-07-15 12:30:45 AM', 'YYYY-MM-DD HH12:MI:SS AM')",
            // Verified: PostgreSQL 16 returns 2023-07-15 00:30:45+00
            Instant.parse("2023-07-15T00:30:45Z").toEpochMilli()
        );
    }

    @Test
    public void testIsoYearIyyy() {
        // PostgreSQL 16 rejects mixing ISO year (IYYY) with Gregorian month/day (MM-DD)
        assertThatThrownBy(() -> assertEvaluate("to_timestamp('2023-07-15', 'IYYY-MM-DD')", -1))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("invalid combination of date conventions");
    }

    @Test
    public void testWeekNumberWw() {
        // Week 28 of 2023 starts July 9 (Sunday start)
        assertEvaluate(
            "to_timestamp('2023-28', 'YYYY-WW')",
            // Verified: PostgreSQL 16 returns 2023-07-09 00:00:00+00
            Instant.parse("2023-07-09T00:00:00Z").toEpochMilli()
        );
    }

    @Test
    public void testIsoWeekNumberIw() {
        // PostgreSQL 16 rejects mixing Gregorian year (YYYY) with ISO week (IW)
        assertThatThrownBy(() -> assertEvaluate("to_timestamp('2023-28', 'YYYY-IW')", -1))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("invalid combination of date conventions");
    }

    @Test
    public void testDayOfWeekD() {
        assertEvaluate(
            "to_timestamp('2023-07-15 7', 'YYYY-MM-DD D')",
            // Verified: PostgreSQL 16 returns 2023-07-15 00:00:00+00
            Instant.parse("2023-07-15T00:00:00Z").toEpochMilli()
        );
    }

    @Test
    public void testIsoDayOfWeekId() {
        // PostgreSQL 16 rejects mixing Gregorian date (YYYY-MM-DD) with ISO day (ID)
        assertThatThrownBy(() -> assertEvaluate("to_timestamp('2023-07-15 6', 'YYYY-MM-DD ID')", -1))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("invalid combination of date conventions");
    }

    @Test
    public void testWeekOfMonthW() {
        assertEvaluate(
            "to_timestamp('2023-07-15 3', 'YYYY-MM-DD W')",
            // Verified: PostgreSQL 16 returns 2023-07-15 00:00:00+00
            Instant.parse("2023-07-15T00:00:00Z").toEpochMilli()
        );
    }

    @Test
    public void testCenturyCc() {
        // Century 21 = 2001-2100, first year = 2001
        assertEvaluate(
            "to_timestamp('21-07-15', 'CC-MM-DD')",
            // Verified: PostgreSQL 16 returns 2001-07-15 00:00:00+00
            Instant.parse("2001-07-15T00:00:00Z").toEpochMilli()
        );
    }

    @Test
    public void testQuarterQ() {
        assertEvaluate(
            "to_timestamp('2023-3', 'YYYY-Q')",
            // Verified: PostgreSQL 16 returns 2023-01-01 00:00:00+00 (Q is ignored)
            Instant.parse("2023-01-01T00:00:00Z").toEpochMilli()
        );
    }

    @Test
    public void testEraAd() {
        assertEvaluate(
            "to_timestamp('2023-07-15 AD', 'YYYY-MM-DD AD')",
            // Verified: PostgreSQL 16 returns 2023-07-15 00:00:00+00
            Instant.parse("2023-07-15T00:00:00Z").toEpochMilli()
        );
    }

    @Test
    public void testEraBc() {
        // Year 2023 BC = astronomical year -2022
        assertEvaluate(
            "to_timestamp('2023-07-15 BC', 'YYYY-MM-DD BC')",
            // Verified: PostgreSQL 16 returns 2023-07-15 00:00:00+00 BC (= -2022)
            Instant.parse("-2022-07-15T00:00:00Z").toEpochMilli()
        );
    }

    @Test
    public void testEraWithPeriodsAD() {
        assertEvaluate(
            "to_timestamp('2023-07-15 A.D.', 'YYYY-MM-DD A.D.')",
            // Verified: PostgreSQL 16 returns 2023-07-15 00:00:00+00
            Instant.parse("2023-07-15T00:00:00Z").toEpochMilli()
        );
    }

    @Test
    public void testLowercaseMonthPattern() {
        assertEvaluate(
            "to_timestamp('15 july 2023', 'DD month YYYY')",
            // Verified: PostgreSQL 16 returns 2023-07-15 00:00:00+00
            Instant.parse("2023-07-15T00:00:00Z").toEpochMilli()
        );
    }

    @Test
    public void testUppercaseMonthPattern() {
        assertEvaluate(
            "to_timestamp('15 JULY 2023', 'DD MONTH YYYY')",
            // Verified: PostgreSQL 16 returns 2023-07-15 00:00:00+00
            Instant.parse("2023-07-15T00:00:00Z").toEpochMilli()
        );
    }

    @Test
    public void testLowercaseDayPattern() {
        assertEvaluate(
            "to_timestamp('saturday 2023-07-15', 'day YYYY-MM-DD')",
            // Verified: PostgreSQL 16 returns 2023-07-15 00:00:00+00
            Instant.parse("2023-07-15T00:00:00Z").toEpochMilli()
        );
    }

    @Test
    public void testUppercaseDayPattern() {
        assertEvaluate(
            "to_timestamp('SATURDAY 2023-07-15', 'DAY YYYY-MM-DD')",
            // Verified: PostgreSQL 16 returns 2023-07-15 00:00:00+00
            Instant.parse("2023-07-15T00:00:00Z").toEpochMilli()
        );
    }

    @Test
    public void testLowercaseAbbreviatedMonth() {
        assertEvaluate(
            "to_timestamp('15 jul 2023', 'DD mon YYYY')",
            // Verified: PostgreSQL 16 returns 2023-07-15 00:00:00+00
            Instant.parse("2023-07-15T00:00:00Z").toEpochMilli()
        );
    }

    @Test
    public void testUppercaseAbbreviatedMonth() {
        assertEvaluate(
            "to_timestamp('15 JUL 2023', 'DD MON YYYY')",
            // Verified: PostgreSQL 16 returns 2023-07-15 00:00:00+00
            Instant.parse("2023-07-15T00:00:00Z").toEpochMilli()
        );
    }

    @Test
    public void testLowercaseAbbreviatedDay() {
        assertEvaluate(
            "to_timestamp('sat 2023-07-15', 'dy YYYY-MM-DD')",
            // Verified: PostgreSQL 16 returns 2023-07-15 00:00:00+00
            Instant.parse("2023-07-15T00:00:00Z").toEpochMilli()
        );
    }

    @Test
    public void testUppercaseAbbreviatedDay() {
        assertEvaluate(
            "to_timestamp('SAT 2023-07-15', 'DY YYYY-MM-DD')",
            // Verified: PostgreSQL 16 returns 2023-07-15 00:00:00+00
            Instant.parse("2023-07-15T00:00:00Z").toEpochMilli()
        );
    }

    @Test
    public void testHour24LowercaseHh24() {
        assertEvaluate(
            "to_timestamp('2023-07-15 14:30:45', 'YYYY-MM-DD hh24:MI:SS')",
            // Verified: PostgreSQL 16 returns 2023-07-15 14:30:45+00
            Instant.parse("2023-07-15T14:30:45Z").toEpochMilli()
        );
    }

    @Test
    public void testMinuteLowercaseMi() {
        assertEvaluate(
            "to_timestamp('2023-07-15 14:30:45', 'YYYY-MM-DD HH24:mi:SS')",
            // Verified: PostgreSQL 16 returns 2023-07-15 14:30:45+00
            Instant.parse("2023-07-15T14:30:45Z").toEpochMilli()
        );
    }

    @Test
    public void testSecondLowercaseSs() {
        assertEvaluate(
            "to_timestamp('2023-07-15 14:30:45', 'YYYY-MM-DD HH24:MI:ss')",
            // Verified: PostgreSQL 16 returns 2023-07-15 14:30:45+00
            Instant.parse("2023-07-15T14:30:45Z").toEpochMilli()
        );
    }

    @Test
    public void testMillisecondLowercaseMs() {
        assertEvaluate(
            "to_timestamp('2023-07-15 14:30:45.123', 'YYYY-MM-DD HH24:MI:SS.ms')",
            // Verified: PostgreSQL 16 returns 2023-07-15 14:30:45.123+00
            Instant.parse("2023-07-15T14:30:45.123Z").toEpochMilli()
        );
    }

    @Test
    public void testMicrosecondLowercaseUs() {
        assertEvaluate(
            "to_timestamp('2023-07-15 14:30:45.123456', 'YYYY-MM-DD HH24:MI:SS.us')",
            // DIFFERS: PostgreSQL 16 returns 2023-07-15 14:30:45.123456+00 (full microsecond precision)
            // CrateDB truncates to milliseconds due to TIMESTAMPTZ storage precision
            Instant.parse("2023-07-15T14:30:45.123Z").toEpochMilli()
        );
    }

    @Test
    public void testDayOfYearLowercaseDdd() {
        assertEvaluate(
            "to_timestamp('2023 196', 'YYYY ddd')",
            // Verified: PostgreSQL 16 returns 2023-07-15 00:00:00+00
            Instant.parse("2023-07-15T00:00:00Z").toEpochMilli()
        );
    }

    @Test
    public void testDayOfMonthLowercaseDd() {
        assertEvaluate(
            "to_timestamp('2023-07-15', 'YYYY-MM-dd')",
            // Verified: PostgreSQL 16 returns 2023-07-15 00:00:00+00
            Instant.parse("2023-07-15T00:00:00Z").toEpochMilli()
        );
    }

    @Test
    public void testMonthNumberLowercaseMm() {
        assertEvaluate(
            "to_timestamp('2023-07-15', 'YYYY-mm-DD')",
            // Verified: PostgreSQL 16 returns 2023-07-15 00:00:00+00
            Instant.parse("2023-07-15T00:00:00Z").toEpochMilli()
        );
    }

    @Test
    public void testEmptyInputUsesDefaults() {
        // Empty string should use all defaults (year 0 = 1 BC, month 1, day 1)
        assertEvaluate(
            "to_timestamp('', '')",
            // Verified: PostgreSQL 16 returns 0001-01-01 00:00:00+00 BC (year 0 = 1 BC)
            Instant.parse("0000-01-01T00:00:00Z").toEpochMilli()
        );
    }

    @Test
    public void testIsoDayOfWeekNumberingYearIddd() {
        // PostgreSQL 16 rejects mixing Gregorian year (YYYY) with ISO day of year (IDDD)
        assertThatThrownBy(() -> assertEvaluate("to_timestamp('2023 196', 'YYYY IDDD')", -1))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("invalid combination of date conventions");
    }

    @Test
    public void testLowercasePmMarker() {
        assertEvaluate(
            "to_timestamp('2023-07-15 02:30:45 pm', 'YYYY-MM-DD HH12:MI:SS pm')",
            // Verified: PostgreSQL 16 returns 2023-07-15 14:30:45+00
            Instant.parse("2023-07-15T14:30:45Z").toEpochMilli()
        );
    }

    @Test
    public void testLowercaseAmMarker() {
        assertEvaluate(
            "to_timestamp('2023-07-15 02:30:45 am', 'YYYY-MM-DD HH12:MI:SS am')",
            // Verified: PostgreSQL 16 returns 2023-07-15 02:30:45+00
            Instant.parse("2023-07-15T02:30:45Z").toEpochMilli()
        );
    }

    @Test
    public void testLowercasePeriodsAm() {
        assertEvaluate(
            "to_timestamp('2023-07-15 02:30:45 a.m.', 'YYYY-MM-DD HH12:MI:SS a.m.')",
            // Verified: PostgreSQL 16 returns 2023-07-15 02:30:45+00
            Instant.parse("2023-07-15T02:30:45Z").toEpochMilli()
        );
    }

    @Test
    public void testLowercasePeriodsPm() {
        assertEvaluate(
            "to_timestamp('2023-07-15 02:30:45 p.m.', 'YYYY-MM-DD HH12:MI:SS p.m.')",
            // Verified: PostgreSQL 16 returns 2023-07-15 14:30:45+00
            Instant.parse("2023-07-15T14:30:45Z").toEpochMilli()
        );
    }

    @Test
    public void testIsoYearShortFormatsIyyIyI() {
        // PostgreSQL 16 rejects mixing ISO year patterns (IYY, IY, I) with Gregorian month/day (MM-DD)
        assertThatThrownBy(() -> assertEvaluate("to_timestamp('970-07-15', 'IYY-MM-DD')", -1))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("invalid combination of date conventions");
        assertThatThrownBy(() -> assertEvaluate("to_timestamp('70-07-15', 'IY-MM-DD')", -1))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("invalid combination of date conventions");
        assertThatThrownBy(() -> assertEvaluate("to_timestamp('0-07-15', 'I-MM-DD')", -1))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("invalid combination of date conventions");
    }

    @Test
    public void testJulianDayParsing() {
        // Julian day 2440588 = 1970-01-01
        assertEvaluate(
            "to_timestamp('2440588', 'J')",
            // Verified: PostgreSQL 16 returns 1970-01-01 00:00:00+00
            Instant.parse("1970-01-01T00:00:00Z").toEpochMilli()
        );
        assertEvaluate(
            "to_timestamp('2440588', 'j')",
            // Verified: PostgreSQL 16 returns 1970-01-01 00:00:00+00
            Instant.parse("1970-01-01T00:00:00Z").toEpochMilli()
        );
    }

    @Test
    public void testRomanMonthParsing() {
        // Roman numeral month I = January (default year 0 = 1 BC)
        assertEvaluate(
            "to_timestamp('I', 'RM')",
            // Verified: PostgreSQL 16 returns 0001-01-01 00:00:00+00 BC (year 0)
            Instant.parse("0000-01-01T00:00:00Z").toEpochMilli()
        );
        assertEvaluate(
            "to_timestamp('XII', 'RM')",
            // Verified: PostgreSQL 16 returns 0001-12-01 00:00:00+00 BC (year 0)
            Instant.parse("0000-12-01T00:00:00Z").toEpochMilli()
        );
        assertEvaluate(
            "to_timestamp('IV', 'RM')",
            // Verified: PostgreSQL 16 returns 0001-04-01 00:00:00+00 BC (year 0)
            Instant.parse("0000-04-01T00:00:00Z").toEpochMilli()
        );
        assertEvaluate(
            "to_timestamp('ix', 'rm')",
            // Verified: PostgreSQL 16 returns 0001-09-01 00:00:00+00 BC (year 0)
            Instant.parse("0000-09-01T00:00:00Z").toEpochMilli()
        );
    }

    @Test
    public void testTimezonePatternsAreRejected() {
        // PostgreSQL 16 rejects TZ/tz patterns in to_timestamp
        assertThatThrownBy(() -> assertEvaluate("to_timestamp('2023-07-15 UTC', 'YYYY-MM-DD TZ')", -1))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("formatting field \"TZ\" is only supported in to_char");
        assertThatThrownBy(() -> assertEvaluate("to_timestamp('2023-07-15 utc', 'YYYY-MM-DD tz')", -1))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("formatting field \"tz\" is only supported in to_char");
    }

    @Test
    public void testTimezoneHoursMinutesPatternsAreIgnored() {
        // TZH and TZM patterns are parsed but ignored
        assertEvaluate(
            "to_timestamp('2023-07-15 00', 'YYYY-MM-DD TZH')",
            // Verified: PostgreSQL 16 returns 2023-07-15 00:00:00+00
            Instant.parse("2023-07-15T00:00:00Z").toEpochMilli()
        );
        assertEvaluate(
            "to_timestamp('2023-07-15 00', 'YYYY-MM-DD TZM')",
            // Verified: PostgreSQL 16 returns 2023-07-15 00:00:00+00
            Instant.parse("2023-07-15T00:00:00Z").toEpochMilli()
        );
    }

    @Test
    public void testTimezoneOffsetPatternIsRejected() {
        // PostgreSQL 16 rejects OF pattern in to_timestamp
        assertThatThrownBy(() -> assertEvaluate("to_timestamp('2023-07-15 +00:00', 'YYYY-MM-DD OF')", -1))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("formatting field \"OF\" is only supported in to_char");
        assertThatThrownBy(() -> assertEvaluate("to_timestamp('2023-07-15 +00:00', 'YYYY-MM-DD of')", -1))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("formatting field \"of\" is only supported in to_char");
    }

    @Test
    public void testInputShorterThanPattern() {
        // When input is shorter than pattern, remaining tokens use defaults
        assertEvaluate(
            "to_timestamp('2023', 'YYYY-MM-DD HH24:MI:SS')",
            // Verified: PostgreSQL 16 returns 2023-01-01 00:00:00+00
            Instant.parse("2023-01-01T00:00:00Z").toEpochMilli()
        );
    }

    @Test
    public void testYearWithCommaLowercase() {
        assertEvaluate(
            "to_timestamp('2,023-07-15', 'y,yyy-mm-dd')",
            // Verified: PostgreSQL 16 returns 2023-07-15 00:00:00+00
            Instant.parse("2023-07-15T00:00:00Z").toEpochMilli()
        );
    }

    @Test
    public void testIsoYearLowercaseVariants() {
        // PostgreSQL 16 rejects ISO year patterns alone without ISO week (IW) or ISO day (IDDD)
        // because ISO year cannot determine a specific date without week/day information
        assertThatThrownBy(() -> assertEvaluate("to_timestamp('2023', 'iyyy')", -1))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("invalid combination of date conventions");
        assertThatThrownBy(() -> assertEvaluate("to_timestamp('023', 'iyy')", -1))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("invalid combination of date conventions");
        assertThatThrownBy(() -> assertEvaluate("to_timestamp('23', 'iy')", -1))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("invalid combination of date conventions");
        assertThatThrownBy(() -> assertEvaluate("to_timestamp('3', 'i')", -1))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("invalid combination of date conventions");
    }

    @Test
    public void testDayOfIsoWeekNumberingYearIdddLowercase() {
        // PostgreSQL 16 rejects mixing Gregorian year (YYYY) with ISO day of year (iddd)
        assertThatThrownBy(() -> assertEvaluate("to_timestamp('2023 196', 'YYYY iddd')", -1))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("invalid combination of date conventions");
    }

    @Test
    public void testSecondsPastMidnightLowercaseSsss() {
        assertEvaluate(
            "to_timestamp('2023-07-15 52245', 'YYYY-MM-DD ssss')",
            // Verified: PostgreSQL 16 returns 2023-07-15 14:30:45+00
            Instant.parse("2023-07-15T14:30:45Z").toEpochMilli()
        );
    }

    @Test
    public void testSecondsPastMidnightLowercaseSssss() {
        assertEvaluate(
            "to_timestamp('2023-07-15 52245', 'YYYY-MM-DD sssss')",
            // Verified: PostgreSQL 16 returns 2023-07-15 14:30:45+00
            Instant.parse("2023-07-15T14:30:45Z").toEpochMilli()
        );
    }

}
