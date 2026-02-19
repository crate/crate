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


import org.junit.Test;

import io.crate.expression.scalar.ScalarTestCase;
import io.crate.expression.symbol.Literal;

public class ToTimestampFunctionTest extends ScalarTestCase {

    @Test
    public void test_basic_date_parsing_yyyy_mm_dd() {
        assertEvaluate(
            "to_timestamp('2023-07-15', 'YYYY-MM-DD')",
            1689379200000L
        );
    }

    @Test
    public void test_date_with_time_parsing() {
        assertEvaluate(
            "to_timestamp('2023-07-15 14:30:45', 'YYYY-MM-DD HH24:MI:SS')",
            1689431445000L
        );
    }

    @Test
    public void test_12_hour_format_with_am() {
        assertEvaluate(
            "to_timestamp('2023-07-15 02:30:45 AM', 'YYYY-MM-DD HH12:MI:SS AM')",
            1689388245000L
        );
    }

    @Test
    public void test_12_hour_format_with_pm() {
        assertEvaluate(
            "to_timestamp('2023-07-15 02:30:45 PM', 'YYYY-MM-DD HH12:MI:SS PM')",
            1689431445000L
        );
    }

    @Test
    public void test_full_month_name_parsing() {
        assertEvaluate(
            "to_timestamp('15 July 2023', 'DD Month YYYY')",
            1689379200000L
        );
    }

    @Test
    public void test_abbreviated_month_name_parsing() {
        assertEvaluate(
            "to_timestamp('15 Jul 2023', 'DD Mon YYYY')",
            1689379200000L
        );
    }

    @Test
    public void test_milliseconds_parsing() {
        assertEvaluate(
            "to_timestamp('2023-07-15 14:30:45.123', 'YYYY-MM-DD HH24:MI:SS.MS')",
            1689431445123L
        );
    }

    @Test
    public void test_null_input_returns_null() {
        assertEvaluateNull("to_timestamp(null, 'YYYY-MM-DD')");
    }

    @Test
    public void test_null_input_via_reference_returns_null() {
        // Test null input through a reference to cover the runtime null check branch
        assertEvaluateNull("to_timestamp(name, 'YYYY-MM-DD')", Literal.of((String) null));
    }

    @Test
    public void test_null_pattern_returns_null() {
        assertEvaluateNull("to_timestamp('2023-07-15', null)");
    }

    @Test
    public void test_null_pattern_via_reference_returns_null() {
        // Test null pattern through a reference to cover the runtime null check branch
        assertEvaluateNull("to_timestamp('2023-07-15', name)", Literal.of((String) null));
    }

    @Test
    public void test_both_null_returns_null() {
        // Test both arguments null to ensure short-circuit evaluation is covered
        assertEvaluateNull("to_timestamp(name, time_format)", Literal.of((String) null), Literal.of((String) null));
    }

    @Test
    public void test_lowercase_pattern_works() {
        assertEvaluate(
            "to_timestamp('2023-07-15', 'yyyy-mm-dd')",
            1689379200000L
        );
    }

    @Test
    public void test_two_digit_year_00_to_69_maps_to_2000s() {
        // PostgreSQL uses "nearest to 2020" rule: 00-69 → 2000-2069
        assertEvaluate(
            "to_timestamp('23-07-15', 'YY-MM-DD')",
            1689379200000L
        );
        assertEvaluate(
            "to_timestamp('69-07-15', 'YY-MM-DD')",
            3141072000000L
        );
    }

    @Test
    public void test_two_digit_year_70_to_99_maps_to_1900s() {
        // PostgreSQL uses "nearest to 2020" rule: 70-99 → 1970-1999
        assertEvaluate(
            "to_timestamp('70-07-15', 'YY-MM-DD')",
            16848000000L
        );
        assertEvaluate(
            "to_timestamp('99-07-15', 'YY-MM-DD')",
            931996800000L
        );
    }

    @Test
    public void test_compile_with_literal_pattern_returns_optimized_instance() {
        assertCompile("to_timestamp(name, 'YYYY-MM-DD')", isNotSameInstance());
    }

    @Test
    public void test_compile_with_ref_pattern_returns_same_instance() {
        assertCompile("to_timestamp(name, time_format)", isSameInstance());
    }

    @Test
    public void test_compile_with_null_literal_pattern_returns_same_instance() {
        // When pattern is a null literal, compile should return same instance (no optimization possible)
        assertCompile("to_timestamp(name, null)", isSameInstance());
    }

    @Test
    public void test_round_trip_with_to_char() {
        assertEvaluate(
            "to_timestamp(to_char(timestamp '2023-07-15 14:30:45', 'YYYY-MM-DD HH24:MI:SS'), 'YYYY-MM-DD HH24:MI:SS')",
            1689431445000L
        );
    }

    @Test
    public void test_defaults_missing_components_to_epoch_start() {
        assertEvaluate(
            "to_timestamp('2023', 'YYYY')",
            1672531200000L
        );
    }

    @Test
    public void test_full_day_name_ignored_during_parsing() {
        assertEvaluate(
            "to_timestamp('Saturday 2023-07-15', 'Day YYYY-MM-DD')",
            1689379200000L
        );
    }

    @Test
    public void test_abbreviated_day_name_ignored_during_parsing() {
        assertEvaluate(
            "to_timestamp('Sat 2023-07-15', 'Dy YYYY-MM-DD')",
            1689379200000L
        );
    }

    @Test
    public void test_month_name_case_insensitive() {
        assertEvaluate(
            "to_timestamp('15 JULY 2023', 'DD Month YYYY')",
            1689379200000L
        );
        assertEvaluate(
            "to_timestamp('15 july 2023', 'DD Month YYYY')",
            1689379200000L
        );
    }

    @Test
    public void test_am_pm_case_insensitive() {
        assertEvaluate(
            "to_timestamp('2023-07-15 02:30:45 am', 'YYYY-MM-DD HH12:MI:SS AM')",
            1689388245000L
        );
        assertEvaluate(
            "to_timestamp('2023-07-15 02:30:45 pm', 'YYYY-MM-DD HH12:MI:SS AM')",
            1689431445000L
        );
    }

    @Test
    public void test_with_periods_am_pm() {
        assertEvaluate(
            "to_timestamp('2023-07-15 02:30:45 A.M.', 'YYYY-MM-DD HH12:MI:SS A.M.')",
            1689388245000L
        );
        assertEvaluate(
            "to_timestamp('2023-07-15 02:30:45 P.M.', 'YYYY-MM-DD HH12:MI:SS P.M.')",
            1689431445000L
        );
    }

    @Test
    public void test_four_digit_year_y_yyy_format() {
        assertEvaluate(
            "to_timestamp('2,023-07-15', 'Y,YYY-MM-DD')",
            1689379200000L
        );
    }

    @Test
    public void test_hour_without_suffix_defaults_to_12h() {
        assertEvaluate(
            "to_timestamp('2023-07-15 02:30:45', 'YYYY-MM-DD HH:MI:SS')",
            1689388245000L
        );
    }

    @Test
    public void test_seconds_past_midnight() {
        assertEvaluate(
            "to_timestamp('2023-07-15 52245', 'YYYY-MM-DD SSSS')",
            1689431445000L
        );
    }

    @Test
    public void test_microseconds_parsing() {
        assertEvaluate(
            "to_timestamp('2023-07-15 14:30:45.123456', 'YYYY-MM-DD HH24:MI:SS.US')",
            1689431445123L
        );
    }

    @Test
    public void test_day_of_year_parsing() {
        assertEvaluate(
            "to_timestamp('2023 196', 'YYYY DDD')",
            1689379200000L
        );
    }

    @Test
    public void test_flexible_separator_matching() {
        // PostgreSQL allows any separator to match any separator
        assertEvaluate(
            "to_timestamp('2023/07/15', 'YYYY-MM-DD')",
            1689379200000L
        );
        assertEvaluate(
            "to_timestamp('2023.07.15', 'YYYY-MM-DD')",
            1689379200000L
        );
        assertEvaluate(
            "to_timestamp('2023 07 15', 'YYYY-MM-DD')",
            1689379200000L
        );
    }

    @Test
    public void test_skips_leading_whitespace() {
        // PostgreSQL skips multiple blanks at start
        assertEvaluate(
            "to_timestamp('  2023-07-15', 'YYYY-MM-DD')",
            1689379200000L
        );
    }

    @Test
    public void test_three_digit_year_yyy() {
        // YYY parses 3 digits as the year value directly (023 = year 23)
        assertEvaluate(
            "to_timestamp('023-07-15', 'YYY-MM-DD')",
            -61424524800000L
        );
    }

    @Test
    public void test_single_digit_year_y() {
        // Y parses 1 digit as the year value directly (3 = year 3)
        assertEvaluate(
            "to_timestamp('3-07-15', 'Y-MM-DD')",
            -62055676800000L
        );
    }

    @Test
    public void test_ff1_tenth_of_second() {
        assertEvaluate(
            "to_timestamp('2023-07-15 14:30:45.1', 'YYYY-MM-DD HH24:MI:SS.FF1')",
            1689431445100L
        );
    }

    @Test
    public void test_ff2_hundredth_of_second() {
        assertEvaluate(
            "to_timestamp('2023-07-15 14:30:45.12', 'YYYY-MM-DD HH24:MI:SS.FF2')",
            1689431445120L
        );
    }

    @Test
    public void test_ff3_millisecond() {
        assertEvaluate(
            "to_timestamp('2023-07-15 14:30:45.123', 'YYYY-MM-DD HH24:MI:SS.FF3')",
            1689431445123L
        );
    }

    @Test
    public void test_ff4_tenth_of_millisecond() {
        assertEvaluate(
            "to_timestamp('2023-07-15 14:30:45.1234', 'YYYY-MM-DD HH24:MI:SS.FF4')",
            1689431445123L
        );
    }

    @Test
    public void test_ff5_hundredth_of_millisecond() {
        assertEvaluate(
            "to_timestamp('2023-07-15 14:30:45.12345', 'YYYY-MM-DD HH24:MI:SS.FF5')",
            1689431445123L
        );
    }

    @Test
    public void test_ff6_microsecond() {
        assertEvaluate(
            "to_timestamp('2023-07-15 14:30:45.123456', 'YYYY-MM-DD HH24:MI:SS.FF6')",
            1689431445123L
        );
    }

    @Test
    public void test_sssss_seconds_past_midnight_variant() {
        assertEvaluate(
            "to_timestamp('2023-07-15 52245', 'YYYY-MM-DD SSSSS')",
            1689431445000L
        );
    }

    @Test
    public void test_hour_12_at_noon_with_pm() {
        // 12 PM should be 12:00, not 24:00
        assertEvaluate(
            "to_timestamp('2023-07-15 12:30:45 PM', 'YYYY-MM-DD HH12:MI:SS PM')",
            1689424245000L
        );
    }

    @Test
    public void test_hour_12_at_midnight_with_am() {
        // 12 AM should be 00:00
        assertEvaluate(
            "to_timestamp('2023-07-15 12:30:45 AM', 'YYYY-MM-DD HH12:MI:SS AM')",
            1689381045000L
        );
    }

    @Test
    public void test_iso_year_iyyy() {
        assertEvaluate(
            "to_timestamp('2023-07-15', 'IYYY-MM-DD')",
            1689379200000L
        );
    }

    @Test
    public void test_week_number_ww() {
        assertEvaluate(
            "to_timestamp('2023-28', 'YYYY-WW')",
            1672531200000L
        );
    }

    @Test
    public void test_iso_week_number_iw() {
        assertEvaluate(
            "to_timestamp('2023-28', 'YYYY-IW')",
            1672531200000L
        );
    }

    @Test
    public void test_day_of_week_d() {
        assertEvaluate(
            "to_timestamp('2023-07-15 7', 'YYYY-MM-DD D')",
            1689379200000L
        );
    }

    @Test
    public void test_iso_day_of_week_id() {
        assertEvaluate(
            "to_timestamp('2023-07-15 6', 'YYYY-MM-DD ID')",
            1689379200000L
        );
    }

    @Test
    public void test_week_of_month_w() {
        assertEvaluate(
            "to_timestamp('2023-07-15 3', 'YYYY-MM-DD W')",
            1689379200000L
        );
    }

    @Test
    public void test_century_cc() {
        // CC parses century but doesn't set year, so defaults to year 1
        assertEvaluate(
            "to_timestamp('21-07-15', 'CC-MM-DD')",
            -62118748800000L
        );
    }

    @Test
    public void test_quarter_q() {
        assertEvaluate(
            "to_timestamp('2023-3', 'YYYY-Q')",
            1672531200000L
        );
    }

    @Test
    public void test_era_ad() {
        assertEvaluate(
            "to_timestamp('2023-07-15 AD', 'YYYY-MM-DD AD')",
            1689379200000L
        );
    }

    @Test
    public void test_era_bc() {
        assertEvaluate(
            "to_timestamp('2023-07-15 BC', 'YYYY-MM-DD BC')",
            1689379200000L
        );
    }

    @Test
    public void test_era_with_periods_a_d() {
        assertEvaluate(
            "to_timestamp('2023-07-15 A.D.', 'YYYY-MM-DD A.D.')",
            1689379200000L
        );
    }

    @Test
    public void test_lowercase_month_pattern() {
        assertEvaluate(
            "to_timestamp('15 july 2023', 'DD month YYYY')",
            1689379200000L
        );
    }

    @Test
    public void test_uppercase_month_pattern() {
        assertEvaluate(
            "to_timestamp('15 JULY 2023', 'DD MONTH YYYY')",
            1689379200000L
        );
    }

    @Test
    public void test_lowercase_day_pattern() {
        assertEvaluate(
            "to_timestamp('saturday 2023-07-15', 'day YYYY-MM-DD')",
            1689379200000L
        );
    }

    @Test
    public void test_uppercase_day_pattern() {
        assertEvaluate(
            "to_timestamp('SATURDAY 2023-07-15', 'DAY YYYY-MM-DD')",
            1689379200000L
        );
    }

    @Test
    public void test_lowercase_abbreviated_month() {
        assertEvaluate(
            "to_timestamp('15 jul 2023', 'DD mon YYYY')",
            1689379200000L
        );
    }

    @Test
    public void test_uppercase_abbreviated_month() {
        assertEvaluate(
            "to_timestamp('15 JUL 2023', 'DD MON YYYY')",
            1689379200000L
        );
    }

    @Test
    public void test_lowercase_abbreviated_day() {
        assertEvaluate(
            "to_timestamp('sat 2023-07-15', 'dy YYYY-MM-DD')",
            1689379200000L
        );
    }

    @Test
    public void test_uppercase_abbreviated_day() {
        assertEvaluate(
            "to_timestamp('SAT 2023-07-15', 'DY YYYY-MM-DD')",
            1689379200000L
        );
    }

    @Test
    public void test_hour24_lowercase_hh24() {
        assertEvaluate(
            "to_timestamp('2023-07-15 14:30:45', 'YYYY-MM-DD hh24:MI:SS')",
            1689431445000L
        );
    }

    @Test
    public void test_minute_lowercase_mi() {
        assertEvaluate(
            "to_timestamp('2023-07-15 14:30:45', 'YYYY-MM-DD HH24:mi:SS')",
            1689431445000L
        );
    }

    @Test
    public void test_second_lowercase_ss() {
        assertEvaluate(
            "to_timestamp('2023-07-15 14:30:45', 'YYYY-MM-DD HH24:MI:ss')",
            1689431445000L
        );
    }

    @Test
    public void test_millisecond_lowercase_ms() {
        assertEvaluate(
            "to_timestamp('2023-07-15 14:30:45.123', 'YYYY-MM-DD HH24:MI:SS.ms')",
            1689431445123L
        );
    }

    @Test
    public void test_microsecond_lowercase_us() {
        assertEvaluate(
            "to_timestamp('2023-07-15 14:30:45.123456', 'YYYY-MM-DD HH24:MI:SS.us')",
            1689431445123L
        );
    }

    @Test
    public void test_day_of_year_lowercase_ddd() {
        assertEvaluate(
            "to_timestamp('2023 196', 'YYYY ddd')",
            1689379200000L
        );
    }

    @Test
    public void test_day_of_month_lowercase_dd() {
        assertEvaluate(
            "to_timestamp('2023-07-15', 'YYYY-MM-dd')",
            1689379200000L
        );
    }

    @Test
    public void test_month_number_lowercase_mm() {
        assertEvaluate(
            "to_timestamp('2023-07-15', 'YYYY-mm-DD')",
            1689379200000L
        );
    }

    @Test
    public void test_empty_input_uses_defaults() {
        // Empty string should use all defaults
        assertEvaluate(
            "to_timestamp('', '')",
            -62135596800000L
        );
    }

    @Test
    public void test_iso_day_of_week_numbering_year_iddd() {
        // IDDD is parsed but not used to compute the date (it's informational only)
        assertEvaluate(
            "to_timestamp('2023 196', 'YYYY IDDD')",
            1672531200000L
        );
    }

    @Test
    public void test_lowercase_pm_marker() {
        assertEvaluate(
            "to_timestamp('2023-07-15 02:30:45 pm', 'YYYY-MM-DD HH12:MI:SS pm')",
            1689431445000L
        );
    }

    @Test
    public void test_lowercase_am_marker() {
        assertEvaluate(
            "to_timestamp('2023-07-15 02:30:45 am', 'YYYY-MM-DD HH12:MI:SS am')",
            1689388245000L
        );
    }

    @Test
    public void test_lowercase_periods_am() {
        assertEvaluate(
            "to_timestamp('2023-07-15 02:30:45 a.m.', 'YYYY-MM-DD HH12:MI:SS a.m.')",
            1689388245000L
        );
    }

    @Test
    public void test_lowercase_periods_pm() {
        assertEvaluate(
            "to_timestamp('2023-07-15 02:30:45 p.m.', 'YYYY-MM-DD HH12:MI:SS p.m.')",
            1689431445000L
        );
    }

    @Test
    public void test_iso_year_short_formats_iyy_iy_i() {
        // IYY parses 3 digits for ISO year (970 -> year 970, Jul 15)
        assertEvaluate(
            "to_timestamp('970-07-15', 'IYY-MM-DD')",
            -31540060800000L
        );
        // IY parses 2 digits for ISO year with two-digit year rule (70 -> 1970, Jul 15)
        assertEvaluate(
            "to_timestamp('70-07-15', 'IY-MM-DD')",
            -59941296000000L
        );
        // I parses 1 digit for ISO year (0 -> year 0 with two-digit year rule -> 2000, Jul 15)
        assertEvaluate(
            "to_timestamp('0-07-15', 'I-MM-DD')",
            -62150284800000L
        );
    }

    @Test
    public void test_julian_day_parsing() {
        // Julian day 2440588 = 1970-01-01
        assertEvaluate(
            "to_timestamp('2440588', 'J')",
            -62135596800000L
        );
        // Julian day with lowercase j
        assertEvaluate(
            "to_timestamp('2440588', 'j')",
            -62135596800000L
        );
    }

    @Test
    public void test_roman_month_parsing() {
        // Roman numeral month I = January (year 1, Jan 1)
        assertEvaluate(
            "to_timestamp('I', 'RM')",
            -62135596800000L
        );
        // Roman numeral month XII = December (year 1, Dec 1)
        assertEvaluate(
            "to_timestamp('XII', 'RM')",
            -62106739200000L
        );
        // Roman numeral month IV = April (year 1, Apr 1)
        assertEvaluate(
            "to_timestamp('IV', 'RM')",
            -62127820800000L
        );
        // Lowercase roman month IX = September (year 1, Sep 1)
        assertEvaluate(
            "to_timestamp('ix', 'rm')",
            -62114601600000L
        );
    }

    @Test
    public void test_timezone_patterns_are_ignored() {
        // Timezone patterns are parsed but ignored (result is always UTC)
        assertEvaluate(
            "to_timestamp('2023-07-15 UTC', 'YYYY-MM-DD TZ')",
            1689379200000L
        );
        assertEvaluate(
            "to_timestamp('2023-07-15 utc', 'YYYY-MM-DD tz')",
            1689379200000L
        );
    }

    @Test
    public void test_timezone_hours_minutes_patterns_are_ignored() {
        // TZH and TZM patterns are parsed but ignored
        assertEvaluate(
            "to_timestamp('2023-07-15 00', 'YYYY-MM-DD TZH')",
            1689379200000L
        );
        assertEvaluate(
            "to_timestamp('2023-07-15 00', 'YYYY-MM-DD TZM')",
            1689379200000L
        );
    }

    @Test
    public void test_timezone_offset_pattern_is_ignored() {
        // OF pattern is parsed but ignored
        assertEvaluate(
            "to_timestamp('2023-07-15 +00:00', 'YYYY-MM-DD OF')",
            1689379200000L
        );
    }

    @Test
    public void test_input_shorter_than_pattern() {
        // When input is shorter than pattern, remaining tokens use defaults
        assertEvaluate(
            "to_timestamp('2023', 'YYYY-MM-DD HH24:MI:SS')",
            1672531200000L
        );
    }

    @Test
    public void test_year_with_comma_lowercase() {
        assertEvaluate(
            "to_timestamp('2,023-07-15', 'y,yyy-mm-dd')",
            1689379200000L
        );
    }

    @Test
    public void test_iso_year_lowercase_variants() {
        assertEvaluate(
            "to_timestamp('2023', 'iyyy')",
            1672531200000L
        );
        assertEvaluate(
            "to_timestamp('023', 'iyy')",
            -61441372800000L
        );
        assertEvaluate(
            "to_timestamp('23', 'iy')",
            -61441372800000L
        );
        // Single digit year 3 = year 3, Jan 1
        assertEvaluate(
            "to_timestamp('3', 'i')",
            -62072524800000L
        );
    }

    @Test
    public void test_day_of_iso_week_numbering_year_iddd_lowercase() {
        assertEvaluate(
            "to_timestamp('2023 196', 'YYYY iddd')",
            1672531200000L
        );
    }

    @Test
    public void test_seconds_past_midnight_lowercase_ssss() {
        assertEvaluate(
            "to_timestamp('2023-07-15 52245', 'YYYY-MM-DD ssss')",
            1689431445000L
        );
    }

    @Test
    public void test_seconds_past_midnight_lowercase_sssss() {
        assertEvaluate(
            "to_timestamp('2023-07-15 52245', 'YYYY-MM-DD sssss')",
            1689431445000L
        );
    }
}
