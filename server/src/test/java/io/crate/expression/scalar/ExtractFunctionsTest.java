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

import java.util.Locale;

import org.junit.Test;

import io.crate.exceptions.UnsupportedFunctionException;
import io.crate.expression.symbol.Literal;
import io.crate.types.DataTypes;

public class ExtractFunctionsTest extends ScalarTestCase {

    private static final String D_2014_02_15___21_33_23 = "2014-02-15T21:33:23";

    private void assertEvaluate(String functionExpression, Object expected) {
        assertEvaluate(functionExpression, expected,
            Literal.of(
                DataTypes.TIMESTAMPZ,
                DataTypes.TIMESTAMPZ.implicitCast(D_2014_02_15___21_33_23)
            )
        );
    }

    private void assertEvaluateIntervalException(String timeUnit) {
        assertThatThrownBy(() -> assertEvaluate("extract(" + timeUnit + " from INTERVAL '10 days')", -1))
            .isExactlyInstanceOf(UnsupportedFunctionException.class)
            .hasMessageStartingWith("Invalid arguments in: extract(" + timeUnit.toUpperCase(Locale.ENGLISH) + " FROM");
    }

    @Test
    public void testNullValue() {
        assertEvaluateNull("extract(day from timestamp_tz)", Literal.of(DataTypes.TIMESTAMPZ, null));
        assertEvaluateNull("extract(year from interval)", Literal.of(DataTypes.INTERVAL, null));
    }

    @Test
    public void testExtractCentury() throws Exception {
        // ISO century, see http://joda-time.sourceforge.net/field.html
        assertEvaluate("extract(century from timestamp_tz)", 20);
        assertEvaluateIntervalException("century");
    }

    @Test
    public void testExtractCenturyFromTimestampWithoutTimeZone() {
        assertEvaluate(
            "extract(day from timestamp)",
            25,
            Literal.of(
                DataTypes.TIMESTAMP,
                DataTypes.TIMESTAMP.implicitCast("2014-03-25")
            )
        );
    }

    @Test
    public void testExtractYear() throws Exception {
        assertEvaluate("extract(year from timestamp_tz)", 2014);
        assertEvaluate("extract(year from INTERVAL '1250 days 49 hours')", 0);
        assertEvaluate("extract(year from INTERVAL '14 years 1250 days 49 hours')", 14);
    }

    @Test
    public void testExtractQuarter() throws Exception {
        assertEvaluate("extract(quarter from timestamp_tz)", 1);
        assertEvaluate("extract(quarter from INTERVAL '14 years 58 months 1250 days 49 hours')", 2);
        assertEvaluate("extract(quarter from INTERVAL '8 months 1250 days 49 hours')", 2);
    }

    @Test
    public void testExtractMonth() throws Exception {
        assertEvaluate("extract(month from timestamp_tz)", 2);
        assertEvaluate("extract(month from INTERVAL '1250 days 49 hours')", 0);
        assertEvaluate("extract(month from INTERVAL '14 years 58 months 1250 days 49 hours')", 10);
    }

    @Test
    public void testExtractWeek() throws Exception {
        assertEvaluate("extract(week from timestamp_tz)", 7);
        assertEvaluateIntervalException("week");
    }

    @Test
    public void testExtractDay() throws Exception {
        assertEvaluate("extract(day from timestamp_tz)", 15);
        assertEvaluate("extract(day_of_month from timestamp_tz)", 15);
        assertEvaluate("extract(day from INTERVAL '14 years 58 months 1250 days 49 hours' DAY TO HOUR)", 1252);
        assertEvaluate("extract(day from INTERVAL '49 hours 127 minutes 43250 seconds')", 2);
        assertEvaluateIntervalException("day_of_month");
    }

    @Test
    public void testDayOfWeek() throws Exception {
        assertEvaluate("extract(day_of_week from timestamp_tz)", 6);
        assertEvaluate("extract(dow from timestamp_tz)", 6);
        assertEvaluateIntervalException("day_of_week");
    }

    @Test
    public void testDayOfYear() throws Exception {
        assertEvaluate("extract(day_of_year from timestamp_tz)", 46);
        assertEvaluate("extract(doy from timestamp_tz)", 46);
        assertEvaluateIntervalException("day_of_year");
    }

    @Test
    public void testHour() throws Exception {
        assertEvaluate("extract(hour from timestamp_tz)", 21);
        assertEvaluate("extract(hour from INTERVAL '14 years 58 months 1250 days 49 hours')", 1);
        assertEvaluate("extract(hour from INTERVAL '49 hours 127 minutes 43250 seconds' HOUR TO SECOND)", 15);
    }

    @Test
    public void testMinute() throws Exception {
        assertEvaluate("extract(minute from timestamp_tz)", 33);
        assertEvaluate("extract(minute from INTERVAL '12 years 46 months 1250 days 49 hours 127 minutes 43250 seconds')", 7);
        assertEvaluate("extract(minute from INTERVAL '49 hours 127 minutes 43250 seconds' DAY TO MINUTE)", 7);
    }

    @Test
    public void testSecond() throws Exception {
        assertEvaluate("extract(second from timestamp_tz)", 23);
        assertEvaluate("extract(second from INTERVAL '12 years 46 months 1250 days 49 hours 127 minutes 43250 seconds')", 50);
        assertEvaluate("extract(second from INTERVAL '49 hours 127 minutes 43250 seconds' DAY TO MINUTE)", 0);
    }

    @Test
    public void testExtractEpoch() {
        assertEvaluate("extract(epoch from '1970-01-01T00:00:01')", 1.0);
        assertEvaluate("extract(epoch from '1970-01-01T00:00:00.5')", 0.5);
        assertEvaluate("extract(epoch from '1970-01-01T00:00:00')", 0.0);
        assertEvaluate("extract(epoch from '1969-12-31T23:59:59')", -1.0);
        assertEvaluate("extract(epoch from timestamp_tz)", 1392500003.0);

        assertEvaluate("extract(epoch from INTERVAL '1025 days 29 hours 137 minutes 72 seconds')", 88672692.0d);
        assertEvaluate("extract(epoch from INTERVAL '21:47:36')", 78456.0d);
        assertEvaluate("extract(epoch from INTERVAL '7 years 11 months 18 days 11 hours')", 251010000.0d);
        assertEvaluate("extract(epoch from " +
                       "'1970-01-01T00:00:01.789'::timestamp - '1970-01-01T00:00:00.123'::timestamp)", 1.666d);
    }
}
