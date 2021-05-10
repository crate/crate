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

import io.crate.expression.scalar.ScalarTestCase;
import org.junit.Test;


public class ToCharFunctionPostgresCompatabilityTest extends ScalarTestCase {

    @Test
    public void testPostgresHourOfDayCompatibilityTimestamp() {
        assertEvaluate("to_char(timestamp '1970-01-01T17:31:12.12345', 'HH HH12 HH24')", "05 05 17");
        assertEvaluate("to_char(timestamp '1970-01-01T03:31:12.12345', 'HH HH12 HH24')", "03 03 03");

    }

    @Test
    public void testPostgresMinuteCompatibilityTimestamp() {
        assertEvaluate("to_char(timestamp '1970-01-01T17:05:12.12345', 'MI')", "05");
        assertEvaluate("to_char(timestamp '1970-01-01T17:30:12.12345', 'MI')", "30");
    }

    @Test
    public void testPostgresSecondCompatibilityTimestamp() {
        assertEvaluate("to_char(timestamp '1970-01-01T17:31:05.72345', 'SS')", "05");
        assertEvaluate("to_char(timestamp '1970-01-01T17:31:45.72345', 'SS')", "45");
    }

    @Test
    public void testPostgresMillisecondCompatibilityTimestamp() {
        assertEvaluate("to_char(timestamp '1970-01-01T17:31:12.12345', 'MS')", "123");
        assertEvaluate("to_char(timestamp '1970-01-01T17:31:12.003', 'MS')", "003");
        assertEvaluate("to_char(timestamp '1970-01-01T17:31:12.300', 'MS')", "300");
        assertEvaluate("to_char(timestamp '1970-01-01T17:31:12.0002', 'MS')", "000");
    }

    @Test
    public void testPostgresMicrosecondCompatibilityTimestamp() {
        assertEvaluate("to_char(timestamp '1970-01-01T17:31:12.12345', 'US')", "123000");
        assertEvaluate("to_char(timestamp '1970-01-01T17:31:12.00005', 'US')", "000000");
    }

    @Test
    public void testPostgresFractionOfSecondCompatibilityTimestamp() {
        assertEvaluate("to_char(timestamp '1970-01-01T17:31:12.12345', 'FF1 FF2 FF3 FF4 FF5 FF6')", "1 12 123 1230 12300 123000");
    }

    @Test
    public void testPostgresSecondsPastMidnightCompatibilityTimestamp() {
        assertEvaluate("to_char(timestamp '1970-01-01T17:31:12.12345', 'SSSS, SSSSS')", "63072, 63072");
    }

    @Test
    public void testPostgresMeridiemIndicatorCompatibilityTimestamp() {
        assertEvaluate("to_char(timestamp '1970-01-01T17:31:12.12345', 'AM am PM pm A.M. a.m. P.M. p.m.')", "PM pm PM pm P.M. p.m. P.M. p.m.");
        assertEvaluate("to_char(timestamp '1970-01-01T03:31:12.12345', 'AM am PM pm A.M. a.m. P.M. p.m.')", "AM am AM am A.M. a.m. A.M. a.m.");
    }

    @Test
    public void testPostgresYearCompatibilityTimestamp() {
        assertEvaluate("to_char(timestamp '1970-01-01T17:31:12.12345', 'Y,YYY YYYY YYY YY Y')", "1,970 1970 970 70 0");
    }

    @Test
    public void testPostgresISOYearCompatibilityTimestamp() {
        assertEvaluate("to_char(timestamp '1970-01-01T17:31:12.12345', 'IYYY IYY IY I')", "1970 970 70 0");
        assertEvaluate("to_char(timestamp '1971-01-03T17:31:12.12345', 'IYYY IYY IY I')", "1970 970 70 0");
        assertEvaluate("to_char(timestamp '1971-01-04T17:31:12.12345', 'IYYY IYY IY I')", "1971 971 71 1");

    }

    @Test
    public void testPostgresEraIndicatorCompatibilityTimestamp() {
        assertEvaluate("to_char(timestamp '1970-01-01T17:31:12.12345', 'BC bc AD ad B.C. b.c. A.D. a.d.')", "AD ad AD ad A.D. a.d. A.D. a.d.");
    }

    @Test
    public void testPostgresMonthNameCompatibilityTimestamp() {
        assertEvaluate("to_char(timestamp '1970-01-01T17:31:12.12345', 'MONTH Month month')", "JANUARY January january");
    }

    @Test
    public void testPostgresAbbreviatedMonthNameCompatibilityTimestamp() {
        assertEvaluate("to_char(timestamp '1970-01-01T17:31:12.12345', 'MON Mon mon')", "JAN Jan jan");
    }

    @Test
    public void testPostgresMonthNumberCompatibilityTimestamp() {
        assertEvaluate("to_char(timestamp '1970-01-01T17:31:12.12345', 'MM')", "01");
        assertEvaluate("to_char(timestamp '1970-11-01T17:31:12.12345', 'MM')", "11");
    }

    @Test
    public void testPostgresDayNameCompatibilityTimestamp() {
        assertEvaluate("to_char(timestamp '1970-01-01T17:31:12.12345', 'DAY Day day')", "THURSDAY Thursday thursday");
    }

    @Test
    public void testPostgresAbbreviatedDayNameCompatibilityTimestamp() {
        assertEvaluate("to_char(timestamp '1970-01-01T17:31:12.12345', 'DY Dy dy')", "THU Thu thu");
    }

    @Test
    public void testPostgresDayOfYearCompatibilityTimestamp() {
        assertEvaluate("to_char(timestamp '1970-01-01T17:31:12.12345', 'DDD IDDD')", "001 004");
        assertEvaluate("to_char(timestamp '1970-08-01T17:31:12.12345', 'DDD IDDD')", "213 216");
    }

    @Test
    public void testPostgresDayOfMonthCompatibilityTimestamp() {
        assertEvaluate("to_char(timestamp '1970-01-01T17:31:12.12345', 'DD')", "01");
        assertEvaluate("to_char(timestamp '1970-01-17T17:31:12.12345', 'DD')", "17");
    }

    @Test
    public void testPostgresDayOfWeekCompatibilityTimestamp() {
        assertEvaluate("to_char(timestamp '1970-01-01T17:31:12.12345', 'D ID')", "5 4");
        assertEvaluate("to_char(timestamp '1970-01-04T17:31:12.12345', 'D ID')", "1 7");
        assertEvaluate("to_char(timestamp '1970-01-05T17:31:12.12345', 'D ID')", "2 1");
    }

    @Test
    public void testPostgresWeekOfMonthCompatibilityTimestamp() {
        assertEvaluate("to_char(timestamp '1970-01-15T17:31:12.12345', 'W')", "3");
        assertEvaluate("to_char(timestamp '1970-01-31T17:31:12.12345', 'W')", "5");
    }

    @Test
    public void testPostgresWeekOfYearCompatibilityTimestamp() {
        assertEvaluate("to_char(timestamp '1970-01-01T17:31:12.12345', 'WW IW')", "01 01");
        assertEvaluate("to_char(timestamp '1970-08-01T17:31:12.12345', 'WW IW')", "31 31");
        assertEvaluate("to_char(timestamp '1971-01-01T17:31:12.12345', 'WW IW')", "01 53");

    }

    @Test
    public void testPostgresCenturyCompatibilityTimestamp() {
        assertEvaluate("to_char(timestamp '1970-01-01T17:31:12.12345', 'CC')", "20");
        assertEvaluate("to_char(timestamp '1999-01-01T17:31:12.12345', 'CC')", "20");
        assertEvaluate("to_char(timestamp '2000-01-01T17:31:12.12345', 'CC')", "20");
        assertEvaluate("to_char(timestamp '2001-01-01T17:31:12.12345', 'CC')", "21");
    }

    @Test
    public void testPostgresJulianDayCompatibilityTimestamp() {
        assertEvaluate("to_char(timestamp '1970-01-01T17:31:12.12345', 'J')", "2440588");
        assertEvaluate("to_char(timestamp '2000-01-01T17:31:12.12345', 'J')", "2451545");
        assertEvaluate("to_char(timestamp '2020-01-01T17:31:12.12345', 'J')", "2458850");
    }

    @Test
    public void testPostgresQuarterCompatibilityTimestamp() {
        assertEvaluate("to_char(timestamp '1970-01-01T17:31:12.12345', 'Q')", "1");
        assertEvaluate("to_char(timestamp '1970-05-01T17:31:12.12345', 'Q')", "2");
        assertEvaluate("to_char(timestamp '1970-09-01T17:31:12.12345', 'Q')", "3");
        assertEvaluate("to_char(timestamp '1970-12-01T17:31:12.12345', 'Q')", "4");
    }

    @Test
    public void testPostgresRomanNumeralMonthCompatibilityTimestamp() {
        assertEvaluate("to_char(timestamp '1970-01-01T17:31:12.12345', 'RM rm')", "I    i   ");
        assertEvaluate("to_char(timestamp '1970-04-01T17:31:12.12345', 'RM rm')", "IV   iv  ");
        assertEvaluate("to_char(timestamp '1970-09-01T17:31:12.12345', 'RM rm')", "IX   ix  ");
        assertEvaluate("to_char(timestamp '1970-12-01T17:31:12.12345', 'RM rm')", "XII  xii ");
    }

    @Test
    public void testPostgresTimeZoneAbbreviationCompatibilityTimestamp() {
        assertEvaluate("to_char(timestamp '1970-01-01T17:31:12.12345', 'TZ tz')", " ");
    }

    @Test
    public void testPostgresTimeZoneHoursMinutesCompatibilityTimestamp() {
        assertEvaluate("to_char(timestamp '1970-01-01T17:31:12.12345', 'TZH TZM')", " ");
    }

    @Test
    public void testPostgresTimeZoneOffsetCompatibilityTimestamp() {
        assertEvaluate("to_char(timestamp '1970-01-01T17:31:12.12345', 'OF')", "");
    }

}
