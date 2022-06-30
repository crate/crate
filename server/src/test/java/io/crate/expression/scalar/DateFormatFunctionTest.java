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
import io.crate.expression.symbol.Symbol;
import io.crate.types.DataTypes;
import org.junit.Test;

import java.util.Locale;
import java.util.Map;

import static io.crate.testing.SymbolMatchers.isLiteral;


public class DateFormatFunctionTest extends ScalarTestCase {

    @Test
    public void testNormalizeDefault() throws Exception {
        assertNormalize("date_format('1970-01-01T00:00:00')", isLiteral("1970-01-01T00:00:00.000000Z"));
    }

    @Test
    public void testNormalizeDefaultTimezone() throws Exception {
        assertNormalize("date_format('%d.%m.%Y', '1970-02-01')", isLiteral("01.02.1970"));
    }

    @Test
    public void testNormalizeWithTimezone() throws Exception {
        assertNormalize("date_format('%d.%m.%Y %T', 'Europe/Rome', '1970-01-01T00:00:00')", isLiteral("01.01.1970 01:00:00"));
    }

    @Test
    public void testEvaluateWithNullValues() {
        assertEvaluate("date_format('%d.%m.%Y %H:%i:%S', timestamp_tz)", null, Literal.of(DataTypes.TIMESTAMPZ, null));
        assertEvaluate("date_format(name, 'Europe/Berlin', 0)", null, Literal.of(DataTypes.STRING, null));
    }

    @Test
    public void testEvaluateDefault() {
        assertEvaluate(
            "date_format(timestamp_tz)", "2015-06-10T07:03:00.004000Z",
            Literal.of(
                DataTypes.TIMESTAMPZ,
                DataTypes.TIMESTAMPZ.implicitCast("2015-06-10T09:03:00.004+02")
            )
        );
    }

    @Test
    public void testEvaluateDefaultTimezone() {
        assertEvaluate("date_format(time_format, timestamp_tz)",
            "Fri Jan 1 1st 01 1 000000 00 12 12 00 001 0 12 January 01 AM 12:00:00 AM " +
                         "00 00 00:00:00 00 00 52 53 Friday 5 2054 2054 2055 55",
            Literal.of("%a %b %c %D %d %e %f %H %h %I %i %j %k %l %M %m %p %r " +
                       "%S %s %T %U %u %V %v %W %w %X %x %Y %y"),
            Literal.of(DataTypes.TIMESTAMPZ, DataTypes.TIMESTAMPZ.implicitCast("2055-01-01")
            )
        );
    }

    @Test
    public void testEvaluateWithTimezone() {
        assertEvaluate("date_format(time_format, timezone, timestamp_tz)",
            "Sun Jan 1 1st 01 1 000000 03 03 03 00 001 3 3 January 01 AM 03:00:00 AM " +
                         "00 00 03:00:00 01 00 01 52 Sunday 0 1871 1870 1871 71",
            Literal.of("%a %b %c %D %d %e %f %H %h %I %i %j %k %l %M %m %p %r " +
                       "%S %s %T %U %u %V %v %W %w %X %x %Y %y"),
            Literal.of("EST"),
            Literal.of(
                DataTypes.TIMESTAMPZ,
                DataTypes.TIMESTAMPZ.implicitCast("1871-01-01T09:00:00.000+01")
            )
        );
    }

    @Test
    public void testNullInputs() {
        assertEvaluate("date_format(time_format, timezone, timestamp_tz)",
            null,
            Literal.of("%d.%m.%Y %H:%i:%S"),
            Literal.of(DataTypes.STRING, null),
            Literal.of(DataTypes.TIMESTAMPZ, null));
        assertEvaluate("date_format(time_format, timezone, timestamp_tz)",
            null,
            Literal.of(DataTypes.STRING, null),
            Literal.of("Europe/Berlin"),
            Literal.of(DataTypes.TIMESTAMPZ, 0L));
    }

    @Test
    public void testEvaluateTimestampWithoutTimezone() {
        assertEvaluate(
            "date_format(time_format, timezone, timestamp)",
            "2015-06-10 09:03:00 Z",
            Literal.of("%Y-%m-%d %H:%i:%S %Z"),
            Literal.of("Europe/Berlin"),
            Literal.of(
                DataTypes.TIMESTAMP,
                DataTypes.TIMESTAMP.implicitCast("2015-06-10T07:03:00+10")
            )
        );
        assertEvaluate(
            "date_format(time_format, timestamp)",
            "2015-06-10 07:03:00 Z",
            Literal.of("%Y-%m-%d %H:%i:%S %Z"),
            Literal.of(
                DataTypes.TIMESTAMP,
                DataTypes.TIMESTAMP.implicitCast("2015-06-10T07:03:00+10")
            )
        );
    }

    @Test
    public void testInvalidTimeZone() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("invalid time zone value 'wrong timezone'");
        assertEvaluate("date_format('%d.%m.%Y', 'wrong timezone', 0)", null);
    }

    @Test
    public void testInvalidTimestamp() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Cannot cast `'NO TIMESTAMP'` of type `text` to type `timestamp with time zone`");
        assertEvaluate("date_format('%d.%m.%Y', 'NO TIMESTAMP')", null);
    }

    @Test
    public void testMySQLCompatibilityForWeeks() throws Exception {
        Map<String, Map<String, String>> dateToInputOutputMap = Map.of(
            "1970-01-30", Map.of(
                "%u", "05",
                "%U", "04",
                "%v", "05",
                "%V", "04",
                "%x", "1970",
                "%X", "1970"
            ),
            "1996-01-01", Map.of(
                "%X %V", "1995 53",
                "%x %v", "1996 01",
                "%u", "01",
                "%U", "00"
            ),
            "2000-01-01", Map.of(
                "%u", "00",
                "%U", "00",
                "%v", "52",
                "%V", "52",
                "%x", "1999",
                "%X", "1999"
            ),
            "2004-01-01", Map.of(
                "%u", "01",
                "%U", "00",
                "%v", "01",
                "%V", "52",
                "%x", "2004",
                "%X", "2003"
            ),
            "2008-02-20", Map.of(
                "%u", "08",
                "%U", "07",
                "%v", "08",
                "%V", "07",
                "%x", "2008",
                "%X", "2008"),
            "2008-12-31", Map.of(
                "%u", "53",
                "%U", "52",
                "%v", "01",
                "%V", "52",
                "%x", "2009",
                "%X", "2008"
            ),
            "2009-01-01", Map.of(
                "%u", "01",
                "%U", "00",
                "%v", "01",
                "%V", "52",
                "%x", "2009",
                "%X", "2008"
            )
        );
        for (Map.Entry<String, Map<String, String>> entry : dateToInputOutputMap.entrySet()) {
            for (Map.Entry<String, String> ioEntry : entry.getValue().entrySet()) {
                Symbol result = sqlExpressions.normalize(sqlExpressions.asSymbol(String.format(Locale.ENGLISH,
                    "date_format('%s', '%s')", ioEntry.getKey(), entry.getKey())));
                assertThat(String.format(Locale.ENGLISH, "Format String '%s' returned wrong result for date '%s'", ioEntry.getKey(), entry.getKey()),
                    result,
                    isLiteral(ioEntry.getValue()));
            }
        }
    }

    @Test
    public void testEvaluateUTF8Support() {
        assertEvaluate("date_format(time_format, timestamp_tz)",
            "2000®01\uD834\uDD1E01 € 00:00:00",
            Literal.of("%Y®%m\uD834\uDD1E%d € %H:%i:%S"),
            Literal.of(DataTypes.TIMESTAMPZ, DataTypes.TIMESTAMPZ.implicitCast("2000-01-01")));
    }

    @Test
    public void testInvalidFormats() {
        assertEvaluate("date_format(time_format, timestamp_tz)",
            "t%Z",
            Literal.of("%t%%%Z"),
            Literal.of(DataTypes.TIMESTAMPZ, DataTypes.TIMESTAMPZ.implicitCast("2000-01-01")));
    }

    @Test
    public void testDayOfMonthWithEnglishSuffix() throws Exception {
        assertNormalize("date_format('%D', '2021-10-01')", isLiteral("1st"));
        assertNormalize("date_format('%D', '2021-10-12')", isLiteral("12th"));
        assertNormalize("date_format('%D', '2021-10-22')", isLiteral("22nd"));
    }
}
