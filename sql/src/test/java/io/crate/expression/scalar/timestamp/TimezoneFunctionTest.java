/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.expression.scalar.timestamp;

import io.crate.expression.scalar.AbstractScalarFunctionsTest;
import io.crate.expression.symbol.Literal;
import io.crate.types.DataTypes;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.time.format.DateTimeParseException;

import static java.lang.String.format;

public class TimezoneFunctionTest extends AbstractScalarFunctionsTest {

    private static final Long TIMESTAMP = 257504400000L;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testEvaluateInvalidZoneIsNull() {
        assertEvaluate(format("timezone(null, %d)", TIMESTAMP), null);
    }

    @Test
    public void testEvaluateInvalidTimestampIsNull() {
        assertEvaluate("timezone('Europe/Madrid', null)", null);
    }

    @Test
    public void testEvaluateInvalidZoneIsBlanc() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("time zone \" \" not recognized");
        assertEvaluate(format("timezone(' ', %d)", TIMESTAMP), null);
    }

    @Test
    public void testEvaluateInvalidZoneIsRandom() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("time zone \"Random/Location\" not recognized");
        assertEvaluate(format("timezone('Random/Location', %d)", TIMESTAMP), null);
    }

    @Test
    public void testEvaluateInvalidZoneIsRandomNumeric() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("time zone \"+31:97\" not recognized");
        assertEvaluate(format("timezone('+31:97', %d)", TIMESTAMP), null);
    }

    @Test
    public void testEvaluateInvalidTimestamp() {
        expectedException.expect(DateTimeParseException.class);
        expectedException.expectMessage(
            "Text 'not_a_timestamp' could not be parsed at index 0");
        assertEvaluate("timezone('Europe/Madrid', 'not_a_timestamp')", null);
    }

    @Test
    public void testEvaluateInvalidZoneIsNullFromColumn() {
        assertEvaluate(format("timezone(name, %d)", TIMESTAMP), null, Literal.of((Long) null));
    }

    @Test
    public void testEvaluateInvalidZoneIsBlancFromColumn() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("time zone \" \" not recognized");
        assertEvaluate(format("timezone(name, %d)", TIMESTAMP), null, Literal.of(" "));
    }

    @Test
    public void testEvaluateInvalidZoneIsRandomFromColumn() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("time zone \"Random/Location\" not recognized");
        assertEvaluate(format("timezone(name, %d)", TIMESTAMP), null, Literal.of("Random/Location"));
    }

    @Test
    public void testEvaluateInvalidZoneIsRandomNumericFromColumn() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("time zone \"+31:97\" not recognized");
        assertEvaluate(format("timezone(name, %d)", TIMESTAMP), null, Literal.of("+31:97"));
    }

    @Test
    public void testEvaluateValidTimestamp() {
        assertEvaluate(format("timezone('UTC', %d)", TIMESTAMP), TIMESTAMP);
        assertEvaluate("timezone('UTC', x)", TIMESTAMP, Literal.of(TIMESTAMP));
        assertEvaluate(format("timezone(name, %d)", TIMESTAMP), TIMESTAMP, Literal.of("UTC"));
        assertEvaluate(format("timezone('Europe/Madrid', %d)", 257491800000L), 257488200000L);
        assertEvaluate("timezone('Europe/Madrid', '1978-02-28T14:30+05:30'::timestamp with time zone)",
                       257508000000L);
        assertEvaluate("timezone('Europe/Madrid', '1978-02-28T14:30+05:30'::timestamp without time zone)",
                       257520600000L);
        assertEvaluate("timezone(name, '1978-02-28T14:30+05:30'::timestamp with time zone)",
                       257508000000L,
                       Literal.of("Europe/Madrid"));
        assertEvaluate("timezone(name, '1978-02-28T14:30+05:30'::timestamp without time zone)",
                       257520600000L,
                       Literal.of("Europe/Madrid"));
        assertEvaluate("timezone('Europe/Madrid', timestamp_tz)",
                       257508000000L,
                       Literal.of(DataTypes.TIMESTAMPZ, DataTypes.TIMESTAMPZ.value("1978-02-28T14:30+05:30")));
        assertEvaluate("timezone('Europe/Madrid', timestamp)",
                       257520600000L,
                       Literal.of(DataTypes.TIMESTAMP, DataTypes.TIMESTAMP.value("1978-02-28T14:30+05:30")));
        assertEvaluate("timezone('Europe/Madrid', x)", 257484600000L, Literal.of(257488200000L));
    }
}
