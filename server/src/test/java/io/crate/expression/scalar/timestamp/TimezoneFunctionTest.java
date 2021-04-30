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

package io.crate.expression.scalar.timestamp;

import io.crate.expression.scalar.ScalarTestCase;
import io.crate.expression.symbol.Literal;
import io.crate.types.DataTypes;
import org.junit.Test;

public class TimezoneFunctionTest extends ScalarTestCase {

    @Test
    public void testEvaluateInvalidZoneIsNull() {
        assertEvaluate("timezone(null, 257504400000)", null);
    }

    @Test
    public void testEvaluateInvalidTimestampIsNull() {
        assertEvaluate("timezone('Europe/Madrid', null)", null);
    }

    @Test
    public void testEvaluateInvalidZoneIsBlanc() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("time zone \" \" not recognized");
        assertEvaluate("timezone(' ', 257504400000)", null);
    }

    @Test
    public void testEvaluateInvalidZoneIsRandom() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("time zone \"Random/Location\" not recognized");
        assertEvaluate("timezone('Random/Location', 257504400000)", null);
    }

    @Test
    public void testEvaluateInvalidZoneIsRandomNumeric() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("time zone \"+31:97\" not recognized");
        assertEvaluate("timezone('+31:97', 257504400000)", null);
    }

    @Test
    public void testEvaluateInvalidTimestamp() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage(
            "Cannot cast `'not_a_timestamp'` of type `text` to type `timestamp with time zone`");
        assertEvaluate("timezone('Europe/Madrid', 'not_a_timestamp')", null);
    }

    @Test
    public void testEvaluateInvalidZoneIsNullFromColumn() {
        assertEvaluate("timezone(name, 257504400000)", null, Literal.of((Long) null));
    }

    @Test
    public void testEvaluateInvalidZoneIsBlancFromColumn() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("time zone \" \" not recognized");
        assertEvaluate("timezone(name, 257504400000)", null, Literal.of(" "));
    }

    @Test
    public void testEvaluateInvalidZoneIsRandomFromColumn() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("time zone \"Random/Location\" not recognized");
        assertEvaluate("timezone(name, 257504400000)", null, Literal.of("Random/Location"));
    }

    @Test
    public void testEvaluateInvalidZoneIsRandomNumericFromColumn() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("time zone \"+31:97\" not recognized");
        assertEvaluate("timezone(name, 257504400000)", null, Literal.of("+31:97"));
    }

    @Test
    public void testEvaluateValidTimestamp() {
        assertEvaluate("timezone('UTC', 257504400000)", 257504400000L);
        assertEvaluate("timezone('UTC', x)", 257504400000L, Literal.of(257504400000L));
        assertEvaluate("timezone(name, 257504400000)", 257504400000L, Literal.of("UTC"));
        assertEvaluate("timezone('Europe/Madrid', 257491800000)", 257488200000L);
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
        assertEvaluate(
            "timezone('Europe/Madrid', timestamp_tz)",
            257508000000L,
            Literal.of(
                DataTypes.TIMESTAMPZ,
                DataTypes.TIMESTAMPZ.implicitCast("1978-02-28T14:30+05:30")
            )
        );
        assertEvaluate(
            "timezone('Europe/Madrid', timestamp)",
            257520600000L,
            Literal.of(
                DataTypes.TIMESTAMP,
                DataTypes.TIMESTAMP.implicitCast("1978-02-28T14:30+05:30")
            )
        );
        assertEvaluate("timezone('Europe/Madrid', x)", 257484600000L, Literal.of(257488200000L));
    }
}
