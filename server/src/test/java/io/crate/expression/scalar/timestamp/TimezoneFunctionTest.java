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

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.crate.exceptions.ConversionException;
import io.crate.expression.scalar.ScalarTestCase;
import io.crate.expression.symbol.Literal;
import io.crate.metadata.SystemClock;
import io.crate.types.DataTypes;

public class TimezoneFunctionTest extends ScalarTestCase {

    private static final long NOW_MILLIS = 1422294644581L;

    @Before
    public void prepare() {
        SystemClock.setCurrentMillisFixedUTC(NOW_MILLIS);
    }

    @After
    public void cleanUp() {
        SystemClock.setCurrentMillisSystemUTC();
    }

    @Test
    public void testEvaluateInvalidZoneIsNull() {
        assertEvaluateNull("timezone(null, 257504400000)");
    }

    @Test
    public void testEvaluateInvalidTimestampIsNull() {
        assertEvaluateNull("timezone('Europe/Madrid', null)");
    }

    @Test
    public void testEvaluateInvalidZoneIsBlanc() {
        assertThatThrownBy(() -> assertEvaluateNull("timezone(' ', 257504400000)"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("time zone \" \" not recognized");
    }

    @Test
    public void testEvaluateInvalidZoneIsRandom() {
        assertThatThrownBy(() -> assertEvaluateNull("timezone('Random/Location', 257504400000)"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("time zone \"Random/Location\" not recognized");
    }

    @Test
    public void testEvaluateInvalidZoneIsRandomNumeric() {
        assertThatThrownBy(() -> assertEvaluateNull("timezone('+31:97', 257504400000)"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("time zone \"+31:97\" not recognized");
    }

    @Test
    public void testEvaluateInvalidTimestamp() {
        assertThatThrownBy(() -> {
            assertEvaluateNull("timezone('Europe/Madrid', 'not_a_timestamp')");
        })
            .isExactlyInstanceOf(ConversionException.class)
            .hasMessage(
                "Cannot cast `'not_a_timestamp'` of type `text` to type `timestamp with time zone`");
    }

    @Test
    public void testEvaluateInvalidZoneIsNullFromColumn() {
        assertEvaluateNull("timezone(name, 257504400000)", Literal.of((Long) null));
    }

    @Test
    public void testEvaluateInvalidZoneIsBlancFromColumn() {
        assertThatThrownBy(() -> {
            assertEvaluateNull("timezone(name, 257504400000)", Literal.of(" "));

        })
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage("time zone \" \" not recognized");
    }

    @Test
    public void testEvaluateInvalidZoneIsRandomFromColumn() {
        assertThatThrownBy(() -> {
            assertEvaluateNull("timezone(name, 257504400000)", Literal.of("Random/Location"));

        })
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage("time zone \"Random/Location\" not recognized");
    }

    @Test
    public void testEvaluateInvalidZoneIsRandomNumericFromColumn() {
        assertThatThrownBy(() -> {
            assertEvaluateNull("timezone(name, 257504400000)", Literal.of("+31:97"));

        })
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage("time zone \"+31:97\" not recognized");
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

    @Test
    public void test_timezone_doesnt_truncate_millis() {
        assertEvaluate("timezone('UTC', current_timestamp)", NOW_MILLIS);
        assertEvaluate("timezone('GMT+1', current_timestamp)", NOW_MILLIS + 3600 * 1000);
    }
}
