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

package io.crate.types;

import static org.hamcrest.Matchers.is;

import java.math.BigDecimal;
import java.util.Map;

import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.settings.SessionSettings;

public class LongTypeTest extends ESTestCase {

    private static final SessionSettings SESSION_SETTINGS = CoordinatorTxnCtx.systemTransactionContext().sessionSettings();

    @Test
    public void test_cast_text_to_bigint() {
        assertThat(LongType.INSTANCE.implicitCast("12839", SESSION_SETTINGS), is(12839L));
        assertThat(LongType.INSTANCE.implicitCast("-12839", SESSION_SETTINGS), is(-12839L));
        assertThat(LongType.INSTANCE.implicitCast(Long.toString(Long.MAX_VALUE), SESSION_SETTINGS), is(Long.MAX_VALUE));
        assertThat(LongType.INSTANCE.implicitCast(Long.toString(Long.MIN_VALUE), SESSION_SETTINGS), is(Long.MIN_VALUE));
        assertThat(LongType.INSTANCE.implicitCast("+2147483647111", SESSION_SETTINGS), is(2147483647111L));
    }

    @Test
    public void test_cast_numeric_to_long() {
        assertThat(LongType.INSTANCE.implicitCast(BigDecimal.valueOf(123), SESSION_SETTINGS), is(123L));
    }

    @Test
    public void test_sanitize_numeric_value() {
        assertThat(LongType.INSTANCE.sanitizeValue(1f), is(1L));
    }

    @Test
    public void test_cast_text_with_only_letters_to_bigint_throws_exception() {
        expectedException.expect(NumberFormatException.class);
        LongType.INSTANCE.implicitCast("hello", SESSION_SETTINGS);
    }

    @Test
    public void testConversionWithNonAsciiCharacter() {
        expectedException.expect(NumberFormatException.class);
        expectedException.expectMessage("\u03C0"); // "π" GREEK SMALL LETTER PI
        LongType.INSTANCE.implicitCast("\u03C0", SESSION_SETTINGS);
    }

    @Test
    public void testInvalidFirstChar() {
        expectedException.expect(NumberFormatException.class);
        LongType.INSTANCE.implicitCast(" 1", SESSION_SETTINGS);
    }

    @Test
    public void testOnlyMinusSign() {
        expectedException.expect(NumberFormatException.class);
        LongType.INSTANCE.implicitCast("-", SESSION_SETTINGS);
    }

    @Test
    public void testOnlyPlusSign() {
        expectedException.expect(NumberFormatException.class);
        LongType.INSTANCE.implicitCast("+", SESSION_SETTINGS);
    }

    @Test
    public void testNumberThatIsGreaterThanMaxValue() {
        expectedException.expect(NumberFormatException.class);
        LongType.INSTANCE.implicitCast(Long.MAX_VALUE + "111", SESSION_SETTINGS);
    }

    @Test
    public void test_cast_out_of_range_numeric_to_integer_throws_exception() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("bigint value out of range: 9223372036854775817");
        LongType.INSTANCE.implicitCast(BigDecimal.valueOf(Long.MAX_VALUE).add(BigDecimal.TEN), SESSION_SETTINGS);
    }

    @Test
    public void test_cast_object_to_bigint_throws_exception() {
        expectedException.expect(ClassCastException.class);
        expectedException.expectMessage("Can't cast '{}' to bigint");
        LongType.INSTANCE.implicitCast(Map.of(), SESSION_SETTINGS);
    }

    @Test
    public void test_cast_boolean_to_bigint_throws_exception() {
        expectedException.expect(ClassCastException.class);
        expectedException.expectMessage("Can't cast 'true' to bigint");
        LongType.INSTANCE.implicitCast(true, SESSION_SETTINGS);
    }

    @Test
    public void test_cast_out_of_range_numeric_to_bigint_throws_exception() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("bigint value out of range: 9223372036854775808");
        LongType.INSTANCE.implicitCast(new BigDecimal("9223372036854775808"), SESSION_SETTINGS);
    }

    @Test
    public void test_cast_numeric_with_fraction_to_long_looses_fraction() {
        assertThat(
            LongType.INSTANCE.implicitCast(BigDecimal.valueOf(12.12), SESSION_SETTINGS),
            is(12L)
        );
    }
}
