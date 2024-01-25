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

import static io.crate.testing.Asserts.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;
import java.util.Map;

import org.junit.Test;

public class LongTypeTest extends DataTypeTestCase<Long> {

    @Override
    public DataType<Long> getType() {
        return LongType.INSTANCE;
    }

    @Test
    public void test_cast_text_to_bigint() {
        assertThat(LongType.INSTANCE.implicitCast("12839")).isEqualTo(12839L);
        assertThat(LongType.INSTANCE.implicitCast("-12839")).isEqualTo(-12839L);
        assertThat(LongType.INSTANCE.implicitCast(Long.toString(Long.MAX_VALUE))).isEqualTo(Long.MAX_VALUE);
        assertThat(LongType.INSTANCE.implicitCast(Long.toString(Long.MIN_VALUE))).isEqualTo(Long.MIN_VALUE);
        assertThat(LongType.INSTANCE.implicitCast("+2147483647111")).isEqualTo(2147483647111L);
    }

    @Test
    public void test_cast_numeric_to_long() {
        assertThat(LongType.INSTANCE.implicitCast(BigDecimal.valueOf(123))).isEqualTo(123L);
    }

    @Test
    public void test_sanitize_numeric_value() {
        assertThat(LongType.INSTANCE.sanitizeValue(1f)).isEqualTo(1L);
    }

    @Test
    public void test_cast_text_with_only_letters_to_bigint_throws_exception() {
        assertThatThrownBy(() -> LongType.INSTANCE.implicitCast("hello"))
            .isExactlyInstanceOf(NumberFormatException.class);
    }

    @Test
    public void testConversionWithNonAsciiCharacter() {
        assertThatThrownBy(() -> LongType.INSTANCE.implicitCast("\u03C0"))
            .isExactlyInstanceOf(NumberFormatException.class)
            .hasMessage("For input string: \"\u03C0\""); // "π" GREEK SMALL LETTER PI
    }

    @Test
    public void testInvalidFirstChar() {
        assertThatThrownBy(() -> LongType.INSTANCE.implicitCast(" 1"))
            .isExactlyInstanceOf(NumberFormatException.class);
    }

    @Test
    public void testOnlyMinusSign() {
        assertThatThrownBy(() -> LongType.INSTANCE.implicitCast("-"))
            .isExactlyInstanceOf(NumberFormatException.class);
    }

    @Test
    public void testOnlyPlusSign() {
        assertThatThrownBy(() -> LongType.INSTANCE.implicitCast("+"))
            .isExactlyInstanceOf(NumberFormatException.class);
    }

    @Test
    public void testNumberThatIsGreaterThanMaxValue() {
        assertThatThrownBy(() -> LongType.INSTANCE.implicitCast(Long.MAX_VALUE + "111"))
            .isExactlyInstanceOf(NumberFormatException.class);
    }

    @Test
    public void test_cast_out_of_range_numeric_to_integer_throws_exception() {
        assertThatThrownBy(() -> LongType.INSTANCE.implicitCast(BigDecimal.valueOf(Long.MAX_VALUE).add(BigDecimal.TEN)))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("bigint value out of range: 9223372036854775817");
    }

    @Test
    public void test_cast_object_to_bigint_throws_exception() {
        assertThatThrownBy(() -> LongType.INSTANCE.implicitCast(Map.of()))
            .isExactlyInstanceOf(ClassCastException.class)
            .hasMessage("Can't cast '{}' to bigint");
    }

    @Test
    public void test_cast_boolean_to_bigint_throws_exception() {
        assertThatThrownBy(() -> LongType.INSTANCE.implicitCast(true))
            .isExactlyInstanceOf(ClassCastException.class)
            .hasMessage("Can't cast 'true' to bigint");
    }

    @Test
    public void test_cast_out_of_range_numeric_to_bigint_throws_exception() {
        assertThatThrownBy(() -> LongType.INSTANCE.implicitCast(new BigDecimal("9223372036854775808")))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("bigint value out of range: 9223372036854775808");
    }

    @Test
    public void test_cast_numeric_with_fraction_to_long_looses_fraction() {
        assertThat(LongType.INSTANCE.implicitCast(BigDecimal.valueOf(12.12))).isEqualTo(12L);
    }
}
