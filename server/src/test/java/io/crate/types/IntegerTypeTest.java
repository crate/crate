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

public class IntegerTypeTest extends DataTypeTestCase<Integer> {

    @Override
    public DataType<Integer> getType() {
        return IntegerType.INSTANCE;
    }

    @Test
    public void test_cast_text_to_integer() {
        assertThat(IntegerType.INSTANCE.implicitCast("123")).isEqualTo(123);
    }

    @Test
    public void test_cast_bigint_to_integer() {
        assertThat(IntegerType.INSTANCE.implicitCast(123L)).isEqualTo(123);
    }

    @Test
    public void test_cast_numeric_to_integer() {
        assertThat(IntegerType.INSTANCE.implicitCast(BigDecimal.valueOf(123))).isEqualTo(123);
    }

    @Test
    public void test_sanitize_numeric_value() {
        assertThat(IntegerType.INSTANCE.sanitizeValue(1f)).isEqualTo(1);
    }

    @Test
    public void test_cast_boolean_to_integer_throws_exception() {
        assertThatThrownBy(() -> IntegerType.INSTANCE.implicitCast(true))
            .isExactlyInstanceOf(ClassCastException.class)
            .hasMessage("Can't cast 'true' to integer");
    }

    @Test
    public void test_cast_object_to_integer_throws_exception() {
        assertThatThrownBy(() -> IntegerType.INSTANCE.implicitCast(Map.of()))
            .isExactlyInstanceOf(ClassCastException.class)
            .hasMessage("Can't cast '{}' to integer");
    }

    @Test
    public void test_cast_bigint_to_integer_out_of_negative_range_throws_exception() {
        assertThatThrownBy(() -> IntegerType.INSTANCE.implicitCast(Long.MIN_VALUE))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("integer value out of range: -9223372036854775808");
    }

    @Test
    public void test_cast_bigint_to_integer_out_of_positive_range_throws_exception() {
        assertThatThrownBy(() -> IntegerType.INSTANCE.implicitCast(Long.MAX_VALUE))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("integer value out of range: 9223372036854775807");
    }

    @Test
    public void test_cast_out_of_range_numeric_to_integer_throws_exception() {
        assertThatThrownBy(() -> IntegerType.INSTANCE.implicitCast(BigDecimal.valueOf(Long.MAX_VALUE)))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("integer value out of range: 9223372036854775807");
    }

    @Test
    public void test_cast_numeric_with_fraction_to_integer_looses_fraction() {
        assertThat(IntegerType.INSTANCE.implicitCast(BigDecimal.valueOf(12.12))).isEqualTo(12);
    }

}
