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
import java.math.MathContext;
import java.util.Map;

import org.junit.Test;

public class DoubleTypeTest extends DataTypeTestCase<Double> {

    @Override
    public DataType<Double> getType() {
        return DoubleType.INSTANCE;
    }

    @Test
    public void test_cast_text_to_double() {
        assertThat(DoubleType.INSTANCE.implicitCast("123")).isEqualTo(123d);
    }

    @Test
    public void test_cast_long_to_double() {
        assertThat(DoubleType.INSTANCE.implicitCast(123L)).isEqualTo(123d);
    }

    @Test
    public void test_cast_numeric_to_double() {
        assertThat(DoubleType.INSTANCE.implicitCast(BigDecimal.valueOf(123.1))).isEqualTo(123.1d);
    }

    @Test
    public void test_cast_numeric_value_with_precision_and_scale_to_double() {
        assertThat(
            DoubleType.INSTANCE.implicitCast(
                new BigDecimal("123.1", MathContext.DECIMAL32)
                    .setScale(2, MathContext.DECIMAL32.getRoundingMode())
            )).isEqualTo(123.1d);
    }

    @Test
    public void test_sanitize_numeric_value() {
        assertThat(DoubleType.INSTANCE.sanitizeValue(1f)).isEqualTo(1d);
    }

    @Test
    public void text_cast_object_to_double_throws_exception() {
        assertThatThrownBy(() -> DoubleType.INSTANCE.implicitCast(Map.of()))
            .isExactlyInstanceOf(ClassCastException.class)
            .hasMessage("Can't cast '{}' to double precision");
    }

    @Test
    public void text_cast_boolean_to_double_throws_exception() {
        assertThatThrownBy(() -> DoubleType.INSTANCE.implicitCast(true))
            .isExactlyInstanceOf(ClassCastException.class)
            .hasMessage("Can't cast 'true' to double precision");
    }

    @Test
    public void test_cast_out_of_range_numeric_to_double_throws_exception() {
        assertThatThrownBy(() -> DoubleType.INSTANCE.implicitCast(new BigDecimal(Double.MAX_VALUE).add(BigDecimal.TEN)))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageStartingWith("double precision value out of range: ");
    }
}
