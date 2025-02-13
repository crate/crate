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


public class ShortTypeTest extends DataTypeTestCase<Short> {

    @Override
    protected DataDef<Short> getDataDef() {
        return DataDef.fromType(ShortType.INSTANCE);
    }

    @Test
    public void test_cast_text_to_smallint() {
        assertThat(ShortType.INSTANCE.implicitCast("123")).isEqualTo((short) 123);
    }

    @Test
    public void test_cast_bigint_to_smallint() {
        assertThat(ShortType.INSTANCE.implicitCast(123L)).isEqualTo((short) 123);
    }

    @Test
    public void test_cast_numeric_to_integer() {
        assertThat(ShortType.INSTANCE.implicitCast(BigDecimal.valueOf(123))).isEqualTo((short) 123);
    }

    @Test
    public void test_sanitize_numeric_value() {
        assertThat(ShortType.INSTANCE.sanitizeValue(1f)).isEqualTo((short) 1);
    }

    @Test
    public void test_cast_boolean_to_smallint_throws_exception() {
        assertThatThrownBy(() -> ShortType.INSTANCE.implicitCast(true))
            .isExactlyInstanceOf(ClassCastException.class)
            .hasMessage("Can't cast 'true' to smallint");
    }

    @Test
    public void test_cast_object_to_smallint_throws_exception() {
        assertThatThrownBy(() -> ShortType.INSTANCE.implicitCast(Map.of()))
            .isExactlyInstanceOf(ClassCastException.class)
            .hasMessage("Can't cast '{}' to smallint");
    }

    @Test
    public void test_cast_int_to_short_out_of_positive_range_throws_exception() {
        assertThatThrownBy(() -> ShortType.INSTANCE.implicitCast(Integer.MAX_VALUE))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("short value out of range: 2147483647");
    }

    @Test
    public void test_cast_out_of_range_numeric_to_integer_throws_exception() {
        assertThatThrownBy(() -> ShortType.INSTANCE.implicitCast(Integer.MAX_VALUE))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("short value out of range: 2147483647");
    }

    @Test
    public void test_cast_int_to_short_out_of_negative_range_throws_exception() {
        assertThatThrownBy(() -> ShortType.INSTANCE.implicitCast(Integer.MIN_VALUE))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("short value out of range: -2147483648");
    }
}
