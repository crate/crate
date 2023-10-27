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

import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

public class ByteTypeTest extends ESTestCase {

    @Test
    public void test_cast_text_to_byte() {
        assertThat(ByteType.INSTANCE.implicitCast("123")).isEqualTo((byte) 123);
    }

    @Test
    public void test_cast_long_to_byte() {
        assertThat(ByteType.INSTANCE.implicitCast(123L)).isEqualTo((byte) 123);
    }

    @Test
    public void test_cast_numeric_to_byte() {
        assertThat(ByteType.INSTANCE.implicitCast(BigDecimal.valueOf(123))).isEqualTo((byte) 123);
    }

    @Test
    public void test_sanitize_numeric_value() {
        assertThat(ByteType.INSTANCE.sanitizeValue(1f)).isEqualTo((byte) 1);
    }

    @Test
    public void test_cast_boolean_to_byte_throws_exception() {
        assertThatThrownBy(() -> ByteType.INSTANCE.implicitCast(true))
            .isExactlyInstanceOf(ClassCastException.class)
            .hasMessage("Can't cast 'true' to byte");
    }

    @Test
    public void test_cast_object_to_byte_throws_exception() {
        assertThatThrownBy(() -> ByteType.INSTANCE.implicitCast(Map.of()))
            .isExactlyInstanceOf(ClassCastException.class)
            .hasMessage("Can't cast '{}' to byte");
    }

    @Test
    public void test_cast_integer_to_byte_out_of_negative_range_throws_exception() {
        assertThatThrownBy(() -> ByteType.INSTANCE.implicitCast(-129))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("byte value out of range: -129");
    }

    @Test
    public void test_cast_integer_to_byte_out_of_positive_range_throws_exception() {
        assertThatThrownBy(() -> ByteType.INSTANCE.implicitCast(129))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("byte value out of range: 129");
    }

    @Test
    public void test_cast_out_of_range_numeric_to_byte_throws_exception() {
        assertThatThrownBy(() -> ByteType.INSTANCE.implicitCast(BigDecimal.valueOf(129)))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("byte value out of range: 129");
    }
}
