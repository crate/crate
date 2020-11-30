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

package io.crate.types;

import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.Map;

import static org.hamcrest.Matchers.is;

public class IntegerTypeTest extends ESTestCase {

    @Test
    public void test_cast_text_to_integer() {
        assertThat(IntegerType.INSTANCE.implicitCast("123"), is(123));
    }

    @Test
    public void test_cast_bigint_to_integer() {
        assertThat(IntegerType.INSTANCE.implicitCast(123L), is(123));
    }

    @Test
    public void test_cast_numeric_to_integer() {
        assertThat(IntegerType.INSTANCE.implicitCast(BigDecimal.valueOf(123)), is(123));
    }

    @Test
    public void test_sanitize_numeric_value() {
        assertThat(IntegerType.INSTANCE.sanitizeValue(1f), is(1));
    }

    @Test
    public void test_cast_boolean_to_integer_throws_exception() {
        expectedException.expect(ClassCastException.class);
        expectedException.expectMessage("Can't cast 'true' to integer");
        IntegerType.INSTANCE.implicitCast(true);
    }

    @Test
    public void test_cast_object_to_integer_throws_exception() {
        expectedException.expect(ClassCastException.class);
        expectedException.expectMessage("Can't cast '{}' to integer");
        IntegerType.INSTANCE.implicitCast(Map.of());
    }

    @Test
    public void test_cast_bigint_to_integer_out_of_negative_range_throws_exception() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("integer value out of range: -9223372036854775808");
        IntegerType.INSTANCE.implicitCast(Long.MIN_VALUE);
    }

    @Test
    public void test_cast_bigint_to_integer_out_of_positive_range_throws_exception() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("integer value out of range: 9223372036854775807");
        IntegerType.INSTANCE.implicitCast(Long.MAX_VALUE);
    }

    @Test
    public void test_cast_out_of_range_numeric_to_integer_throws_exception() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("integer value out of range: 9223372036854775807");
        IntegerType.INSTANCE.implicitCast(BigDecimal.valueOf(Long.MAX_VALUE));
    }
}
