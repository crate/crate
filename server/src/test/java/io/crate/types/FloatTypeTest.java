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
import java.math.MathContext;
import java.util.Map;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;

public class FloatTypeTest extends ESTestCase {

    @Test
    public void test_cast_long_to_real() {
        assertThat(FloatType.INSTANCE.implicitCast(123L), is(123.0f));
    }

    @Test
    public void test_cast_text_to_real() {
        assertThat(FloatType.INSTANCE.implicitCast("123"), is(123.0f));
    }

    @Test
    public void test_cast_numeric_to_float() {
        assertThat(FloatType.INSTANCE.implicitCast(BigDecimal.valueOf(123.12)), is(123.12F));
    }

    @Test
    public void test_sanitize_numeric_value() {
        assertThat(FloatType.INSTANCE.sanitizeValue(1d), is(1f));
    }

    @Test
    public void test_cast_numeric_value_with_precision_and_scale_to_double() {
        assertThat(
            FloatType.INSTANCE.implicitCast(
                new BigDecimal("123.1", MathContext.DECIMAL32)
                    .setScale(2, MathContext.DECIMAL32.getRoundingMode())
            ), is(123.1f)
        );
    }

    @Test
    public void test_infinite_float_can_be_casted_to_infinite_float() throws Exception {
        assertThat(FloatType.INSTANCE.implicitCast(Float.POSITIVE_INFINITY), is(Float.POSITIVE_INFINITY));
        assertThat(FloatType.INSTANCE.implicitCast(Float.NEGATIVE_INFINITY), is(Float.NEGATIVE_INFINITY));
    }

    @Test
    public void test_infinite_double_can_be_casted_to_infinite_float() throws Exception {
        assertThat(FloatType.INSTANCE.implicitCast(Double.POSITIVE_INFINITY), is(Float.POSITIVE_INFINITY));
        assertThat(FloatType.INSTANCE.implicitCast(Double.NEGATIVE_INFINITY), is(Float.NEGATIVE_INFINITY));
    }

    @Test
    public void test_negative_max_float_value_can_be_casted_to_float() throws Exception {
        assertThat(FloatType.INSTANCE.implicitCast(-3.4028235e38d), is(- Float.MAX_VALUE));
        assertThat(FloatType.INSTANCE.implicitCast(-3.4028235e38f), is(- Float.MAX_VALUE));
    }

    @Test
    public void test_cast_double_to_real_out_of_positive_range_throws_exception() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("float value out of range: 1.7976931348623157E308");
        FloatType.INSTANCE.implicitCast(Double.MAX_VALUE);
    }

    @Test
    public void test_cast_double_to_real_out_of_negative_range_throws_exception() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("float value out of range: -1.7976931348623157E308");
        FloatType.INSTANCE.implicitCast(-Double.MAX_VALUE);
    }

    @Test
    public void test_cast_out_of_range_numeric_to_float_throws_exception() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage(startsWith("float value out of range: "));
        FloatType.INSTANCE.implicitCast(new BigDecimal(Double.MAX_VALUE));
    }

    @Test
    public void test_cast_boolean_to_real_throws_exception() {
        expectedException.expect(ClassCastException.class);
        expectedException.expectMessage("Can't cast 'true' to real");
        FloatType.INSTANCE.implicitCast(true);
    }

    @Test
    public void test_cast_object_to_real_throws_exception() {
        expectedException.expect(ClassCastException.class);
        expectedException.expectMessage("Can't cast '{}' to real");
        FloatType.INSTANCE.implicitCast(Map.of());
    }
}
