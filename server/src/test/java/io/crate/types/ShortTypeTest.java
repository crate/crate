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
import static org.junit.Assert.assertThat;

import java.math.BigDecimal;
import java.util.Map;

import org.junit.Test;


public class ShortTypeTest extends DataTypeTestCase<Short> {

    @Override
    public DataType<Short> getType() {
        return ShortType.INSTANCE;
    }

    @Test
    public void test_cast_text_to_smallint() {
        assertThat(ShortType.INSTANCE.implicitCast("123"), is((short) 123));
    }

    @Test
    public void test_cast_bigint_to_smallint() {
        assertThat(ShortType.INSTANCE.implicitCast(123L), is((short) 123));
    }

    @Test
    public void test_cast_numeric_to_integer() {
        assertThat(ShortType.INSTANCE.implicitCast(BigDecimal.valueOf(123)), is((short) 123));
    }

    @Test
    public void test_sanitize_numeric_value() {
        assertThat(ShortType.INSTANCE.sanitizeValue(1f), is((short) 1));
    }

    @Test
    public void test_cast_boolean_to_smallint_throws_exception() {
        expectedException.expect(ClassCastException.class);
        expectedException.expectMessage("Can't cast 'true' to smallint");
        ShortType.INSTANCE.implicitCast(true);
    }

    @Test
    public void test_cast_object_to_smallint_throws_exception() {
        expectedException.expect(ClassCastException.class);
        expectedException.expectMessage("Can't cast '{}' to smallint");
        ShortType.INSTANCE.implicitCast(Map.of());
    }

    @Test
    public void test_cast_int_to_short_out_of_positive_range_throws_exception() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("short value out of range: 2147483647");
        ShortType.INSTANCE.implicitCast(Integer.MAX_VALUE);
    }

    @Test
    public void test_cast_out_of_range_numeric_to_integer_throws_exception() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("short value out of range: 2147483647");
        ShortType.INSTANCE.implicitCast(BigDecimal.valueOf(Integer.MAX_VALUE));
    }

    @Test
    public void test_cast_int_to_short_out_of_negative_range_throws_exception() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("short value out of range: -2147483648");
        ShortType.INSTANCE.implicitCast(Integer.MIN_VALUE);
    }
}
