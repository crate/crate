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


import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class NumericTypeTest extends ESTestCase {

    @Test
    public void test_implicit_cast_text_to_unscaled_numeric() {
        assertThat(NumericType.INSTANCE.implicitCast("12839"), is(BigDecimal.valueOf(12839)));
        assertThat(NumericType.INSTANCE.implicitCast("-12839"), is(BigDecimal.valueOf(-12839)));
        assertThat(NumericType.INSTANCE.implicitCast("+2147483647111"), is(BigDecimal.valueOf(2147483647111L)));
        assertThat(NumericType.INSTANCE.implicitCast("+214748364711119475"), is(new BigDecimal("214748364711119475")));
    }

    @Test
    public void test_implicit_cast_floating_point_to_unscaled_numeric() {
        assertThat(NumericType.INSTANCE.implicitCast(10.0d), is(BigDecimal.valueOf(10.0)));
        assertThat(NumericType.INSTANCE.implicitCast(1.023f), is(BigDecimal.valueOf(1.023)));
    }

    @Test
    public void test_implicit_cast_decimal_types_to_unscaled_numeric() {
        assertThat(NumericType.INSTANCE.implicitCast(1), is(BigDecimal.valueOf(1)));
        assertThat(NumericType.INSTANCE.implicitCast(2L), is(BigDecimal.valueOf(2)));
        assertThat(NumericType.INSTANCE.implicitCast((short) 3), is(BigDecimal.valueOf(3)));
        assertThat(NumericType.INSTANCE.implicitCast((byte) 4), is(BigDecimal.valueOf(4)));
    }

    @Test
    public void test_implicit_cast_text_types_to_numeric_with_precision() {
        assertThat(NumericType.of(5).implicitCast("12345"), is(BigDecimal.valueOf(12345)));
        assertThat(NumericType.of(6).implicitCast("12345"), is(BigDecimal.valueOf(12345)));
    }

    @Test
    public void test_implicit_cast_text_types_to_numeric_with_precision_and_scale() {
        assertThat(NumericType.of(16, 0).implicitCast("12345"), is(BigDecimal.valueOf(12345)));
        assertThat(NumericType.of(16, 2).implicitCast("12345").toString(), is("12345.00"));
        assertThat(NumericType.of(10, 4).implicitCast("12345").toString(), is("12345.0000"));
    }

    @Test
    public void test_implicit_cast_decimal_types_to_numeric_with_precision() {
        assertThat(NumericType.of(5).implicitCast(12345), is(BigDecimal.valueOf(12345)));
        assertThat(NumericType.of(6).implicitCast(12345), is(BigDecimal.valueOf(12345)));
    }

    @Test
    public void test_implicit_cast_decimal_types_to_numeric_with_precision_and_scale() {
        assertThat(NumericType.of(16, 0).implicitCast(12345), is(BigDecimal.valueOf(12345)));
        assertThat(NumericType.of(16, 2).implicitCast(12345).toString(), is("12345.00"));
        assertThat(NumericType.of(10, 4).implicitCast(12345).toString(), is("12345.0000"));
    }

    @Test
    public void test_implicit_cast_floating_point_to_numeric_with_precision() {
        assertThat(NumericType.of(2).implicitCast(10.1234d), is(BigDecimal.valueOf(10)));
        assertThat(NumericType.of(3).implicitCast(10.1234d), is(BigDecimal.valueOf(10)));
        assertThat(NumericType.of(3).implicitCast(10.9234d), is(BigDecimal.valueOf(11)));
    }

    @Test
    public void test_implicit_cast_floating_point_to_numeric_with_precision_and_scale() {
        assertThat(NumericType.of(6, 0).implicitCast(10.1235d), is(BigDecimal.valueOf(10)));
        assertThat(NumericType.of(6, 2).implicitCast(10.1235d), is(BigDecimal.valueOf(10.12)));
        assertThat(NumericType.of(6, 3).implicitCast(10.1235d), is(BigDecimal.valueOf(10.124)));
    }

    @Test
    public void test_implicit_cast_to_itself() {
        assertThat(NumericType.INSTANCE.implicitCast(BigDecimal.valueOf(1)), is(BigDecimal.valueOf(1)));
    }

    @Test
    public void test_implicit_cast_null_value() {
        assertThat(NumericType.INSTANCE.implicitCast(null), is(nullValue()));
    }

    public void test_sanitize_numeric_value() {
        assertThat(NumericType.INSTANCE.sanitizeValue(BigDecimal.valueOf(1)), is(BigDecimal.valueOf(1)));
    }

    @Test
    public void test_cast_boolean_to_smallint_throws_exception() {
        expectedException.expect(ClassCastException.class);
        expectedException.expectMessage("Can't cast 'true' to numeric");
        NumericType.INSTANCE.implicitCast(true);
    }

    @Test
    public void test_cast_array_to_numeric_throws_exception() {
        expectedException.expect(ClassCastException.class);
        expectedException.expectMessage("Can't cast '[]' to numeric");
        NumericType.INSTANCE.implicitCast(List.of());
    }

    @Test
    public void test_cast_row_to_numeric_throws_exception() {
        expectedException.expect(ClassCastException.class);
        expectedException.expectMessage("Can't cast 'record' to numeric");
        NumericType.INSTANCE.implicitCast(RowType.EMPTY);
    }

    @Test
    public void test_cast_object_to_smallint_throws_exception() {
        expectedException.expect(ClassCastException.class);
        expectedException.expectMessage("Can't cast '{}' to numeric");
        NumericType.INSTANCE.implicitCast(Map.of());
    }

    @Test
    public void test_numeric_null_value_streaming() throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        NumericType.INSTANCE.writeValueTo(out, null);

        StreamInput in = out.bytes().streamInput();

        assertThat(NumericType.INSTANCE.readValueFrom(in), is(nullValue()));
    }

    @Test
    public void test_numeric_value_streaming() throws IOException {
        BigDecimal expected = BigDecimal.TEN;

        BytesStreamOutput out = new BytesStreamOutput();
        NumericType.INSTANCE.writeValueTo(out, expected);

        StreamInput in = out.bytes().streamInput();
        BigDecimal actual = NumericType.INSTANCE.readValueFrom(in);

        assertThat(expected, is(actual));
    }

    @Test
    public void test_unscaled_numeric_serialization_round_trip() throws IOException {
        var out = new BytesStreamOutput();
        DataTypes.toStream(NumericType.INSTANCE, out);

        var in = out.bytes().streamInput();
        NumericType actual = (NumericType) DataTypes.fromStream(in);

        assertThat(actual.precision(), is(nullValue()));
        assertThat(actual.scale(), is(nullValue()));
    }

    @Test
    public void test_numeric_with_precision_and_scale_serialization_round_trip() throws IOException {
        var out = new BytesStreamOutput();
        var expected = NumericType.of(1, 2);
        DataTypes.toStream(expected, out);

        var in = out.bytes().streamInput();
        NumericType actual = (NumericType) DataTypes.fromStream(in);

        assertThat(actual.precision(), is(1));
        assertThat(actual.scale(), is(2));
    }
}
