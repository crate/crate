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

package io.crate.protocols.postgres.types;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.MathContext;

import static org.hamcrest.Matchers.is;

public class NumericTypeTest extends BasePGTypeTest<BigDecimal> {

    public NumericTypeTest() {
        super(NumericType.INSTANCE);
    }

    @Test
    public void test_read_and_write_numeric_text_value() {
        var expected = new BigDecimal("12.123", MathContext.DECIMAL64)
            .setScale(2, MathContext.DECIMAL64.getRoundingMode());
        ByteBuf buffer = Unpooled.buffer();
        try {
            //noinspection unchecked
            pgType.writeAsText(buffer, expected);
            var actual = pgType.readTextValue(buffer, buffer.readInt());
            assertThat(actual, is(expected));
            assertThat(actual.toString(), is("12.12"));
        } finally {
            buffer.release();
        }
    }

    @Test
    public void test_read_and_write_numeric_binary_value() {
        var expected = new BigDecimal("12.123")
            .setScale(2, MathContext.DECIMAL64.getRoundingMode());
        assertThat(expected.scale(), is(2));
        assertThat(expected.precision(), is(4));

        ByteBuf buffer = Unpooled.buffer();
        try {
            //noinspection unchecked
            pgType.writeAsBinary(buffer, expected);
            BigDecimal actual = (BigDecimal) pgType.readBinaryValue(buffer, buffer.readInt());
            assertThat(actual.precision(), is(expected.precision()));
            assertThat(actual.scale(), is(expected.scale()));
            assertThat(actual.toString(), is("12.12"));
        } finally {
            buffer.release();
        }
    }
}
